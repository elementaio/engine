defmodule Chat.Session do
  @moduledoc """
  One connected device.

  Transport-agnostic: holds a `{transport_mod, ref}` and pushes outbound
  envelopes through it. Registered in `Chat.SessionRegistry`, subscribed to its
  conversations' online fan-out groups (`Chat.Fanout`), and tracked for presence
  (`Chat.Presence`).

  ## Identity & authorization

  `connect/1` takes EITHER a trusted `:user_id` (the body pre-authenticated the
  principal) OR `:credentials`, which the engine resolves through
  `Chat.Auth.Port.authenticate/1` — a bad credential refuses the connection.
  Every inbound side-effect (`:send`, `:read`, `:sync`, `:read_state`, `:typing`,
  `:presence_query`, `subscribe`) is gated by `Chat.Auth.Port.authorize/3`: the
  engine enforces *your adapter's* verdict and pushes `:error` (reason
  `:forbidden`) on a deny — it never crashes on one. The bundled in-memory adapter
  is allow-all, so trusted in-VM bodies need no auth wiring.

  ## Catch-up

  On (re)connect it runs **automatic catch-up** (`handle_continue/2`): it drains
  the durable log page-by-page from this device's cursor (no silent 100-message
  cap), pushing everything missed before live messages, then advances the cursor.
  Reconnect is exactly-once in steady state; the reconnect window is at-least-once
  and the client dedups by message id.

  ## Backpressure

  Inbound and outbound enqueues are shed when the session's mailbox exceeds
  `:max_mailbox` (returns `{:error, :overloaded}`); dropped durable deliveries are
  recovered by cursor catch-up on the next reconnect.

  It holds only ephemeral state.
  """
  # :temporary — a dead session is NEVER restarted; the client reconnects and
  # resyncs by cursor. Resurrecting a socket-bound process would be wrong (plan
  # Part 6: edge processes are ephemeral).
  use GenServer, restart: :temporary

  require Logger
  alias Chat.{Cursors, Envelope, Fanout, Message, Receipts}

  # ── Public API ──────────────────────────────────────────────────────────────

  @doc """
  Open a session for a connected device. Requires `:device_id` and `:transport`,
  plus EITHER a trusted `:user_id` OR `:credentials` (resolved via the Auth port).
  """
  def connect(%{device_id: _, transport: _} = attrs) do
    # A draining node (being rolled) refuses NEW sessions; existing ones keep
    # running until their clients disconnect (OBS-5). The body routes the retry
    # to another node.
    if Chat.Health.draining?() do
      {:error, :draining}
    else
      DynamicSupervisor.start_child(Chat.Session.Supervisor, {__MODULE__, attrs})
    end
  end

  def start_link(attrs), do: GenServer.start_link(__MODULE__, attrs)

  @doc "Feed an inbound envelope (from the client) to its session. Sheds on overload."
  def handle_inbound(pid, %Envelope{} = env), do: cast_unless_overloaded(pid, {:inbound, env})

  @doc "Push an outbound envelope to this session's client (called by Fanout). Sheds on overload."
  def deliver(pid, %Envelope{} = env), do: cast_unless_overloaded(pid, {:deliver, env})

  @doc "Join a conversation's online fan-out group (used when added to a group live)."
  def subscribe(pid, conversation_id), do: GenServer.call(pid, {:subscribe, conversation_id})

  @doc "Leave a conversation's online fan-out group (used when removed from a group)."
  def unsubscribe(pid, conversation_id), do: GenServer.call(pid, {:unsubscribe, conversation_id})

  @doc "Disconnect and stop the session."
  def disconnect(pid), do: GenServer.stop(pid, :normal)

  @doc "Synchronization point — returns only after all prior casts are processed (tests)."
  def sync(pid, timeout \\ 5000), do: GenServer.call(pid, :sync, timeout)

  # ── Server ──────────────────────────────────────────────────────────────────

  @impl true
  def init(%{device_id: device_id, transport: transport} = attrs) do
    case resolve_user(attrs) do
      {:ok, user_id} ->
        :telemetry.execute(
          [:chat, :session, :connected],
          %{},
          %{user_id: user_id, device_id: device_id}
        )

        # Join the cluster-global :users group so this device is discoverable from
        # any node (`Chat.Router.sessions_for/1`). :syn auto-removes us on death.
        :ok = :syn.join(:users, user_id, self())

        conversations = conversations_for(user_id)
        Enum.each(conversations, &Fanout.subscribe/1)
        Chat.Presence.track(user_id, self())

        state = %{
          user_id: user_id,
          device_id: device_id,
          device_ref: {user_id, device_id},
          transport: transport,
          conversations: conversations
        }

        {:ok, state, {:continue, :catch_up}}

      {:error, reason} ->
        :telemetry.execute(
          [:chat, :session, :auth_failed],
          %{},
          %{device_id: device_id, reason: reason}
        )

        # Authentication failed — refuse the connection (do not start the session).
        {:stop, {:unauthenticated, reason}}
    end
  end

  @impl true
  def handle_continue(:catch_up, state) do
    Enum.each(state.conversations, &catch_up(state, &1))
    {:noreply, state}
  end

  @impl true
  def handle_call({:subscribe, conversation_id}, _from, state) do
    case authorize(:subscribe, state, conversation_id) do
      :ok ->
        Fanout.subscribe(conversation_id)
        {:reply, :ok, state}

      {:error, :forbidden} = err ->
        {:reply, err, state}
    end
  end

  def handle_call({:unsubscribe, conversation_id}, _from, state) do
    Fanout.unsubscribe(conversation_id)
    {:reply, :ok, state}
  end

  def handle_call(:sync, _from, state), do: {:reply, :ok, state}

  # Inbound :send — the client posted a new message.
  @impl true
  def handle_cast({:inbound, %Envelope{type: :send} = env}, state) do
    case authorize(:send, state, env.conversation_id) do
      :ok ->
        msg = %Message{
          id: env.id,
          sender_id: state.user_id,
          payload: env.payload,
          kind: message_kind(env.kind)
        }

        case Chat.Conversation.submit(env.conversation_id, msg, self()) do
          # Live-only message: no durable seq, no cursor advance — ack so the
          # client knows it was broadcast, with status :ephemeral and seq nil.
          {:ok, :ephemeral} ->
            push(state, %Envelope{
              type: :ack,
              conversation_id: env.conversation_id,
              id: env.id,
              seq: nil,
              status: :ephemeral
            })

          {:ok, seq} ->
            Cursors.advance(state.device_ref, env.conversation_id, seq)

            push(state, %Envelope{
              type: :ack,
              conversation_id: env.conversation_id,
              id: env.id,
              seq: seq,
              status: :server_received
            })

          {:error, reason} ->
            # No durable seq assigned ⇒ NO ack. Surface the error; client retries.
            push(state, %Envelope{
              type: :error,
              conversation_id: env.conversation_id,
              id: env.id,
              reason: reason
            })
        end

      {:error, :forbidden} ->
        push(state, error_env(env.conversation_id, env.id, :forbidden))
    end

    {:noreply, state}
  end

  # Inbound :read — record the watermark, advance cursor, relay a receipt (1:1).
  def handle_cast({:inbound, %Envelope{type: :read} = env}, state) do
    case authorize(:read, state, env.conversation_id) do
      :ok ->
        # A nil seq records nothing AND relays nothing (CC-7).
        if env.seq do
          Cursors.advance(state.device_ref, env.conversation_id, env.seq)
          Receipts.record_read(env.conversation_id, state.user_id, env.seq)

          Chat.Conversation.receipt(
            env.conversation_id,
            %{type: :read, seq: env.seq, user_id: state.user_id},
            self()
          )
        end

      {:error, :forbidden} ->
        push(state, error_env(env.conversation_id, nil, :forbidden))
    end

    {:noreply, state}
  end

  # Inbound :sync — explicit catch-up by sequence. ONE page per request: the reply
  # carries a `seq` continuation cursor and a `more` flag; the client re-issues
  # :sync with that `seq` until `more` is false. Page size is the client's `count`
  # (capped at :sync_page_max). Delivered messages advance the device cursor, so
  # :sync and auto catch-up share semantics (CC-5).
  def handle_cast({:inbound, %Envelope{type: :sync} = env}, state) do
    case authorize(:sync, state, env.conversation_id) do
      :ok ->
        deliver_sync_page(state, env)

      {:error, :forbidden} ->
        push(state, error_env(env.conversation_id, nil, :forbidden))
    end

    {:noreply, state}
  end

  # Inbound :typing — ephemeral; fan out to online members (small convs only).
  def handle_cast({:inbound, %Envelope{type: :typing} = env}, state) do
    with :ok <- authorize(:typing, state, env.conversation_id),
         true <- small_for_typing?(env.conversation_id) do
      Fanout.dispatch(
        env.conversation_id,
        %Envelope{
          type: :typing,
          conversation_id: env.conversation_id,
          sender_id: state.user_id,
          status: env.status
        },
        self()
      )
    end

    {:noreply, state}
  end

  # Inbound :read_state — pull the aggregate "seen by N" for a seq.
  def handle_cast({:inbound, %Envelope{type: :read_state} = env}, state) do
    case authorize(:read_state, state, env.conversation_id) do
      :ok ->
        {count, readers} = Receipts.aggregate(env.conversation_id, env.seq || 0)

        push(state, %Envelope{
          type: :read_state,
          conversation_id: env.conversation_id,
          seq: env.seq,
          count: count,
          readers: readers
        })

      {:error, :forbidden} ->
        push(state, error_env(env.conversation_id, nil, :forbidden))
    end

    {:noreply, state}
  end

  # Inbound :presence_query — pull a user's presence. Authorized on the TARGET user.
  def handle_cast({:inbound, %Envelope{type: :presence_query} = env}, state) do
    case authorize(:presence_query, state, env.user_id) do
      :ok ->
        {status, ts} =
          case Chat.presence_of(env.user_id) do
            :online -> {:online, nil}
            {:offline, ts} -> {:offline, ts}
          end

        push(state, %Envelope{type: :presence, user_id: env.user_id, status: status, ts: ts})

      {:error, :forbidden} ->
        push(state, %Envelope{type: :error, reason: :forbidden})
    end

    {:noreply, state}
  end

  # Outbound: a live message ⇒ push, advance cursor, (1:1) emit delivered receipt.
  def handle_cast({:deliver, %Envelope{type: :message} = env}, state) do
    push(state, env)
    if env.seq, do: Cursors.advance(state.device_ref, env.conversation_id, env.seq)

    if env.receipts do
      Chat.Conversation.receipt(
        env.conversation_id,
        %{type: :delivered, seq: env.seq, user_id: state.user_id},
        self()
      )
    end

    {:noreply, state}
  end

  # Any other outbound envelope (receipt, system, typing, presence, …) ⇒ push it.
  def handle_cast({:deliver, %Envelope{} = env}, state) do
    push(state, env)
    {:noreply, state}
  end

  # ── Helpers ──────────────────────────────────────────────────────────────────

  # Identity: a trusted user_id (body pre-authenticated) wins; else authenticate
  # the supplied credentials through the Auth port; else refuse.
  defp resolve_user(%{user_id: user_id}) when is_binary(user_id), do: {:ok, user_id}

  defp resolve_user(%{credentials: credentials}) do
    Chat.Ports.auth().authenticate(credentials)
  end

  defp resolve_user(_), do: {:error, :no_identity}

  defp authorize(action, %{user_id: user_id}, resource) do
    case Chat.Ports.auth().authorize(action, user_id, resource) do
      :ok ->
        :ok

      {:error, _} = err ->
        :telemetry.execute(
          [:chat, :session, :authorize_denied],
          %{},
          %{action: action, user_id: user_id, resource: resource}
        )

        err
    end
  end

  defp conversations_for(user_id) do
    case Chat.Ports.conversation_store().conversations_for(user_id) do
      {:ok, conversations} ->
        conversations

      {:error, reason} ->
        Logger.warning(
          "conversations_for #{inspect(user_id)} failed at connect: #{inspect(reason)} — starting with none"
        )

        []
    end
  end

  # Drain the durable log from the device's cursor, page by page, until exhausted —
  # no silent 100-message truncation (CC-4). Each page advances the cursor so a
  # disconnect mid-catch-up resumes correctly.
  @catch_up_page 100

  defp catch_up(state, conversation_id) do
    cursor = Cursors.get(state.device_ref, conversation_id)
    drain(state, conversation_id, cursor)
  end

  # Deliver ONE page of explicit :sync and advance the cursor over it. The client
  # drives pagination via the returned `seq`/`more`.
  defp deliver_sync_page(state, %Envelope{conversation_id: conv} = env) do
    after_seq = env.seq || 0

    case Chat.history_page(conv, after_seq, sync_limit(env.count)) do
      {:ok, page} ->
        Enum.each(page.messages, &Cursors.advance(state.device_ref, conv, &1.seq))

        push(state, %Envelope{
          type: :sync_page,
          conversation_id: conv,
          seq: page.next_after,
          more: page.more?,
          messages: Enum.map(page.messages, &message_map/1)
        })

      {:error, reason} ->
        push(state, error_env(conv, nil, reason))
    end
  end

  defp sync_limit(n) when is_integer(n) and n > 0, do: min(n, sync_page_max())
  defp sync_limit(_), do: sync_page_max()

  defp sync_page_max, do: Application.get_env(:chat_engine, :sync_page_max, 100)

  # Only the engine's two known kinds are honored from the wire; anything else
  # (including nil) is treated as a normal durable message.
  defp message_kind(:ephemeral), do: :ephemeral
  defp message_kind(_), do: :chat

  defp drain(state, conversation_id, after_seq) do
    case Chat.history_page(conversation_id, after_seq, @catch_up_page) do
      {:ok, %{messages: []}} ->
        :ok

      {:ok, %{messages: messages, next_after: last, more?: more?}} ->
        Enum.each(messages, fn %Message{} = m ->
          push(state, %Envelope{
            type: :message,
            conversation_id: conversation_id,
            id: m.id,
            sender_id: m.sender_id,
            seq: m.seq,
            payload: m.payload,
            receipts: false
          })

          Cursors.advance(state.device_ref, conversation_id, m.seq)
        end)

        if more? do
          :telemetry.execute(
            [:chat, :session, :catch_up_page],
            %{count: length(messages)},
            %{conversation_id: conversation_id}
          )

          drain(state, conversation_id, last)
        else
          :ok
        end

      {:error, reason} ->
        Logger.warning(
          "catch-up history failed (conv=#{inspect(conversation_id)}): #{inspect(reason)}"
        )

        :ok
    end
  end

  defp push(%{transport: {mod, ref}}, %Envelope{} = env), do: mod.push(ref, env)

  defp error_env(conversation_id, id, reason),
    do: %Envelope{type: :error, conversation_id: conversation_id, id: id, reason: reason}

  defp message_map(%Message{} = m),
    do: %{id: m.id, seq: m.seq, sender_id: m.sender_id, payload: m.payload, ts: m.server_ts}

  # Fail CLOSED: on a store error, do NOT fan out typing (assume too large).
  defp small_for_typing?(conversation_id) do
    case Chat.Ports.conversation_store().member_count(conversation_id) do
      {:ok, n} -> n <= typing_max()
      _ -> false
    end
  end

  defp typing_max, do: Application.get_env(:chat_engine, :typing_max, 100)

  # ── Backpressure ─────────────────────────────────────────────────────────────

  defp cast_unless_overloaded(pid, msg) do
    if overloaded?(pid) do
      :telemetry.execute([:chat, :session, :overloaded], %{}, %{})
      {:error, :overloaded}
    else
      GenServer.cast(pid, msg)
    end
  end

  defp overloaded?(pid) do
    case Process.info(pid, :message_queue_len) do
      {:message_queue_len, len} -> len > max_mailbox()
      _ -> false
    end
  end

  defp max_mailbox, do: Application.get_env(:chat_engine, :max_mailbox, 10_000)
end
