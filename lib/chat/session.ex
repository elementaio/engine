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
  cap), pushing everything missed before live messages. If a page read fails
  mid-catch-up it stops the session rather than going live past the gap — the
  client reconnects and resumes from the un-advanced cursor. Reconnect is
  exactly-once in steady state; the reconnect window is at-least-once and the
  client dedups by message id.

  ## Delivery cursor — contiguous high-water invariant

  The persisted cursor advances ONLY across the contiguous delivered prefix
  (`note_delivered/3`). A delivery whose seq sits above an as-yet-undelivered gap
  is buffered and folded in only once the gap closes. So a shed delivery, a
  swallowed catch-up error, an unvalidated client `:read` seq, or a send-ack
  racing a lower-seq peer deliver can never leapfrog the cursor past a message the
  device never received. `:read` records only the read watermark; it does not move
  the delivery cursor.

  ## Backpressure

  Inbound and outbound enqueues are shed when the session's mailbox exceeds
  `:max_mailbox` (returns `{:error, :overloaded}`); a shed durable delivery leaves
  a gap that catch-up re-reads on the next reconnect. `:system` membership-control
  frames are exempt from shedding (they are not in the durable log). If an
  outstanding gap grows past `:max_pending_gap`, the session forces a resync.

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

  @doc """
  Push an outbound envelope to this session's client (called by Fanout). Sheds on
  overload — EXCEPT `:system` membership-control frames (group join/leave/removal),
  which are not in the durable log and so cannot be recovered by catch-up if
  dropped; those are always enqueued (B14).
  """
  def deliver(pid, %Envelope{type: :system} = env), do: GenServer.cast(pid, {:deliver, env})
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
          conversations: conversations,
          # Per-conversation delivery accounting for the contiguous high-water
          # cursor invariant (B2/B3/B6): %{conv => %{hw: seq, pending: MapSet}}.
          # `hw` is the highest CONTIGUOUSLY-delivered seq (what we persist to the
          # cursor store); `pending` holds delivered seqs above a gap, absorbed
          # into `hw` only once the gap fills. Seeded lazily / on catch-up.
          cursors: %{}
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
    case catch_up_all(state, state.conversations) do
      {:ok, state} ->
        {:noreply, state}

      {:error, state} ->
        # A durable-log read failed mid-catch-up (transient store/pool/partition
        # blip). Do NOT go live with an outstanding gap: the next live delivery
        # would advance the cursor past the un-fetched range and it would never be
        # re-read (B2). Stop instead — the session is :temporary, so the client
        # reconnects and resumes cleanly from the un-advanced cursor.
        {:stop, {:shutdown, :catch_up_failed}, state}
    end
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
    state =
      case gate(:send, state, env.conversation_id) do
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

              state

            {:ok, seq} ->
              # Note our own send as delivered-to-self, but only through the
              # contiguous invariant: the cursor advances past `seq` iff every
              # lower seq is already delivered. A peer's lower-seq deliver queued
              # behind this ack (while we were blocked in submit) is thus NOT
              # leapfrogged — it fills the gap first (B6).
              state = note_delivered(state, env.conversation_id, seq)

              push(state, %Envelope{
                type: :ack,
                conversation_id: env.conversation_id,
                id: env.id,
                seq: seq,
                status: :server_received
              })

              state

            {:error, reason} ->
              # No durable seq assigned ⇒ NO ack. Surface the error; client retries.
              push(state, %Envelope{
                type: :error,
                conversation_id: env.conversation_id,
                id: env.id,
                reason: reason
              })

              state
          end

        {:error, :forbidden} ->
          push(state, error_env(env.conversation_id, env.id, :forbidden))
          state
      end

    {:noreply, state}
  end

  # Inbound :read — record the read watermark and relay a receipt (1:1). It does
  # NOT touch the per-device delivery cursor: the read watermark is a per-USER
  # "seen up to" marker and is client-supplied/unvalidated, so conflating it with
  # the delivery cursor let a device skip messages it never received (B4). Delivery
  # progress is driven solely by actual deliveries (see `note_delivered/3`).
  def handle_cast({:inbound, %Envelope{type: :read} = env}, state) do
    case gate(:read, state, env.conversation_id) do
      :ok ->
        # A nil seq records nothing AND relays nothing (CC-7).
        if env.seq do
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
    state =
      case gate(:sync, state, env.conversation_id) do
        :ok ->
          deliver_sync_page(state, env)

        {:error, :forbidden} ->
          push(state, error_env(env.conversation_id, nil, :forbidden))
          state
      end

    {:noreply, state}
  end

  # Inbound :typing — ephemeral; fan out to online members (small convs only).
  def handle_cast({:inbound, %Envelope{type: :typing} = env}, state) do
    with :ok <- gate(:typing, state, env.conversation_id),
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
    case gate(:read_state, state, env.conversation_id) do
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

  # Inbound :presence_query — pull a user's presence. A user-targeted action, so
  # the resource is tagged `{:user, id}` (NOT a bare id in the conversation_id
  # slot — B10) so an adapter can tell "authorize on this USER" from "authorize on
  # this CONVERSATION" and gate them differently. Allow-all bodies still see :ok.
  def handle_cast({:inbound, %Envelope{type: :presence_query} = env}, state) do
    case authorize(:presence_query, state, {:user, env.user_id}) do
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

  # Outbound: a live message ⇒ push, note delivery (contiguous cursor), (1:1) emit
  # a delivered receipt. The cursor advances ONLY across the contiguous delivered
  # prefix (`note_delivered/3`): a message shed under backpressure (B3) is never
  # noted, so a later higher-seq delivery cannot leapfrog the gap — catch-up
  # re-reads the shed range on reconnect.
  def handle_cast({:deliver, %Envelope{type: :message} = env}, state) do
    push(state, env)
    state = note_delivered(state, env.conversation_id, env.seq)

    if gapped_too_far?(state, env.conversation_id) do
      # An outstanding gap has grown past the bound (sustained shedding). Force a
      # clean resync rather than buffer unboundedly: stop; the client reconnects
      # and catch-up re-reads from the un-advanced cursor.
      :telemetry.execute(
        [:chat, :session, :gap_forced_resync],
        %{},
        %{conversation_id: env.conversation_id, user_id: state.user_id}
      )

      {:stop, {:shutdown, :gap_overflow}, state}
    else
      if env.receipts do
        Chat.Conversation.receipt(
          env.conversation_id,
          %{type: :delivered, seq: env.seq, user_id: state.user_id},
          self()
        )
      end

      {:noreply, state}
    end
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

  # Gate a conversation-scoped verb: the body's `authorize/3` verdict AND, when
  # `:enforce_membership` is on, a core membership check (B1). Bodies that ship an
  # allow-all `authorize` (both bundled ones do) otherwise let any authenticated
  # user read/write ANY conversation in their tenant; this opt-in closes that with
  # the `member?/2` primitive every store already implements.
  defp gate(action, state, conversation_id) do
    with :ok <- authorize(action, state, conversation_id) do
      check_membership(action, state, conversation_id)
    end
  end

  defp check_membership(action, %{user_id: user_id}, conversation_id) do
    if enforce_membership?() and not member?(conversation_id, user_id) do
      :telemetry.execute(
        [:chat, :session, :membership_denied],
        %{},
        %{action: action, user_id: user_id, conversation_id: conversation_id}
      )

      {:error, :forbidden}
    else
      :ok
    end
  end

  # Fail CLOSED on any store fault (a raise or a non-true reply denies): a security
  # gate must not fall open on a transient blip.
  defp member?(conversation_id, user_id) do
    Chat.Ports.conversation_store().member?(conversation_id, user_id) == true
  rescue
    _ -> false
  end

  defp enforce_membership?, do: Application.get_env(:chat_engine, :enforce_membership, false)

  defp conversations_for(user_id) do
    case Chat.Ports.conversation_store().conversations_for(user_id) do
      {:ok, conversations} ->
        conversations

      {:error, reason} ->
        :telemetry.execute(
          [:chat, :session, :conversations_for_error],
          %{},
          %{user_id: user_id, reason: reason}
        )

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

  # Catch up every conversation, threading session state; abort the whole
  # continue on the FIRST read error (B2) so we never go live past a gap.
  defp catch_up_all(state, conversations) do
    Enum.reduce_while(conversations, {:ok, state}, fn conv, {:ok, st} ->
      case catch_up(st, conv) do
        {:ok, st} -> {:cont, {:ok, st}}
        {:error, st} -> {:halt, {:error, st}}
      end
    end)
  end

  defp catch_up(state, conversation_id) do
    cursor = Cursors.get(state.device_ref, conversation_id)
    # Seed the in-memory high-water from the persisted cursor before draining.
    state = put_in(state.cursors[conversation_id], %{hw: cursor, pending: MapSet.new()})
    drain(state, conversation_id, cursor)
  end

  # Deliver ONE page of explicit :sync and advance the cursor over it. The client
  # drives pagination via the returned `seq`/`more`.
  defp deliver_sync_page(state, %Envelope{conversation_id: conv} = env) do
    after_seq = env.seq || 0

    case Chat.history_page(conv, after_seq, sync_limit(env.count)) do
      {:ok, page} ->
        state = Enum.reduce(page.messages, state, &note_delivered(&2, conv, &1.seq))

        push(state, %Envelope{
          type: :sync_page,
          conversation_id: conv,
          seq: page.next_after,
          more: page.more?,
          messages: Enum.map(page.messages, &message_map/1)
        })

        state

      {:error, reason} ->
        push(state, error_env(conv, nil, reason))
        state
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
        {:ok, state}

      {:ok, %{messages: messages, next_after: last, more?: more?}} ->
        state =
          Enum.reduce(messages, state, fn %Message{} = m, st ->
            push(st, %Envelope{
              type: :message,
              conversation_id: conversation_id,
              id: m.id,
              sender_id: m.sender_id,
              seq: m.seq,
              payload: m.payload,
              receipts: false
            })

            note_delivered(st, conversation_id, m.seq)
          end)

        if more? do
          :telemetry.execute(
            [:chat, :session, :catch_up_page],
            %{count: length(messages)},
            %{conversation_id: conversation_id}
          )

          drain(state, conversation_id, last)
        else
          {:ok, state}
        end

      {:error, reason} ->
        # Surface the failure (telemetry, not just a log) and signal the caller to
        # stop the session — the cursor is left at the last contiguously-delivered
        # seq, so a clean reconnect re-reads the rest (B2).
        :telemetry.execute(
          [:chat, :session, :catch_up_failed],
          %{},
          %{conversation_id: conversation_id, reason: reason}
        )

        Logger.warning(
          "catch-up history failed (conv=#{inspect(conversation_id)}): #{inspect(reason)}"
        )

        {:error, state}
    end
  end

  # ── Contiguous-delivered cursor (B2/B3/B6) ───────────────────────────────────
  #
  # The persisted delivery cursor may only advance across the CONTIGUOUS delivered
  # prefix. Each actual delivery (live, catch-up, sync page, or the sender's own
  # ack) is recorded here; the cursor moves to `hw` — the highest seq such that
  # every seq ≤ hw has been delivered. A seq above an unfilled gap is buffered in
  # `pending` and folded in only when the gap closes. This defeats the whole
  # silent-loss family: a shed delivery, a swallowed catch-up error, an unvalidated
  # client `:read` seq, or an ack racing a lower-seq peer deliver can no longer
  # leapfrog the cursor past a message the device never received.

  defp note_delivered(state, _conversation_id, nil), do: state

  defp note_delivered(state, conversation_id, seq) when is_integer(seq) do
    %{hw: hw, pending: pending} = cursor_entry(state, conversation_id)

    entry =
      if seq <= hw do
        # Already covered by the contiguous prefix (duplicate/replay) — no-op.
        %{hw: hw, pending: pending}
      else
        {hw2, pending2} = absorb_contiguous(hw, MapSet.put(pending, seq))
        if hw2 > hw, do: Cursors.advance(state.device_ref, conversation_id, hw2)
        %{hw: hw2, pending: pending2}
      end

    put_in(state.cursors[conversation_id], entry)
  end

  # Fold consecutive buffered seqs (hw+1, hw+2, …) into the high-water mark.
  defp absorb_contiguous(hw, pending) do
    if MapSet.member?(pending, hw + 1) do
      absorb_contiguous(hw + 1, MapSet.delete(pending, hw + 1))
    else
      {hw, pending}
    end
  end

  # Lazily seed a conversation's accounting from the persisted cursor.
  defp cursor_entry(state, conversation_id) do
    case Map.get(state.cursors, conversation_id) do
      nil -> %{hw: Cursors.get(state.device_ref, conversation_id), pending: MapSet.new()}
      entry -> entry
    end
  end

  # An outstanding gap this deep means the client is shedding faster than it drains;
  # stop buffering and force a clean catch-up resync instead of growing `pending`.
  defp gapped_too_far?(state, conversation_id) do
    case Map.get(state.cursors, conversation_id) do
      %{pending: pending} -> MapSet.size(pending) > max_pending_gap()
      _ -> false
    end
  end

  defp max_pending_gap, do: Application.get_env(:chat_engine, :max_pending_gap, 10_000)

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
