defmodule Chat.Session do
  @moduledoc """
  One connected device.

  Transport-agnostic: holds a `{transport_mod, ref}` and pushes outbound
  envelopes through it. Registered in `Chat.SessionRegistry`, subscribed to its
  conversations' online fan-out groups (`Chat.Fanout`), and tracked for presence
  (`Chat.Presence`).

  On (re)connect it runs **automatic catch-up** (`handle_continue/2`): delivers
  everything missed since this device's cursor before any live messages, then
  advances the cursor — so reconnect is automatic, ordered, and exactly-once
  (plan Part 10). It holds only ephemeral state.

  Inbound it handles: `:send`, `:read`, `:sync`, `:typing`, `:read_state`,
  `:presence_query`.
  """
  # :temporary — a dead session is NEVER restarted; the client reconnects and
  # resyncs by cursor. Resurrecting a socket-bound process would be wrong (plan
  # Part 6: edge processes are ephemeral).
  use GenServer, restart: :temporary

  alias Chat.{Cursors, Envelope, Fanout, Message, Receipts}

  # ── Public API ──────────────────────────────────────────────────────────────

  def connect(%{user_id: _, device_id: _, transport: _} = attrs) do
    DynamicSupervisor.start_child(Chat.Session.Supervisor, {__MODULE__, attrs})
  end

  def start_link(attrs), do: GenServer.start_link(__MODULE__, attrs)

  @doc "Feed an inbound envelope (from the client) to its session."
  def handle_inbound(pid, %Envelope{} = env), do: GenServer.cast(pid, {:inbound, env})

  @doc "Push an outbound envelope to this session's client (called by Fanout)."
  def deliver(pid, %Envelope{} = env), do: GenServer.cast(pid, {:deliver, env})

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
  def init(%{user_id: user_id, device_id: device_id, transport: transport}) do
    # Join the cluster-global :users group so this device is discoverable from
    # any node (`Chat.Router.sessions_for/1`). :syn auto-removes us on death.
    :ok = :syn.join(:users, user_id, self())

    {:ok, conversations} = Chat.Ports.conversation_store().conversations_for(user_id)
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
  end

  @impl true
  def handle_continue(:catch_up, state) do
    Enum.each(state.conversations, &catch_up(state, &1))
    {:noreply, state}
  end

  @impl true
  def handle_call({:subscribe, conversation_id}, _from, state) do
    Fanout.subscribe(conversation_id)
    {:reply, :ok, state}
  end

  def handle_call({:unsubscribe, conversation_id}, _from, state) do
    Fanout.unsubscribe(conversation_id)
    {:reply, :ok, state}
  end

  def handle_call(:sync, _from, state), do: {:reply, :ok, state}

  # Inbound :send — the client posted a new message.
  @impl true
  def handle_cast({:inbound, %Envelope{type: :send} = env}, state) do
    msg = %Message{id: env.id, sender_id: state.user_id, payload: env.payload}
    {:ok, seq} = Chat.Conversation.submit(env.conversation_id, msg, self())
    Cursors.advance(state.device_ref, env.conversation_id, seq)

    push(state, %Envelope{
      type: :ack,
      conversation_id: env.conversation_id,
      id: env.id,
      seq: seq,
      status: :server_received
    })

    {:noreply, state}
  end

  # Inbound :read — record the watermark, advance cursor, relay a receipt (1:1).
  def handle_cast({:inbound, %Envelope{type: :read} = env}, state) do
    if env.seq do
      Cursors.advance(state.device_ref, env.conversation_id, env.seq)
      Receipts.record_read(env.conversation_id, state.user_id, env.seq)
    end

    Chat.Conversation.receipt(
      env.conversation_id,
      %{type: :read, seq: env.seq, user_id: state.user_id},
      self()
    )

    {:noreply, state}
  end

  # Inbound :sync — explicit catch-up by sequence.
  def handle_cast({:inbound, %Envelope{type: :sync} = env}, state) do
    {:ok, messages} = Chat.history(env.conversation_id, env.seq || 0)

    push(state, %Envelope{
      type: :sync_page,
      conversation_id: env.conversation_id,
      messages: Enum.map(messages, &message_map/1)
    })

    {:noreply, state}
  end

  # Inbound :typing — ephemeral; fan out to online members (small convs only).
  def handle_cast({:inbound, %Envelope{type: :typing} = env}, state) do
    if conv_size(env.conversation_id) <= typing_max() do
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
    {count, readers} = Receipts.aggregate(env.conversation_id, env.seq || 0)

    push(state, %Envelope{
      type: :read_state,
      conversation_id: env.conversation_id,
      seq: env.seq,
      count: count,
      readers: readers
    })

    {:noreply, state}
  end

  # Inbound :presence_query — pull a user's presence.
  def handle_cast({:inbound, %Envelope{type: :presence_query} = env}, state) do
    {status, ts} =
      case Chat.presence_of(env.user_id) do
        :online -> {:online, nil}
        {:offline, ts} -> {:offline, ts}
      end

    push(state, %Envelope{type: :presence, user_id: env.user_id, status: status, ts: ts})
    {:noreply, state}
  end

  # Outbound: a live message ⇒ push, advance cursor, (1:1) emit delivered receipt.
  def handle_cast({:deliver, %Envelope{type: :message} = env}, state) do
    push(state, env)
    Cursors.advance(state.device_ref, env.conversation_id, env.seq)

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

  defp catch_up(state, conversation_id) do
    cursor = Cursors.get(state.device_ref, conversation_id)
    {:ok, messages} = Chat.history(conversation_id, cursor)

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
  end

  defp push(%{transport: {mod, ref}}, %Envelope{} = env), do: mod.push(ref, env)

  defp message_map(%Message{} = m),
    do: %{id: m.id, seq: m.seq, sender_id: m.sender_id, payload: m.payload, ts: m.server_ts}

  defp conv_size(conversation_id) do
    case Chat.Ports.conversation_store().member_count(conversation_id) do
      {:ok, n} -> n
      _ -> 2
    end
  end

  defp typing_max, do: Application.get_env(:chat_engine, :typing_max, 100)
end
