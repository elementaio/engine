defmodule Chat.Conversation do
  @moduledoc """
  The single-writer owner of one conversation.

  Because exactly one of these exists per conversation (registered in
  `Chat.ConversationRegistry`) and a GenServer processes one message at a time,
  it is the natural serialization point that assigns the monotonic `seq` — no
  locks needed (plan Part 10).

  Responsibilities:

    1. persist the message via the Persistence port (persist BEFORE ack),
    2. assign and return its `seq` to the sender,
    3. fan the message out to ONLINE members via `Chat.Fanout` (O(online)),
    4. relay receipts — but only in 1:1, to avoid group receipt storms (M2).

  It caches `member_count` (cheap O(1) via the port) to decide receipt policy;
  the cache is invalidated on add/remove member.
  """
  use GenServer

  alias Chat.{Envelope, Fanout, Message}

  # ── Public API ──────────────────────────────────────────────────────────────

  def start_link(conversation_id) do
    GenServer.start_link(__MODULE__, conversation_id, name: via(conversation_id))
  end

  @doc "Submit a new message; returns its assigned seq. `from` is the sender's session pid."
  @spec submit(Chat.Types.conversation_id(), Message.t(), pid()) :: {:ok, Chat.Types.seq()}
  def submit(conversation_id, %Message{} = msg, from) do
    conversation_id
    |> Chat.Router.ensure_conversation()
    |> GenServer.call({:submit, msg, from})
  end

  @doc "Relay a delivered/read receipt to the other members (1:1 only in M2)."
  @spec receipt(Chat.Types.conversation_id(), map(), pid()) :: :ok
  def receipt(conversation_id, receipt, from) when is_map(receipt) do
    conversation_id
    |> Chat.Router.ensure_conversation()
    |> GenServer.cast({:receipt, receipt, from})
  end

  @doc """
  Publish a message into the channel from a non-session PUBLISHER (the control
  API / a body like Pulsar). Assigns seq, persists, fans out to all subscribers
  (no originating session to exclude); receipts are suppressed.
  """
  @spec inject(Chat.Types.conversation_id(), Message.t()) :: {:ok, Chat.Types.seq()}
  def inject(conversation_id, %Message{} = msg) do
    conversation_id
    |> Chat.Router.ensure_conversation()
    |> GenServer.call({:inject, msg})
  end

  defp via(id), do: {:via, Registry, {Chat.ConversationRegistry, id}}

  # ── Server ──────────────────────────────────────────────────────────────────

  @impl true
  def init(conversation_id), do: {:ok, %{id: conversation_id, size: nil}}

  @impl true
  def handle_call({:submit, %Message{} = msg, from}, _from, state) do
    size = current_size(state)

    # Persist FIRST. seq is assigned durably and idempotently by the port — this
    # is the at-least-once hinge: we do not ack the sender until it is durable.
    {:ok, seq} = Chat.Ports.persistence().append(state.id, msg)

    env = %Envelope{
      type: :message,
      conversation_id: state.id,
      id: msg.id,
      sender_id: msg.sender_id,
      seq: seq,
      payload: msg.payload,
      # ask recipients for receipts only in 1:1
      receipts: size <= 2
    }

    # Deliver to the ONLINE subset only — cost is independent of roster size.
    Fanout.dispatch(state.id, env, from)

    {:reply, {:ok, seq}, %{state | size: size}}
  end

  @impl true
  def handle_call({:inject, %Message{} = msg}, _from, state) do
    {:ok, seq} = Chat.Ports.persistence().append(state.id, msg)

    env = %Envelope{
      type: :message,
      conversation_id: state.id,
      id: msg.id,
      sender_id: msg.sender_id,
      seq: seq,
      payload: msg.payload,
      # publisher feed item — recipients must NOT emit receipts
      receipts: false
    }

    Fanout.dispatch(state.id, env, nil)
    {:reply, {:ok, seq}, state}
  end

  @impl true
  def handle_cast({:receipt, receipt, from}, state) do
    size = current_size(state)

    # Suppress receipt fan-out in groups (M2). Aggregated group receipts are M4.
    if size <= 2 do
      env = %Envelope{
        type: :receipt,
        conversation_id: state.id,
        sender_id: receipt.user_id,
        seq: receipt.seq,
        status: receipt.type
      }

      Fanout.dispatch(state.id, env, from)
    end

    {:noreply, %{state | size: size}}
  end

  # Membership changed ⇒ drop the cached size so it is refetched next message.
  def handle_cast(:invalidate_size, state), do: {:noreply, %{state | size: nil}}

  # ── Helpers ──────────────────────────────────────────────────────────────────

  defp current_size(%{size: nil, id: id}) do
    case Chat.Ports.conversation_store().member_count(id) do
      {:ok, n} -> n
      _ -> 2
    end
  end

  defp current_size(%{size: n}), do: n
end
