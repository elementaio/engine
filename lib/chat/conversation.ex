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

  It also tracks the conversation's `latest` seq in-process so it can drive the
  optional CP-fence (`Chat.Persistence.Port.append/3`): on a `{:fenced, _}` it has
  lost the seq election to another owner (split-brain) and steps down rather than
  fan out a divergent message.

  `restart: :temporary` — owner state (`size`, `latest`) is fully reconstructable
  from the ports, and the owner is re-created on demand by `Chat.Router`. A
  stepped-down (fenced) owner must NOT be auto-restarted on the same (wrong) node;
  the next write re-elects a fresh owner via HRW once the cluster view reconverges.
  """
  use GenServer, restart: :temporary

  alias Chat.{Envelope, Fanout, Message}

  # ── Public API ──────────────────────────────────────────────────────────────

  def start_link(conversation_id) do
    GenServer.start_link(__MODULE__, conversation_id, name: via(conversation_id))
  end

  @doc """
  Submit a new message; returns its assigned `seq`, or `:ephemeral` for a
  `kind: :ephemeral` message (live-only, not persisted). `from` is the sender's
  session pid.
  """
  @spec submit(Chat.Types.conversation_id(), Message.t(), pid()) ::
          {:ok, Chat.Types.seq() | :ephemeral} | {:error, :fenced | :too_large | term()}
  def submit(conversation_id, %Message{} = msg, from) do
    with {:ok, owner} <- Chat.Router.ensure_conversation(conversation_id) do
      call_owner(owner, {:submit, msg, from})
    end
  end

  @doc "Relay a delivered/read receipt to the other members (1:1 only in M2)."
  @spec receipt(Chat.Types.conversation_id(), map(), pid()) :: :ok
  def receipt(conversation_id, receipt, from) when is_map(receipt) do
    case Chat.Router.ensure_conversation(conversation_id) do
      {:ok, owner} -> GenServer.cast(owner, {:receipt, receipt, from})
      # Receipts are best-effort; a temporarily unreachable owner just drops them.
      {:error, _} -> :ok
    end

    :ok
  end

  @doc """
  Publish a message into the channel from a non-session PUBLISHER (the control
  API / a body like Pulsar). Assigns seq, persists, fans out to all subscribers
  (no originating session to exclude); receipts are suppressed. A
  `kind: :ephemeral` message is fanned out live-only (no persist) and returns
  `{:ok, :ephemeral}` — the path for live feeds / dashboards / IoT telemetry.
  """
  @spec inject(Chat.Types.conversation_id(), Message.t()) ::
          {:ok, Chat.Types.seq() | :ephemeral} | {:error, :fenced | :too_large | term()}
  def inject(conversation_id, %Message{} = msg) do
    with {:ok, owner} <- Chat.Router.ensure_conversation(conversation_id) do
      call_owner(owner, {:inject, msg})
    end
  end

  defp via(id), do: {:via, Registry, {Chat.ConversationRegistry, id}}

  # Bound on the owner GenServer.call. The owner pid may be remote (Erlang
  # distribution); if its node dies AFTER lookup the call would otherwise exit and
  # crash the caller. Convert a timeout/down into a clean error so the send path
  # degrades and the client retries — symmetric with the router's lookup fence (DF-2).
  @owner_call_timeout_ms 5_000

  defp call_owner(owner, request) do
    GenServer.call(owner, request, @owner_call_timeout_ms)
  catch
    :exit, reason ->
      :telemetry.execute(
        [:chat, :conversation, :owner_call_failed],
        %{},
        %{reason: reason}
      )

      {:error, :owner_unreachable}
  end

  # ── Server ──────────────────────────────────────────────────────────────────

  @impl true
  def init(conversation_id), do: {:ok, %{id: conversation_id, size: nil, latest: nil}}

  # Ephemeral (kind: :ephemeral) — live-only: NO durable append, NO seq, NO offline
  # wake, NO cursor advance. Lossy by design: offline and late subscribers never
  # see it and it is never in history. For live feeds, dashboards, presence-style
  # signals, and IoT telemetry — anything where only the current value matters and
  # replay does not. (Matched before the durable clauses below.)
  @impl true
  def handle_call({:submit, %Message{kind: :ephemeral} = msg, from}, _from, state) do
    reply_ephemeral(check_payload(msg), state, msg, from)
  end

  @impl true
  def handle_call({:inject, %Message{kind: :ephemeral} = msg}, _from, state) do
    reply_ephemeral(check_payload(msg), state, msg, nil)
  end

  @impl true
  def handle_call({:submit, %Message{} = msg, from}, _from, state) do
    case check_payload(msg) do
      :ok ->
        {size, state} = ensure_size(state)

        case fenced_append(state, msg) do
          {:ok, seq, state} ->
            env = %Envelope{
              type: :message,
              conversation_id: state.id,
              id: msg.id,
              sender_id: msg.sender_id,
              seq: seq,
              payload: msg.payload,
              # ask recipients for receipts only in confirmed 1:1
              receipts: receipts?(size)
            }

            # Deliver to the ONLINE subset only — cost is independent of roster size.
            Fanout.dispatch(state.id, env, from)
            # Wake offline members (off the hub; best-effort — see OfflineNotifier).
            notify_offline(state.id, %{msg | seq: seq})
            {:reply, {:ok, seq}, state}

          other ->
            handle_append_failure(other, state)
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:inject, %Message{} = msg}, _from, state) do
    case check_payload(msg) do
      :ok ->
        case fenced_append(state, msg) do
          {:ok, seq, state} ->
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
            notify_offline(state.id, %{msg | seq: seq})
            {:reply, {:ok, seq}, state}

          other ->
            handle_append_failure(other, state)
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_cast({:receipt, receipt, from}, state) do
    {size, state} = ensure_size(state)

    # Suppress receipt fan-out in groups (M2). Aggregated group receipts are M4.
    if receipts?(size) do
      env = %Envelope{
        type: :receipt,
        conversation_id: state.id,
        sender_id: receipt.user_id,
        seq: receipt.seq,
        status: receipt.type
      }

      Fanout.dispatch(state.id, env, from)
    end

    {:noreply, state}
  end

  # Membership changed ⇒ drop the cached size so it is refetched next message.
  def handle_cast(:invalidate_size, state), do: {:noreply, %{state | size: nil}}

  # ── Helpers ──────────────────────────────────────────────────────────────────

  defp reply_ephemeral(:ok, state, %Message{} = msg, from) do
    dispatch_ephemeral(state.id, msg, from)
    {:reply, {:ok, :ephemeral}, state}
  end

  defp reply_ephemeral({:error, reason}, state, _msg, _from) do
    {:reply, {:error, reason}, state}
  end

  defp dispatch_ephemeral(conversation_id, %Message{} = msg, from) do
    env = %Envelope{
      type: :message,
      conversation_id: conversation_id,
      id: msg.id,
      sender_id: msg.sender_id,
      # no durable seq, and recipients must NOT emit receipts for a live-only item
      seq: nil,
      payload: msg.payload,
      receipts: false
    }

    Fanout.dispatch(conversation_id, env, from)

    :telemetry.execute(
      [:chat, :conversation, :ephemeral],
      %{},
      %{conversation_id: conversation_id}
    )
  end

  # Wake conversation members with no online session via the OfflineQueue port.
  # Runs OFF the hub (a supervised task) so the single-writer owner never blocks
  # on roster scans or a slow push backend; no-op when no adapter is configured.
  defp notify_offline(conversation_id, %Message{} = msg) do
    case Chat.Ports.offline_queue() do
      nil ->
        :ok

      mod ->
        Task.Supervisor.start_child(Chat.TaskSupervisor, Chat.OfflineNotifier, :run, [
          mod,
          conversation_id,
          msg
        ])

        :ok
    end
  end

  # Persist FIRST. seq is assigned durably and idempotently by the port — this is
  # the at-least-once hinge: we do not ack the sender until it is durable. When the
  # adapter implements the optional CP-fence (`append/3`), pass our believed-latest
  # so a split-brain second owner is rejected instead of forking the log.
  defp fenced_append(%{id: id} = state, msg) do
    mod = Chat.Ports.persistence()
    latest = state.latest || seed_latest(mod, id)

    cond do
      latest == :error -> {:error, :latest_seq_unavailable}
      function_exported?(mod, :append, 3) -> do_fenced_append(mod, state, msg, latest)
      # Adapter provides no fence ⇒ no split-brain protection (documented).
      true -> do_plain_append(mod, state, msg, latest)
    end
  end

  defp do_fenced_append(mod, %{id: id} = state, msg, latest) do
    case mod.append(id, msg, latest) do
      {:ok, seq} -> {:ok, seq, %{state | latest: max(latest, seq)}}
      {:error, {:fenced, current}} -> {:fenced, current}
      {:error, reason} -> {:error, reason}
    end
  end

  defp do_plain_append(mod, %{id: id} = state, msg, latest) do
    case mod.append(id, msg) do
      {:ok, seq} -> {:ok, seq, %{state | latest: max(latest, seq)}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp seed_latest(mod, id) do
    case mod.latest_seq(id) do
      {:ok, n} -> n
      {:error, _} -> :error
    end
  end

  # Lost the seq election (split-brain): do NOT fan out a divergent message; step
  # down so a fresh owner is elected after the cluster view reconverges.
  defp handle_append_failure({:fenced, current}, state) do
    :telemetry.execute(
      [:chat, :conversation, :fenced],
      %{current_seq: current},
      %{conversation_id: state.id}
    )

    {:stop, {:shutdown, :fenced}, {:error, :fenced}, state}
  end

  # Non-fence durability error: we did NOT append, so do NOT ack — reply an error
  # and let the client retry (preserves at-least-once). In-process state is still
  # valid, so keep the owner alive.
  defp handle_append_failure({:error, reason}, state) do
    :telemetry.execute(
      [:chat, :conversation, :append_error],
      %{system_time: System.system_time()},
      %{conversation_id: state.id, reason: reason}
    )

    {:reply, {:error, reason}, state}
  end

  # Receipts ON only for a CONFIRMED 1:1/small conversation. Fail CLOSED: when the
  # member count is unknown (store error), treat as large ⇒ receipts OFF, so a
  # transient store blip can't trigger a receipt storm in a big group.
  defp receipts?(:unknown), do: false
  defp receipts?(n) when is_integer(n), do: n <= 2

  # Returns {size_or_:unknown, state}. Caches only a confirmed integer count; on
  # error leaves the cache nil so it is retried next message (no cache poisoning).
  defp ensure_size(%{size: n} = state) when is_integer(n), do: {n, state}

  defp ensure_size(%{size: nil, id: id} = state) do
    case Chat.Ports.conversation_store().member_count(id) do
      {:ok, n} -> {n, %{state | size: n}}
      _ -> {:unknown, state}
    end
  end

  defp check_payload(%Message{payload: payload}) when is_binary(payload) do
    max = max_payload_bytes()
    if is_integer(max) and byte_size(payload) > max, do: {:error, :too_large}, else: :ok
  end

  defp check_payload(_), do: :ok

  defp max_payload_bytes, do: Application.get_env(:chat_engine, :max_payload_bytes, 1_048_576)
end
