defmodule Chat.Adapters.InMemory.Persistence do
  @moduledoc """
  In-memory reference adapter for `Chat.Persistence.Port`. For tests and demos
  only — state lives in a single GenServer and is lost on restart.

  It is, however, a faithful model of the real contract:

    * one GenServer = one serialization point ⇒ per-conversation `seq` is
      monotonic and gap-free *by construction*. This mirrors the engine's "single
      writer per conversation" rule — in the cluster that single writer is one
      `Chat.Conversation` owner process; here it is this GenServer.
    * `append/2` is idempotent on `message.id`.

  Start it like any process. By default it registers under its own module name;
  pass `name: nil` to start an anonymous instance (used by property tests):

      {:ok, _pid} = Chat.Adapters.InMemory.Persistence.start_link([])
      {:ok, srv}  = Chat.Adapters.InMemory.Persistence.start_link(name: nil)
  """
  use GenServer
  @behaviour Chat.Persistence.Port

  alias Chat.Message

  # ── Lifecycle ───────────────────────────────────────────────────────────────

  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)

    case name do
      nil -> GenServer.start_link(__MODULE__, opts)
      name -> GenServer.start_link(__MODULE__, opts, name: name)
    end
  end

  # ── Chat.Persistence.Port callbacks (target the module-named singleton) ──────

  @impl Chat.Persistence.Port
  def append(conversation_id, %Message{} = message),
    do: GenServer.call(__MODULE__, {:append, conversation_id, message})

  # CP-fencing append. NOTE on arity: this callback `append/3` is
  # (conversation_id, message, expected_seq); the server-explicit test helper
  # `append/3` below is (server, conversation_id, message). The two are
  # disambiguated by argument shape — the `is_integer(expected_seq)` guard /
  # `%Message{}` position — so they coexist without collision.
  @impl Chat.Persistence.Port
  def append(conversation_id, %Message{} = message, expected_seq) when is_integer(expected_seq),
    do: GenServer.call(__MODULE__, {:append_fenced, conversation_id, message, expected_seq})

  def append(server, conversation_id, %Message{} = message),
    do: GenServer.call(server, {:append, conversation_id, message})

  @impl Chat.Persistence.Port
  def read_after(conversation_id, after_seq, limit),
    do: read_after(__MODULE__, conversation_id, after_seq, limit)

  @impl Chat.Persistence.Port
  def latest_seq(conversation_id),
    do: latest_seq(__MODULE__, conversation_id)

  # ── Server-explicit variants (so tests can target an anonymous instance) ─────

  @doc "Fenced append against an anonymous instance (tests)."
  def append(server, conversation_id, %Message{} = message, expected_seq)
      when is_integer(expected_seq),
      do: GenServer.call(server, {:append_fenced, conversation_id, message, expected_seq})

  def read_after(server, conversation_id, after_seq, limit),
    do: GenServer.call(server, {:read_after, conversation_id, after_seq, limit})

  def latest_seq(server, conversation_id),
    do: GenServer.call(server, {:latest_seq, conversation_id})

  @doc "Wipe all state (test helper)."
  def reset(server \\ __MODULE__), do: GenServer.call(server, :reset)

  # ── Server callbacks ────────────────────────────────────────────────────────

  @impl GenServer
  def init(_opts), do: {:ok, %{convs: %{}}}

  @impl GenServer
  def handle_call({:append, conv_id, %Message{} = msg}, _from, state) do
    conv = Map.get(state.convs, conv_id, new_conv())

    case Map.get(conv.by_id, msg.id) do
      nil ->
        {seq, conv} = do_append(conv, msg)
        {:reply, {:ok, seq}, put_conv(state, conv_id, conv)}

      existing_seq ->
        # Idempotent: same client id ⇒ same seq, no new message, no advance.
        {:reply, {:ok, existing_seq}, state}
    end
  end

  # Compare-and-set append (faithful single-node model of the contract: this
  # GenServer's serialization makes the read-of-latest + conditional write one
  # linearizable step, exactly as a real adapter MUST do at the store layer).
  def handle_call({:append_fenced, conv_id, %Message{} = msg, expected_seq}, _from, state) do
    conv = Map.get(state.convs, conv_id, new_conv())
    latest = conv.next - 1

    case Map.get(conv.by_id, msg.id) do
      # Idempotency BEATS fencing: a replayed id returns its original seq even if
      # the caller's expected_seq is now stale (a retry is not a fence loss).
      existing_seq when is_integer(existing_seq) ->
        {:reply, {:ok, existing_seq}, state}

      nil when latest == expected_seq ->
        {seq, conv} = do_append(conv, msg)
        {:reply, {:ok, seq}, put_conv(state, conv_id, conv)}

      nil ->
        # The caller has been overtaken — another writer advanced the log.
        {:reply, {:error, {:fenced, latest}}, state}
    end
  end

  def handle_call({:read_after, conv_id, after_seq, limit}, _from, state) do
    conv = Map.get(state.convs, conv_id, new_conv())

    msgs =
      conv.log
      |> Enum.filter(fn {seq, _msg} -> seq > after_seq end)
      |> Enum.sort_by(fn {seq, _msg} -> seq end)
      |> Enum.take(limit)
      |> Enum.map(fn {_seq, msg} -> msg end)

    {:reply, {:ok, msgs}, state}
  end

  def handle_call({:latest_seq, conv_id}, _from, state) do
    conv = Map.get(state.convs, conv_id, new_conv())
    {:reply, {:ok, conv.next - 1}, state}
  end

  def handle_call(:reset, _from, _state), do: {:reply, :ok, %{convs: %{}}}

  # ── Helpers ──────────────────────────────────────────────────────────────────

  defp do_append(conv, %Message{} = msg) do
    seq = conv.next
    stored = %Message{msg | seq: seq, server_ts: msg.server_ts || now_ms()}

    conv = %{
      conv
      | next: seq + 1,
        by_id: Map.put(conv.by_id, msg.id, seq),
        log: Map.put(conv.log, seq, stored)
    }

    {seq, conv}
  end

  defp new_conv, do: %{next: 1, by_id: %{}, log: %{}}
  defp put_conv(state, conv_id, conv), do: %{state | convs: Map.put(state.convs, conv_id, conv)}
  defp now_ms, do: System.system_time(:millisecond)
end
