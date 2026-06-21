defmodule Chat.Presence do
  @moduledoc """
  Tracks who is online, monitor-based so it survives abrupt socket death.

  A session registers itself on connect (`track/2`); `Chat.Presence` monitors it.
  A user is "online" while they have ≥1 live session. On the first session up /
  last session down, it broadcasts a `:presence` event to the online members of
  the user's conversations (delta, and skipped for groups over `:presence_max` —
  those are pulled on demand via `:presence_query`). On going offline it records
  `last_seen` via the `PresenceStore` port.
  """
  use GenServer

  require Logger
  alias Chat.Envelope

  # ── Public API ──────────────────────────────────────────────────────────────

  def start_link(_opts), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  @doc "Register a live session for a user (called from `Chat.Session.init/1`)."
  def track(user_id, session_pid), do: GenServer.cast(__MODULE__, {:track, user_id, session_pid})

  @doc """
  Is the user online (≥1 live session) ANYWHERE in the cluster? Answered from the
  cluster-global `:users` group, so it's correct across nodes.
  """
  def online?(user_id), do: :syn.members(:users, user_id) != []

  @doc "Clear all presence (test helper)."
  def reset, do: GenServer.call(__MODULE__, :reset)

  # ── Server ──────────────────────────────────────────────────────────────────

  @impl true
  def init(_), do: {:ok, %{users: %{}, mons: %{}}}

  @impl true
  def handle_cast({:track, user_id, pid}, state) do
    ref = Process.monitor(pid)
    sessions = Map.get(state.users, user_id, MapSet.new())
    was_online = MapSet.size(sessions) > 0

    state = %{
      state
      | users: Map.put(state.users, user_id, MapSet.put(sessions, pid)),
        mons: Map.put(state.mons, ref, {user_id, pid})
    }

    unless was_online, do: broadcast(user_id, :online, nil)
    {:noreply, state}
  end

  @impl true
  def handle_call(:reset, _from, state) do
    Enum.each(state.mons, fn {ref, _} -> Process.demonitor(ref, [:flush]) end)
    {:reply, :ok, %{users: %{}, mons: %{}}}
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    case Map.pop(state.mons, ref) do
      {nil, _} ->
        {:noreply, state}

      {{user_id, pid}, mons} ->
        sessions = state.users |> Map.get(user_id, MapSet.new()) |> MapSet.delete(pid)
        locally_offline = MapSet.size(sessions) == 0

        users =
          if locally_offline,
            do: Map.delete(state.users, user_id),
            else: Map.put(state.users, user_id, sessions)

        # Only declare the user offline if they have NO sessions left ANYWHERE in
        # the cluster (other nodes may still hold a session). Exclude the pid that
        # just died in case :syn hasn't pruned it yet.
        if locally_offline and not online_elsewhere?(user_id, pid) do
          ts = System.system_time(:millisecond)
          Chat.Ports.presence_store().touch(user_id, ts)
          broadcast(user_id, :offline, ts)
        end

        {:noreply, %{state | users: users, mons: mons}}
    end
  end

  # ── Helpers ──────────────────────────────────────────────────────────────────

  defp broadcast(user_id, status, ts) do
    case Chat.Ports.conversation_store().conversations_for(user_id) do
      {:ok, conversations} ->
        env = %Envelope{type: :presence, user_id: user_id, status: status, ts: ts}

        for conv <- conversations, small_enough?(conv) do
          Chat.Fanout.dispatch(conv, env)
        end

        :ok

      {:error, reason} ->
        Logger.warning(
          "presence broadcast skipped (conversations_for #{inspect(user_id)} failed): #{inspect(reason)}"
        )

        :ok
    end
  end

  # Fail CLOSED: on a store error treat the conversation as too large to push
  # presence to, rather than flooding a possibly-huge group on a transient blip.
  defp small_enough?(conversation_id) do
    case Chat.Ports.conversation_store().member_count(conversation_id) do
      {:ok, n} -> n <= Application.get_env(:chat_engine, :presence_max, 100)
      _ -> false
    end
  end

  defp online_elsewhere?(user_id, dying_pid) do
    :users |> :syn.members(user_id) |> Enum.any?(fn {pid, _} -> pid != dying_pid end)
  end
end
