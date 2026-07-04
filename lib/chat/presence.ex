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

    unless was_online, do: safe("broadcast_online", fn -> broadcast(user_id, :online, nil) end)
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

        cond do
          not locally_offline ->
            :ok

          # No local sessions AND none anywhere else in the cluster ⇒ truly offline.
          not online_elsewhere?(user_id, pid) ->
            go_offline(user_id, System.system_time(:millisecond))

          # No local sessions, but :syn still shows another node's session. That may
          # be a real session on a peer OR the peer's own dying pid not yet pruned
          # (both nodes would otherwise suppress and the user sticks "online" — B9).
          # Re-check after the prune window: if it's then empty, go offline.
          true ->
            schedule_offline_recheck(user_id)
        end

        {:noreply, %{state | users: users, mons: mons}}
    end
  end

  # Delayed convergence check (B9): fired after suppression. If the user has no
  # session anywhere now that :syn has had time to prune, declare them offline.
  @impl true
  def handle_info({:recheck_offline, user_id}, state) do
    unless online?(user_id), do: go_offline(user_id, System.system_time(:millisecond))
    {:noreply, state}
  end

  # ── Helpers ──────────────────────────────────────────────────────────────────

  # Record last_seen + broadcast the offline delta. Both touch adapter ports, so
  # both go through `safe/2` — an adapter raise/exit (e.g. a Locus client call
  # timeout) must NOT crash Presence and orphan every other session's monitor (B8).
  defp go_offline(user_id, ts) do
    safe("touch", fn -> Chat.Ports.presence_store().touch(user_id, ts) end)
    safe("broadcast_offline", fn -> broadcast(user_id, :offline, ts) end)
  end

  defp schedule_offline_recheck(user_id) do
    Process.send_after(self(), {:recheck_offline, user_id}, offline_recheck_ms())
  end

  defp offline_recheck_ms, do: Application.get_env(:chat_engine, :offline_recheck_ms, 3_000)

  # Run a port-touching side effect; a fault degrades to a logged no-op instead of
  # taking down the single Presence process (which would drop ALL live monitors).
  defp safe(label, fun) do
    fun.()
  rescue
    e -> Logger.warning("presence #{label} raised: #{Exception.message(e)}")
  catch
    kind, reason -> Logger.warning("presence #{label} #{kind}: #{inspect(reason)}")
  end

  defp broadcast(user_id, status, ts) do
    case Chat.Ports.conversation_store().conversations_for(user_id) do
      {:ok, conversations} ->
        env = %Envelope{type: :presence, user_id: user_id, status: status, ts: ts}

        for conv <- conversations, small_enough?(conv) do
          Chat.Fanout.dispatch(conv, env)
        end

        :ok

      {:error, reason} ->
        :telemetry.execute(
          [:chat, :presence, :broadcast_error],
          %{},
          %{user_id: user_id, reason: reason}
        )

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
