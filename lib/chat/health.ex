defmodule Chat.Health do
  @moduledoc """
  Liveness/readiness surface and graceful drain (OBS-4/5).

  A body fronts the engine with a load balancer or orchestrator that needs two
  signals: *is this node ready to take new connections* and *please stop sending
  it new ones so it can be rolled*. This module is that seam.

  * `ready?/0` — config is valid AND the node is not draining. A health endpoint
    in the body maps this to 200/503 so a draining or misconfigured node is taken
    out of rotation.
  * `drain/0` / `resume/0` — flip a node-local drain flag. While draining,
    `Chat.Session.connect/1` refuses NEW sessions with `{:error, :draining}`;
    existing sessions keep running until their clients disconnect, so a deploy can
    roll a node without dropping live traffic.

  The flag lives in `:persistent_term` (node-local, lock-free reads on the hot
  `connect` path) — no process, no extra dependency.
  """

  @key {__MODULE__, :draining}

  @doc "Mark this node as draining: it stops accepting new sessions but keeps existing ones."
  @spec drain() :: :ok
  def drain do
    :persistent_term.put(@key, true)
    # Advertise to the cluster so placement pre-migrates our owned conversations
    # off this node while it is still up (B12), not on stop.
    Chat.Cluster.mark_draining(true)
    :telemetry.execute([:chat, :health, :drain], %{}, %{node: Node.self()})
    :ok
  end

  @doc "Resume accepting new sessions after a drain."
  @spec resume() :: :ok
  def resume do
    :persistent_term.put(@key, false)
    Chat.Cluster.mark_draining(false)
    :telemetry.execute([:chat, :health, :resume], %{}, %{node: Node.self()})
    :ok
  end

  @doc "Is this node draining (refusing new sessions)?"
  @spec draining?() :: boolean()
  def draining?, do: :persistent_term.get(@key, false)

  @doc "Is this node ready to accept new connections? (config valid and not draining)."
  @spec ready?() :: boolean()
  def ready?, do: not draining?() and Chat.Config.valid?()

  @doc "How many live device sessions are currently running on this node."
  @spec live_session_count() :: non_neg_integer()
  def live_session_count do
    %{active: n} = DynamicSupervisor.count_children(Chat.Session.Supervisor)
    n
  rescue
    # Supervisor not started (e.g. app not booted) — treat as drained.
    _ -> 0
  end

  @doc """
  Graceful-stop PRIMITIVE for a body's pre-stop hook (B16): mark the node draining,
  then busy-wait (polling `live_session_count/0`) until every session has
  disconnected or `timeout_ms` elapses. Returns `:ok` if fully drained, or
  `{:timeout, remaining}` with the still-live count so the body can decide whether
  to hard-stop. Orchestrating the actual shutdown stays the body's job — the engine
  only owns the drain flag and the count.
  """
  @spec await_drained(timeout_ms :: non_neg_integer(), poll_ms :: pos_integer()) ::
          :ok | {:timeout, non_neg_integer()}
  def await_drained(timeout_ms \\ 30_000, poll_ms \\ 100) do
    drain()
    await_drained_loop(timeout_ms, poll_ms)
  end

  defp await_drained_loop(remaining_ms, poll_ms) do
    case live_session_count() do
      0 ->
        :ok

      n when remaining_ms <= 0 ->
        {:timeout, n}

      _ ->
        Process.sleep(poll_ms)
        await_drained_loop(remaining_ms - poll_ms, poll_ms)
    end
  end
end
