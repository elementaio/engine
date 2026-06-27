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
    :telemetry.execute([:chat, :health, :drain], %{}, %{node: Node.self()})
    :ok
  end

  @doc "Resume accepting new sessions after a drain."
  @spec resume() :: :ok
  def resume do
    :persistent_term.put(@key, false)
    :telemetry.execute([:chat, :health, :resume], %{}, %{node: Node.self()})
    :ok
  end

  @doc "Is this node draining (refusing new sessions)?"
  @spec draining?() :: boolean()
  def draining?, do: :persistent_term.get(@key, false)

  @doc "Is this node ready to accept new connections? (config valid and not draining)."
  @spec ready?() :: boolean()
  def ready?, do: not draining?() and Chat.Config.valid?()
end
