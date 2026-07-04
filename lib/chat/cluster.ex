defmodule Chat.Cluster do
  @moduledoc """
  Placement of conversation owners across the cluster.

  Every conversation has exactly ONE owner process (the single writer that
  assigns `seq`). Which node hosts it is decided by **rendezvous hashing (HRW)**:
  the owner node is the one maximizing `hash({conversation_id, node})`. Every
  node computes the same answer from the same node list, so any node can find a
  conversation's owner without coordination — and when a node joins/leaves, only
  ~1/N conversations move (plan Part 2). HRW needs no ring process and no extra
  dependency.

  Node *formation* (who is in `Node.list()`) is the body's concern — via
  libcluster, k8s DNS, or manual `Node.connect/1`. The engine just reads it.
  """

  # A draining node advertises itself in this cluster-global :syn group (anchored on
  # the always-running Presence pid). Placement then excludes it, so its ~1/N
  # conversations pre-migrate to healthy nodes WHILE it is still up — instead of
  # their owners dying on stop and incurring the owner-unreachable window (B12).
  # :syn monitors the anchor, so a crash auto-prunes the mark (self-healing — no
  # stale exclusion of a node that restarts).
  @draining_key :"$chat_engine_draining$"

  @doc "All cluster nodes eligible to OWN conversations (peers minus draining ones)."
  @spec nodes() :: [node()]
  def nodes do
    all = [Node.self() | Node.list()]
    draining = draining_nodes()

    # Never return an empty placement set (HRW needs ≥1 target). If every node is
    # draining — or this lone node is — fall back to the full list.
    case Enum.reject(all, &MapSet.member?(draining, &1)) do
      [] -> all
      eligible -> eligible
    end
  end

  @doc "The set of nodes currently advertising themselves as draining."
  @spec draining_nodes() :: MapSet.t(node())
  def draining_nodes do
    @draining_key
    |> then(&:syn.members(:users, &1))
    |> Enum.map(fn {pid, _meta} -> node(pid) end)
    |> MapSet.new()
  rescue
    # :syn scope not started (shouldn't happen once the app is up) — treat as none.
    _ -> MapSet.new()
  end

  @doc false
  # Advertise/withdraw THIS node's drain state (called by `Chat.Health`). Anchored
  # on the Presence pid so the mark lives exactly as long as the node does.
  @spec mark_draining(boolean()) :: :ok
  def mark_draining(draining?) do
    case Process.whereis(Chat.Presence) do
      nil ->
        :ok

      anchor ->
        if draining?,
          do: :syn.join(:users, @draining_key, anchor),
          else: :syn.leave(:users, @draining_key, anchor)

        :ok
    end
  end

  @doc "The node that owns a conversation (rendezvous hashing)."
  @spec owner_node(Chat.Types.conversation_id()) :: node()
  def owner_node(conversation_id) do
    Enum.max_by(nodes(), fn node -> :erlang.phash2({conversation_id, node}) end)
  end

  @doc "Does THIS node own the conversation?"
  @spec owner_local?(Chat.Types.conversation_id()) :: boolean()
  def owner_local?(conversation_id), do: owner_node(conversation_id) == Node.self()
end
