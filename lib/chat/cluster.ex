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

  @doc "All cluster nodes (this node + connected peers)."
  @spec nodes() :: [node()]
  def nodes, do: [Node.self() | Node.list()]

  @doc "The node that owns a conversation (rendezvous hashing)."
  @spec owner_node(Chat.Types.conversation_id()) :: node()
  def owner_node(conversation_id) do
    Enum.max_by(nodes(), fn node -> :erlang.phash2({conversation_id, node}) end)
  end

  @doc "Does THIS node own the conversation?"
  @spec owner_local?(Chat.Types.conversation_id()) :: boolean()
  def owner_local?(conversation_id), do: owner_node(conversation_id) == Node.self()
end
