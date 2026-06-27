defmodule Chat.Router do
  @moduledoc """
  Finds (or lazily starts) the single owner process for a conversation — on the
  right node — and looks up a user's sessions cluster-wide.

  The owner lives on the node `Chat.Cluster.owner_node/1` selects. If that node
  is remote, we `:erpc` to it to start/find the owner there; the returned pid may
  be remote, and callers `GenServer.call/cast` it transparently over Erlang
  distribution. A node-local `Registry` makes the per-node start race-safe.
  """
  alias Chat.Types

  # Bound on the cross-node owner-lookup `:erpc`. A remote owner node that is slow
  # or partitioned must NOT hang the caller's send path indefinitely (DF-2); after
  # this it degrades to `{:error, {:owner_unreachable, node}}` and the client retries
  # (a fresh HRW election picks a reachable owner once the view reconverges).
  @owner_timeout_ms 5_000

  @doc """
  Return the (possibly remote) owner pid for a conversation, starting it if needed.

  `{:error, {:owner_unreachable, node}}` when the owner lives on a remote node we
  cannot reach within `#{@owner_timeout_ms}`ms (down, partitioned, or overloaded) —
  the send path degrades instead of blocking forever (DF-2).
  """
  @spec ensure_conversation(Types.conversation_id()) ::
          {:ok, pid()} | {:error, {:owner_unreachable, node()}}
  def ensure_conversation(conversation_id) do
    node = Chat.Cluster.owner_node(conversation_id)

    if node == Node.self() do
      {:ok, ensure_local(conversation_id)}
    else
      try do
        {:ok, :erpc.call(node, __MODULE__, :ensure_local, [conversation_id], @owner_timeout_ms)}
      catch
        kind, reason ->
          :telemetry.execute(
            [:chat, :router, :owner_unreachable],
            %{},
            %{conversation_id: conversation_id, node: node, kind: kind, reason: reason}
          )

          {:error, {:owner_unreachable, node}}
      end
    end
  end

  @doc false
  # Runs ON the owner node. Node-local Registry makes concurrent starts race-safe.
  @spec ensure_local(Types.conversation_id()) :: pid()
  def ensure_local(conversation_id) do
    case Registry.lookup(Chat.ConversationRegistry, conversation_id) do
      [{pid, _}] ->
        pid

      [] ->
        case DynamicSupervisor.start_child(
               Chat.Conversation.Supervisor,
               {Chat.Conversation, conversation_id}
             ) do
          {:ok, pid} -> pid
          {:error, {:already_started, pid}} -> pid
        end
    end
  end

  @doc "Online session pids for a user, anywhere in the cluster (`:syn` `:users` group)."
  @spec sessions_for(Types.user_id()) :: [pid()]
  def sessions_for(user_id) do
    :users |> :syn.members(user_id) |> Enum.map(fn {pid, _meta} -> pid end)
  end
end
