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

  @doc "Return the (possibly remote) pid of the conversation owner, starting it if needed."
  @spec ensure_conversation(Types.conversation_id()) :: pid()
  def ensure_conversation(conversation_id) do
    node = Chat.Cluster.owner_node(conversation_id)

    if node == Node.self() do
      ensure_local(conversation_id)
    else
      :erpc.call(node, __MODULE__, :ensure_local, [conversation_id])
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
