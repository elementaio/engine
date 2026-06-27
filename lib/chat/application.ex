defmodule Chat.Application do
  @moduledoc """
  OTP application + root supervisor for the chat engine core.

  M5 makes the runtime cluster-capable. Two lookups are now cluster-global via
  `:syn` process groups: the `:users` scope (a user's sessions, anywhere) and the
  `:conv_subs` scope (a conversation's online members, anywhere). The conversation
  *owner* is placed on a deterministic node (`Chat.Cluster`) and looked up through
  a node-local `Registry`. On a single node this is a cluster-of-one and behaves
  exactly as before.

  Concrete adapters (persistence, membership, …) are started by the *body*, not
  here. Node *formation* (libcluster/`Node.connect`) is also the body's job.
  """
  use Application

  @impl true
  def start(_type, _args) do
    # Fail LOUD and EARLY on bad config (missing/invalid adapters, bad limits)
    # rather than with a confusing crash on the first message (OBS-3).
    :ok = Chat.Config.validate!()

    # Join this node to the cluster-global scopes (safe on a single node too).
    :syn.add_node_to_scopes([:users, :conv_subs])

    children = [
      # Node-local owner lookup: conversation_id -> the owner pid ON THIS NODE.
      # (The owner is placed on its hash-node; this Registry is that node's index.)
      {Registry, keys: :unique, name: Chat.ConversationRegistry},
      # Monitor-based online/last-seen tracker (cluster-aware online via :syn).
      Chat.Presence,
      # One owner process per ACTIVE conversation, started on demand.
      {DynamicSupervisor, strategy: :one_for_one, name: Chat.Conversation.Supervisor},
      # One process per connected device.
      {DynamicSupervisor, strategy: :one_for_one, name: Chat.Session.Supervisor},
      # Off-hub best-effort work (e.g. offline push notifications) so the
      # single-writer conversation owners never block on it.
      {Task.Supervisor, name: Chat.TaskSupervisor}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Chat.Supervisor)
  end
end
