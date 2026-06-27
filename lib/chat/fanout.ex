defmodule Chat.Fanout do
  @moduledoc """
  O(online) message fan-out — the trick that makes unlimited groups cheap, now
  cluster-wide.

  Each online member's session joins a per-conversation `:syn` process group
  (`:conv_subs` scope). Fan-out delivers only to that group's members, which may
  live on any node — a `cast` to a remote pid is transparent over Erlang
  distribution. So a 1,000,000-member group with 50 people online costs 50 sends
  regardless of roster size or how the 50 are spread across nodes. Offline members
  are not a fan-out problem; they catch up by `seq` (plan Part 11).

  `subscribe/1` and `unsubscribe/1` MUST run inside the session process (`:syn`
  joins the calling pid). `:syn` joins are idempotent and auto-cleaned on death.

  Cross-node delivery is **coalesced** (plan Part 11): members are grouped by
  node, and each remote node receives exactly ONE message (an `:erpc.cast` with
  the list of its local member pids), which then fans out locally there. So the
  cross-node cost scales with the number of nodes that have online members, not
  with the member count — a 100k-member group spread over 10 nodes costs ≤10
  cross-node messages per broadcast, and the envelope is serialized once per node.
  """
  alias Chat.Types

  @scope :conv_subs

  @doc "Subscribe the calling session to a conversation's online group."
  @spec subscribe(Types.conversation_id()) :: :ok
  def subscribe(conversation_id) do
    :syn.join(@scope, conversation_id, self())
    :ok
  end

  @doc "Unsubscribe the calling session from a conversation's online group."
  @spec unsubscribe(Types.conversation_id()) :: :ok
  def unsubscribe(conversation_id) do
    :syn.leave(@scope, conversation_id, self())
    :ok
  end

  @doc """
  Deliver `env` to every online subscriber (cluster-wide) except `except`.
  Coalesces to one cross-node message per node, then fans out locally on each.
  """
  @spec dispatch(Types.conversation_id(), Chat.Envelope.t(), pid() | nil) :: :ok
  def dispatch(conversation_id, env, except \\ nil) do
    by_node =
      @scope
      |> :syn.members(conversation_id)
      |> Stream.map(fn {pid, _meta} -> pid end)
      |> Stream.reject(&(&1 == except))
      |> Enum.group_by(&node/1)

    Enum.each(by_node, fn {node, pids} ->
      if node == Node.self() do
        deliver_local(pids, env)
      else
        # one cross-node message for this whole node; it fans out locally there
        :erpc.cast(node, __MODULE__, :deliver_local, [pids, env])
      end
    end)

    :telemetry.execute(
      [:chat, :fanout, :dispatch],
      %{
        recipients: by_node |> Map.values() |> Enum.map(&length/1) |> Enum.sum(),
        nodes: map_size(by_node)
      },
      %{conversation_id: conversation_id, type: env.type}
    )

    :ok
  end

  @doc false
  # Runs ON the node that owns these session pids; a local cast each.
  def deliver_local(pids, env), do: Enum.each(pids, &Chat.Session.deliver(&1, env))

  @doc "How many sessions are currently online (subscribed) for a conversation, cluster-wide."
  @spec online_count(Types.conversation_id()) :: non_neg_integer()
  def online_count(conversation_id), do: length(:syn.members(@scope, conversation_id))
end
