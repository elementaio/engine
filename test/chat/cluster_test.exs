defmodule Chat.ClusterTest do
  @moduledoc """
  M5: real multi-node test. Starts a second BEAM node (`:peer`), forms a cluster,
  and proves a message from a user on node A reaches a user on node B — routed
  through the conversation's single owner (placed by `Chat.Cluster`) and fanned
  out across nodes via the `:syn` `:conv_subs` group.

  Excluded by default (needs epmd + distribution). Run with:

      mix test --include distributed
  """
  use ExUnit.Case
  @moduletag :distributed

  alias Chat.Adapters.TestTransport
  alias Chat.{Envelope, Session}

  setup_all do
    # epmd (the Erlang port mapper) must be running before distribution starts.
    System.cmd("epmd", ["-daemon"])

    unless Node.alive?() do
      {:ok, _} = :net_kernel.start([:"primary@127.0.0.1", :longnames])
    end

    Node.set_cookie(:chat_cluster_cookie)

    # :peer.start (NOT start_link) so the peer isn't killed when the setup_all
    # process exits; we stop it explicitly in on_exit.
    {:ok, peer, peer_node} =
      :peer.start(%{
        name: :peer1,
        host: ~c"127.0.0.1",
        longnames: true,
        args: [~c"-setcookie", ~c"chat_cluster_cookie"]
      })

    true = Node.connect(peer_node)
    :pong = Node.ping(peer_node)

    # Drive the peer over Erlang distribution (:erpc): give it our code + deps,
    # the engine config, then start the engine + adapters there.
    :erpc.call(peer_node, :code, :add_pathsa, [:code.get_path()])

    for {k, v} <- Application.get_all_env(:chat_engine) do
      :erpc.call(peer_node, Application, :put_env, [:chat_engine, k, v])
    end

    {:ok, _} = :erpc.call(peer_node, Application, :ensure_all_started, [:chat_engine])
    :ok = :erpc.call(peer_node, Chat.Adapters.InMemory, :start_all, [])
    Chat.Adapters.InMemory.start_all()

    # let :syn finish syncing its scopes across the two nodes
    Process.sleep(300)

    on_exit(fn -> if Process.alive?(peer), do: :peer.stop(peer) end)
    %{peer_node: peer_node}
  end

  setup %{peer_node: peer_node} do
    Chat.Adapters.InMemory.reset_all()
    :erpc.call(peer_node, Chat.Adapters.InMemory, :reset_all, [])
    :ok
  end

  test "cluster is formed", %{peer_node: peer_node} do
    assert peer_node in Node.list()
  end

  test "conversation owner is placed deterministically on its hash node", %{peer_node: peer_node} do
    local = Chat.Cluster.owner_node("some-conversation")
    remote = :erpc.call(peer_node, Chat.Cluster, :owner_node, ["some-conversation"])
    assert local == remote
    assert local in [Node.self(), peer_node]
  end

  test "a message from A (node 1) reaches B (node 2)", %{peer_node: peer_node} do
    # A connects here (primary); B connects on the peer. Both transports push
    # frames back to THIS test process (cross-node sends are transparent).
    {:ok, a} =
      Session.connect(%{user_id: "A", device_id: "a", transport: {TestTransport, {self(), :A}}})

    :ok = Session.subscribe(a, "C")

    {:ok, b} =
      :erpc.call(peer_node, Session, :connect, [
        %{user_id: "B", device_id: "b", transport: {TestTransport, {self(), :B}}}
      ])

    :ok = :erpc.call(peer_node, Session, :subscribe, [b, "C"])

    # let the :syn :conv_subs membership propagate to both nodes
    Process.sleep(200)
    assert Chat.Fanout.online_count("C") == 2

    Session.handle_inbound(a, %Envelope{
      type: :send,
      conversation_id: "C",
      id: "m1",
      payload: "cross-node!"
    })

    assert_receive {:frame, :B, %Envelope{type: :message, id: "m1", payload: "cross-node!"}}, 3000
    assert_receive {:frame, :A, %Envelope{type: :ack, id: "m1", seq: 1}}, 3000
  end

  test "owner placement is balanced across the cluster (rendezvous hashing)", %{
    peer_node: peer_node
  } do
    # ~half of conversations should hash to each of the two nodes — and when the
    # node set changes, only ~1/N keys move (HRW), which is what makes rebalancing
    # cheap. Here we just assert both nodes get a real share.
    nodes = for i <- 1..400, do: Chat.Cluster.owner_node("conv-#{i}")
    counts = Enum.frequencies(nodes)
    assert counts[Node.self()] > 100
    assert counts[peer_node] > 100
  end
end
