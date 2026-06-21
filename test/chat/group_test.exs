defmodule Chat.GroupTest do
  @moduledoc """
  M2: unlimited group chat. Proves fan-out cost tracks the ONLINE subset (not
  the roster), live add/remove membership, group receipt suppression, and that
  offline members catch up by seq.
  """
  use ExUnit.Case, async: false

  alias Chat.Adapters.InMemory.{Persistence, ConversationStore}
  alias Chat.Adapters.TestTransport
  alias Chat.{Envelope, Fanout, Session}

  setup do
    ensure_clean(Persistence)
    ensure_clean(ConversationStore)
    on_exit(&drain/0)
    :ok
  end

  defp ensure_clean(mod) do
    case start_supervised(mod) do
      {:ok, _} -> :ok
      {:error, _} -> :ok
    end

    mod.reset()
  end

  defp connect(user, label) do
    {:ok, pid} =
      Session.connect(%{
        user_id: user,
        device_id: to_string(label),
        transport: {TestTransport, {self(), label}}
      })

    pid
  end

  defp send_msg(session, conv, id, payload) do
    Session.handle_inbound(session, %Envelope{
      type: :send,
      conversation_id: conv,
      id: id,
      payload: payload
    })

    Session.sync(session)
  end

  test "fan-out reaches only the ONLINE subset, not the whole roster" do
    roster = for i <- 1..1000, do: "u#{i}"
    :ok = Chat.create_conversation("big", roster)

    # connect only 5 of the 1000 members
    sessions = for i <- 1..5, do: {i, connect("u#{i}", :"s#{i}")}
    assert Fanout.online_count("big") == 5

    {_, sender} = hd(sessions)
    send_msg(sender, "big", "m1", "hello group")

    # sender gets the ack
    assert_receive {:frame, :s1, %Envelope{type: :ack, seq: 1}}

    # the other 4 online members receive it; the 995 offline do not (no sessions)
    for i <- 2..5 do
      label = :"s#{i}"
      assert_receive {:frame, ^label, %Envelope{type: :message, id: "m1", seq: 1}}
    end

    # stored exactly once
    assert {:ok, 1} = Chat.latest_seq("big")
  end

  test "groups suppress receipts (no O(n^2) storm); 1:1 still gets them" do
    :ok = Chat.create_conversation("grp", ["A", "B", "C"])
    a = connect("A", :A)
    _b = connect("B", :B)
    _c = connect("C", :C)

    send_msg(a, "grp", "m1", "hi all")
    assert_receive {:frame, :B, %Envelope{type: :message, receipts: false}}
    assert_receive {:frame, :C, %Envelope{type: :message, receipts: false}}
    # A must NOT receive delivered receipts from B/C
    refute_receive {:frame, :A, %Envelope{type: :receipt}}

    # but a 1:1 conversation still does
    :ok = Chat.create_conversation("dm", ["A", "B"])
    send_msg(a, "dm", "m2", "psst")
    assert_receive {:frame, :B, %Envelope{type: :message, receipts: true}}
    assert_receive {:frame, :A, %Envelope{type: :receipt, status: :delivered}}
  end

  test "adding a member live subscribes them and announces a join" do
    :ok = Chat.create_conversation("g", ["A", "B"])
    a = connect("A", :A)
    _b = connect("B", :B)
    _c = connect("C", :C)

    # C is not a member yet → gets nothing
    send_msg(a, "g", "m1", "members only")
    assert_receive {:frame, :B, %Envelope{type: :message, seq: 1}}
    refute_receive {:frame, :C, %Envelope{type: :message}}

    # add C at runtime → join announced, C now subscribed
    :ok = Chat.add_member("g", "C")
    assert_receive {:frame, :C, %Envelope{type: :system, status: :join, sender_id: "C"}}

    send_msg(a, "g", "m2", "welcome C")
    assert_receive {:frame, :C, %Envelope{type: :message, seq: 2}}
  end

  test "removing a member unsubscribes them and announces a leave" do
    :ok = Chat.create_conversation("g", ["A", "B", "C"])
    a = connect("A", :A)
    _b = connect("B", :B)
    _c = connect("C", :C)

    :ok = Chat.remove_member("g", "C")
    assert_receive {:frame, :C, %Envelope{type: :system, status: :leave, sender_id: "C"}}

    send_msg(a, "g", "m1", "C should miss this")
    assert_receive {:frame, :B, %Envelope{type: :message, seq: 1}}
    refute_receive {:frame, :C, %Envelope{type: :message}}
  end

  test "offline member catches up via sync after reconnect" do
    :ok = Chat.create_conversation("g", ["A", "B", "C"])
    a = connect("A", :A)
    _b = connect("B", :B)
    # C is offline

    send_msg(a, "g", "m1", "while you were out")
    assert_receive {:frame, :A, %Envelope{type: :ack, seq: 1}}

    c = connect("C", :C)
    Session.handle_inbound(c, %Envelope{type: :sync, conversation_id: "g", seq: 0})
    assert_receive {:frame, :C, %Envelope{type: :sync_page, messages: [%{id: "m1", seq: 1}]}}
  end

  defp drain do
    for sup <- [Chat.Session.Supervisor, Chat.Conversation.Supervisor],
        {_, pid, _, _} <- DynamicSupervisor.which_children(sup),
        is_pid(pid) do
      DynamicSupervisor.terminate_child(sup, pid)
    end
  end
end
