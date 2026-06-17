defmodule Chat.OfflineCatchupTest do
  @moduledoc """
  M3: automatic offline store-and-forward. A reconnecting device receives
  everything it missed — automatically, in order, exactly once — via per-device
  cursors + the durable log. No manual `sync` required.
  """
  use ExUnit.Case, async: false

  alias Chat.Adapters.InMemory.{Persistence, ConversationStore, CursorStore}
  alias Chat.Adapters.TestTransport
  alias Chat.{Envelope, Session}

  setup do
    Enum.each([Persistence, ConversationStore, CursorStore], &ensure_clean/1)
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

  # Sync the sender after the (async) cast so the message is fully persisted and
  # fanned out before the test proceeds — avoids the at-least-once reconnect race.
  defp send_msg(session, conv, id, payload) do
    Session.handle_inbound(session, %Envelope{type: :send, conversation_id: conv, id: id, payload: payload})
    Session.sync(session)
  end

  test "offline member auto-receives missed messages on connect (no manual sync)" do
    :ok = Chat.create_conversation("c", ["A", "B"])
    a = connect("A", :A)

    send_msg(a, "c", "m1", "one")
    send_msg(a, "c", "m2", "two")
    assert_receive {:frame, :A, %Envelope{type: :ack, seq: 2}}

    # B connects → automatically receives m1 then m2, in order. No sync request.
    _b = connect("B", :B)
    assert_receive {:frame, :B, %Envelope{type: :message, seq: 1, payload: "one"}}
    assert_receive {:frame, :B, %Envelope{type: :message, seq: 2, payload: "two"}}
  end

  test "reconnect does not re-deliver already-delivered messages (exactly-once)" do
    :ok = Chat.create_conversation("c", ["A", "B"])
    a = connect("A", :A)

    send_msg(a, "c", "m1", "one")
    b = connect("B", :B)
    assert_receive {:frame, :B, %Envelope{type: :message, seq: 1}}

    # B drops, A keeps sending
    Session.disconnect(b)
    send_msg(a, "c", "m2", "two")
    assert_receive {:frame, :A, %Envelope{type: :ack, seq: 2}}

    # B reconnects (same device) → gets ONLY m2, never m1 again
    _b2 = connect("B", :B)
    assert_receive {:frame, :B, %Envelope{type: :message, seq: 2, payload: "two"}}
    refute_receive {:frame, :B, %Envelope{type: :message, seq: 1}}
  end

  test "a sender's own messages are not re-delivered to it on reconnect" do
    :ok = Chat.create_conversation("c", ["A", "B"])
    a = connect("A", :A)
    send_msg(a, "c", "m1", "hi")
    assert_receive {:frame, :A, %Envelope{type: :ack, seq: 1}}

    Session.disconnect(a)
    _a2 = connect("A", :A)
    refute_receive {:frame, :A, %Envelope{type: :message, seq: 1}}
  end

  test "fresh connect with nothing missed delivers no backlog" do
    :ok = Chat.create_conversation("c", ["A", "B"])
    _a = connect("A", :A)
    refute_receive {:frame, :A, %Envelope{type: :message}}
  end

  test "a newly connecting group member catches up the group backlog" do
    :ok = Chat.create_conversation("g", ["A", "B", "C"])
    a = connect("A", :A)
    send_msg(a, "g", "m1", "hello team")
    send_msg(a, "g", "m2", "you around?")

    _c = connect("C", :C)
    assert_receive {:frame, :C, %Envelope{type: :message, seq: 1, payload: "hello team"}}
    assert_receive {:frame, :C, %Envelope{type: :message, seq: 2, payload: "you around?"}}
  end

  test "cursor advances as messages are delivered" do
    :ok = Chat.create_conversation("c", ["A", "B"])
    a = connect("A", :A)
    _b = connect("B", :B)

    send_msg(a, "c", "m1", "one")
    assert_receive {:frame, :B, %Envelope{type: :message, seq: 1}}

    # B's device cursor for "c" reflects what it has received
    assert {:ok, 1} = CursorStore.get({"B", "B"}, "c")
    # and the sender's cursor advanced too (it sent seq 1)
    assert {:ok, 1} = CursorStore.get({"A", "A"}, "c")
  end

  defp drain do
    for sup <- [Chat.Session.Supervisor, Chat.Conversation.Supervisor],
        {_, pid, _, _} <- DynamicSupervisor.which_children(sup),
        is_pid(pid) do
      DynamicSupervisor.terminate_child(sup, pid)
    end
  end
end
