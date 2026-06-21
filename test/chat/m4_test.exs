defmodule Chat.M4Test do
  @moduledoc """
  M4: presence (online/last-seen), typing indicators, and first-class read
  receipts — live for 1:1, aggregated "seen by N" for groups.
  """
  use ExUnit.Case, async: false

  alias Chat.Adapters.InMemory.{
    Persistence,
    ConversationStore,
    CursorStore,
    PresenceStore,
    ReceiptStore
  }

  alias Chat.Adapters.TestTransport
  alias Chat.{Envelope, Session}

  @adapters [Persistence, ConversationStore, CursorStore, PresenceStore, ReceiptStore]

  setup do
    Enum.each(@adapters, &ensure_clean/1)
    Chat.Presence.reset()
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

  defp send_msg(s, conv, id, payload) do
    Session.handle_inbound(s, %Envelope{
      type: :send,
      conversation_id: conv,
      id: id,
      payload: payload
    })

    Session.sync(s)
  end

  # ── Presence ────────────────────────────────────────────────────────────────

  test "a peer sees a user come online and go offline; last_seen is recorded" do
    :ok = Chat.create_conversation("c", ["A", "B"])
    _b = connect("B", :B)

    a = connect("A", :A)
    assert_receive {:frame, :B, %Envelope{type: :presence, user_id: "A", status: :online}}
    assert Chat.presence_of("A") == :online

    Session.disconnect(a)

    assert_receive {:frame, :B,
                    %Envelope{type: :presence, user_id: "A", status: :offline, ts: ts}}

    assert is_integer(ts)
    assert {:offline, ^ts} = Chat.presence_of("A")
  end

  test "presence_query pulls a user's current presence" do
    :ok = Chat.create_conversation("c", ["A", "B"])
    b = connect("B", :B)

    Session.handle_inbound(b, %Envelope{type: :presence_query, user_id: "A"})
    assert_receive {:frame, :B, %Envelope{type: :presence, user_id: "A", status: :offline}}

    _a = connect("A", :A)
    Session.handle_inbound(b, %Envelope{type: :presence_query, user_id: "A"})
    assert_receive {:frame, :B, %Envelope{type: :presence, user_id: "A", status: :online}}
  end

  # ── Typing ──────────────────────────────────────────────────────────────────

  test "typing indicator fans out to other online members, not the sender" do
    :ok = Chat.create_conversation("c", ["A", "B"])
    a = connect("A", :A)
    _b = connect("B", :B)

    Session.handle_inbound(a, %Envelope{type: :typing, conversation_id: "c", status: :start})
    assert_receive {:frame, :B, %Envelope{type: :typing, sender_id: "A", status: :start}}
    refute_receive {:frame, :A, %Envelope{type: :typing}}
  end

  # ── Receipts ────────────────────────────────────────────────────────────────

  test "1:1 read receipt is delivered live to the sender" do
    :ok = Chat.create_conversation("dm", ["A", "B"])
    a = connect("A", :A)
    b = connect("B", :B)

    send_msg(a, "dm", "m1", "hi")
    assert_receive {:frame, :B, %Envelope{type: :message, seq: 1}}

    Session.handle_inbound(b, %Envelope{type: :read, conversation_id: "dm", seq: 1})
    assert_receive {:frame, :A, %Envelope{type: :receipt, status: :read, seq: 1, sender_id: "B"}}
  end

  test "group read receipts are aggregated (seen by N), not fanned out per member" do
    :ok = Chat.create_conversation("g", ["A", "B", "C"])
    a = connect("A", :A)
    b = connect("B", :B)
    c = connect("C", :C)

    send_msg(a, "g", "m1", "hi team")
    assert_receive {:frame, :B, %Envelope{type: :message, seq: 1}}
    assert_receive {:frame, :C, %Envelope{type: :message, seq: 1}}

    Session.handle_inbound(b, %Envelope{type: :read, conversation_id: "g", seq: 1})
    Session.handle_inbound(c, %Envelope{type: :read, conversation_id: "g", seq: 1})
    Session.sync(b)
    Session.sync(c)

    # the sender gets NO per-member receipt storm
    refute_receive {:frame, :A, %Envelope{type: :receipt}}

    # but the aggregate is queryable (pull): seen by 2 — B and C
    assert {2, readers} = Chat.read_state("g", 1)
    assert Enum.sort(readers) == ["B", "C"]

    Session.handle_inbound(a, %Envelope{type: :read_state, conversation_id: "g", seq: 1})
    assert_receive {:frame, :A, %Envelope{type: :read_state, seq: 1, count: 2}}
  end

  defp drain do
    for sup <- [Chat.Session.Supervisor, Chat.Conversation.Supervisor],
        {_, pid, _, _} <- DynamicSupervisor.which_children(sup),
        is_pid(pid) do
      DynamicSupervisor.terminate_child(sup, pid)
    end
  end
end
