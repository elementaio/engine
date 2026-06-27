defmodule Chat.HistoryPaginationTest do
  use ExUnit.Case, async: false

  alias Chat.Adapters.InMemory.{
    Persistence,
    ConversationStore,
    CursorStore,
    PresenceStore,
    ReceiptStore
  }

  alias Chat.Adapters.TestTransport
  alias Chat.{Cursors, Envelope, Message, Session}

  setup do
    Enum.each([Persistence, ConversationStore, CursorStore, PresenceStore, ReceiptStore], fn m ->
      case start_supervised(m) do
        {:ok, _} -> :ok
        {:error, _} -> :ok
      end

      m.reset()
    end)

    Chat.Presence.reset()

    on_exit(fn ->
      Application.delete_env(:chat_engine, :sync_page_max)

      for sup <- [Chat.Session.Supervisor, Chat.Conversation.Supervisor],
          {_, p, _, _} <- DynamicSupervisor.which_children(sup),
          is_pid(p),
          do: DynamicSupervisor.terminate_child(sup, p)
    end)

    :ok
  end

  defp seed(conv, n) do
    for i <- 1..n do
      {:ok, ^i} = Chat.inject(conv, %Message{id: "m#{i}", sender_id: "s", payload: "p#{i}"})
    end
  end

  # ── Chat.history_page/3 ──────────────────────────────────────────────────────

  test "history_page returns a page, a next cursor, and more? when the log is longer" do
    seed("c", 5)

    assert {:ok, %{messages: msgs, next_after: 2, more?: true}} = Chat.history_page("c", 0, 2)
    assert Enum.map(msgs, & &1.seq) == [1, 2]

    assert {:ok, %{messages: msgs2, next_after: 4, more?: true}} = Chat.history_page("c", 2, 2)
    assert Enum.map(msgs2, & &1.seq) == [3, 4]

    assert {:ok, %{messages: msgs3, next_after: 5, more?: false}} = Chat.history_page("c", 4, 2)
    assert Enum.map(msgs3, & &1.seq) == [5]
  end

  test "history_page on an exact page boundary reports more? false without an extra round-trip" do
    seed("c", 4)
    # Exactly two full pages; the second must report more? false (the +1 look-ahead).
    assert {:ok, %{next_after: 2, more?: true}} = Chat.history_page("c", 0, 2)
    assert {:ok, %{messages: msgs, next_after: 4, more?: false}} = Chat.history_page("c", 2, 2)
    assert Enum.map(msgs, & &1.seq) == [3, 4]
  end

  test "history_page past the end is empty and echoes the cursor" do
    seed("c", 3)
    assert {:ok, %{messages: [], next_after: 3, more?: false}} = Chat.history_page("c", 3, 10)
  end

  # ── :sync over a session (continuation + cursor advance) ─────────────────────

  defp connect(user, label) do
    {:ok, pid} =
      Session.connect(%{
        user_id: user,
        device_id: to_string(label),
        transport: {TestTransport, {self(), label}}
      })

    pid
  end

  test "explicit :sync returns one page with a continuation cursor; client pages to the end" do
    seed("room", 3)
    s = connect("u", :U)

    # First page (size 2).
    Session.handle_inbound(s, %Envelope{type: :sync, conversation_id: "room", seq: 0, count: 2})
    assert_receive {:frame, :U, %Envelope{type: :sync_page, seq: 2, more: true, messages: p1}}
    assert Enum.map(p1, & &1.seq) == [1, 2]

    # Next page, driven by the returned cursor.
    Session.handle_inbound(s, %Envelope{type: :sync, conversation_id: "room", seq: 2, count: 2})
    assert_receive {:frame, :U, %Envelope{type: :sync_page, seq: 3, more: false, messages: p2}}
    assert Enum.map(p2, & &1.seq) == [3]
  end

  test ":sync advances the device cursor over delivered pages (unified with catch-up)" do
    seed("room", 2)
    s = connect("u", :U)
    Session.sync(s)

    Session.handle_inbound(s, %Envelope{type: :sync, conversation_id: "room", seq: 0, count: 10})
    assert_receive {:frame, :U, %Envelope{type: :sync_page, more: false}}
    Session.sync(s)

    assert Cursors.get({"u", "U"}, "room") == 2
  end

  test ":sync page size is capped at :sync_page_max" do
    Application.put_env(:chat_engine, :sync_page_max, 2)
    seed("room", 5)
    s = connect("u", :U)

    # Client asks for 100 but is capped to 2.
    Session.handle_inbound(s, %Envelope{type: :sync, conversation_id: "room", seq: 0, count: 100})
    assert_receive {:frame, :U, %Envelope{type: :sync_page, seq: 2, more: true, messages: msgs}}
    assert length(msgs) == 2
  end
end
