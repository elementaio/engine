defmodule Chat.OfflinePushTest.Probe do
  @moduledoc false
  # Test OfflineQueue adapter: forwards each wake to the test process so timing-
  # tolerant assert_receive/refute_receive can observe the (async) notifications.
  @behaviour Chat.OfflineQueue.Port

  @impl true
  def notify(user_id, conversation_id, msg) do
    case Application.get_env(:chat_engine, :test_offline_pid) do
      pid when is_pid(pid) -> send(pid, {:offline_push, user_id, conversation_id, msg})
      _ -> :ok
    end

    :ok
  end
end

defmodule Chat.OfflinePushTest do
  use ExUnit.Case, async: false

  alias Chat.Adapters.InMemory.{
    Persistence,
    ConversationStore,
    CursorStore,
    PresenceStore,
    ReceiptStore
  }

  alias Chat.Adapters.TestTransport
  alias Chat.{Message, Session}

  setup do
    Enum.each([Persistence, ConversationStore, CursorStore, PresenceStore, ReceiptStore], fn m ->
      case start_supervised(m) do
        {:ok, _} -> :ok
        {:error, _} -> :ok
      end

      m.reset()
    end)

    Chat.Presence.reset()

    # Wire the probe as the OfflineQueue adapter for the duration of the test.
    Application.put_env(:chat_engine, :offline_queue_adapter, Chat.OfflinePushTest.Probe)
    Application.put_env(:chat_engine, :test_offline_pid, self())

    on_exit(fn ->
      Application.delete_env(:chat_engine, :offline_queue_adapter)
      Application.delete_env(:chat_engine, :test_offline_pid)
      Application.delete_env(:chat_engine, :offline_push_max_members)

      for sup <- [Chat.Session.Supervisor, Chat.Conversation.Supervisor],
          {_, p, _, _} <- DynamicSupervisor.which_children(sup),
          is_pid(p),
          do: DynamicSupervisor.terminate_child(sup, p)
    end)

    :ok
  end

  defp connect(user) do
    {:ok, pid} =
      Session.connect(%{
        user_id: user,
        device_id: "d",
        transport: {TestTransport, {self(), user}}
      })

    pid
  end

  test "offline members are woken; the online member and the sender are not" do
    :ok = Chat.create_conversation("team", ["sender", "online", "offline"])
    _ = connect("online")

    {:ok, 1} =
      Chat.inject("team", %Message{id: "m1", sender_id: "sender", payload: "hi"})

    # The single offline member is notified, with the assigned seq on the message.
    assert_receive {:offline_push, "offline", "team", %Message{id: "m1", seq: 1}}, 500

    # The online member got the live fan-out, the sender authored it — neither is pushed.
    refute_received {:offline_push, "online", _, _}
    refute_received {:offline_push, "sender", _, _}
  end

  test "a fully-offline 1:1 recipient is woken on a sent message" do
    :ok = Chat.create_conversation("dm", ["a", "b"])
    a = connect("a")

    # `a` sends to `b`, who has no session.
    Session.handle_inbound(a, %Chat.Envelope{
      type: :send,
      conversation_id: "dm",
      id: "x1",
      payload: "yo"
    })

    assert_receive {:offline_push, "b", "dm", %Message{id: "x1"}}, 500
    refute_received {:offline_push, "a", _, _}
  end

  test "rosters above :offline_push_max_members are skipped (no per-member push)" do
    Application.put_env(:chat_engine, :offline_push_max_members, 1)
    :ok = Chat.create_conversation("big", ["sender", "u1", "u2", "u3"])

    {:ok, 1} = Chat.inject("big", %Message{id: "m1", sender_id: "sender", payload: "hi"})

    refute_receive {:offline_push, _, "big", _}, 300
  end

  test "no offline adapter configured ⇒ no-op (no crash)" do
    Application.delete_env(:chat_engine, :offline_queue_adapter)
    :ok = Chat.create_conversation("quiet", ["sender", "offline"])

    assert {:ok, 1} = Chat.inject("quiet", %Message{id: "m1", sender_id: "sender", payload: "hi"})
    refute_receive {:offline_push, _, _, _}, 200
  end
end
