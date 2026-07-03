defmodule Chat.EphemeralTest do
  @moduledoc """
  Live-only (kind: :ephemeral) messages: fanned out to online subscribers but
  never persisted, never assigned a seq, never woken-for, and never in history.
  The non-game path for live feeds, dashboards, presence signals, IoT telemetry.
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
      Application.delete_env(:chat_engine, :offline_queue_adapter)
      Application.delete_env(:chat_engine, :test_offline_pid)

      for sup <- [Chat.Session.Supervisor, Chat.Conversation.Supervisor],
          {_, p, _, _} <- DynamicSupervisor.which_children(sup),
          is_pid(p),
          do: DynamicSupervisor.terminate_child(sup, p)
    end)

    :ok
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

  test "injected ephemeral item reaches an online watcher but is not persisted" do
    w = connect("watcher", :W)
    :ok = Session.subscribe(w, "live:prices")

    assert {:ok, :ephemeral} =
             Chat.inject("live:prices", %Message{
               id: "tick1",
               sender_id: "feed",
               payload: "BTC=42",
               kind: :ephemeral
             })

    assert_receive {:frame, :W,
                    %Envelope{type: :message, id: "tick1", seq: nil, payload: "BTC=42"}}

    # Nothing durable: no history, no seq consumed.
    assert {:ok, []} = Chat.history("live:prices", 0, 50)
    assert {:ok, 0} = Chat.latest_seq("live:prices")
  end

  test "broadcast_ephemeral fans out to a watcher directly — no persist, no seq, no writer" do
    w = connect("watcher2", :W2)
    :ok = Session.subscribe(w, "live:signal")

    assert {:ok, :ephemeral} =
             Chat.broadcast_ephemeral("live:signal", %Message{
               id: "sig1",
               sender_id: "peer",
               payload: "offer-sdp"
             })

    assert_receive {:frame, :W2,
                    %Envelope{type: :message, id: "sig1", seq: nil, payload: "offer-sdp"}}

    # Bypasses the per-conversation writer entirely: nothing durable, no seq.
    assert {:ok, []} = Chat.history("live:signal", 0, 50)
    assert {:ok, 0} = Chat.latest_seq("live:signal")
  end

  test "ephemeral does not consume a seq — durable messages stay gap-free" do
    assert {:ok, 1} = Chat.inject("mix", %Message{id: "d1", sender_id: "s", payload: "a"})

    assert {:ok, :ephemeral} =
             Chat.inject("mix", %Message{id: "e1", sender_id: "s", payload: "x", kind: :ephemeral})

    assert {:ok, 2} = Chat.inject("mix", %Message{id: "d2", sender_id: "s", payload: "b"})

    assert {:ok, log} = Chat.history("mix", 0, 50)
    assert Enum.map(log, & &1.id) == ["d1", "d2"]
  end

  test "ephemeral does not wake offline members" do
    Application.put_env(:chat_engine, :offline_queue_adapter, Chat.OfflinePushTest.Probe)
    Application.put_env(:chat_engine, :test_offline_pid, self())
    :ok = Chat.create_conversation("room", ["feed", "offline_user"])

    assert {:ok, :ephemeral} =
             Chat.inject("room", %Message{
               id: "e1",
               sender_id: "feed",
               payload: "x",
               kind: :ephemeral
             })

    refute_receive {:offline_push, _, _, _}, 200
  end

  test "client :send with kind :ephemeral acks ephemeral and advances no cursor" do
    a = connect("a", :A)
    b = connect("b", :B)
    :ok = Session.subscribe(a, "c")
    :ok = Session.subscribe(b, "c")

    Session.handle_inbound(a, %Envelope{
      type: :send,
      conversation_id: "c",
      id: "m1",
      payload: "typing-ish",
      kind: :ephemeral
    })

    # Sender gets an :ephemeral ack with no seq; the other online member gets it live.
    assert_receive {:frame, :A, %Envelope{type: :ack, id: "m1", seq: nil, status: :ephemeral}}
    assert_receive {:frame, :B, %Envelope{type: :message, id: "m1", seq: nil}}

    Session.sync(a)
    assert Cursors.get({"a", "A"}, "c") == 0
    assert {:ok, 0} = Chat.latest_seq("c")
  end
end
