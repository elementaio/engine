defmodule Chat.InjectTest do
  @moduledoc "The engine change Pulsar needs: publish into a channel from a non-session publisher."
  use ExUnit.Case, async: false

  alias Chat.Adapters.InMemory.{
    Persistence,
    ConversationStore,
    CursorStore,
    PresenceStore,
    ReceiptStore
  }

  alias Chat.Adapters.TestTransport
  alias Chat.{Envelope, Message, Session}

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
      for sup <- [Chat.Session.Supervisor, Chat.Conversation.Supervisor],
          {_, p, _, _} <- DynamicSupervisor.which_children(sup),
          is_pid(p),
          do: DynamicSupervisor.terminate_child(sup, p)
    end)

    :ok
  end

  test "an injected item is fanned out to a watcher and kept in the recent buffer" do
    {:ok, w} =
      Session.connect(%{
        user_id: "watcher",
        device_id: "d",
        transport: {TestTransport, {self(), :W}}
      })

    :ok = Session.subscribe(w, "topic:gaza")

    {:ok, 1} =
      Chat.inject("topic:gaza", %Message{
        id: "i1",
        sender_id: "bluesky",
        payload: ~s({"text":"breaking"})
      })

    # watcher gets it live, with receipts suppressed (it's a feed item, not a DM)
    assert_receive {:frame, :W,
                    %Envelope{
                      type: :message,
                      id: "i1",
                      sender_id: "bluesky",
                      receipts: false,
                      payload: payload
                    }}

    assert payload =~ "breaking"

    # and it's in the recent window — which is what a later watcher is replayed on watch
    assert {:ok, [%Message{id: "i1"}]} = Chat.history("topic:gaza", 0, 50)
  end

  test "injection assigns a monotonic seq per topic and is idempotent on id" do
    assert {:ok, 1} = Chat.inject("topic:ai", %Message{id: "a", sender_id: "s", payload: "1"})
    assert {:ok, 2} = Chat.inject("topic:ai", %Message{id: "b", sender_id: "s", payload: "2"})
    assert {:ok, 1} = Chat.inject("topic:ai", %Message{id: "a", sender_id: "s", payload: "1"})
  end
end
