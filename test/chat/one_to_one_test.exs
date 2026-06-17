defmodule Chat.OneToOneTest do
  @moduledoc """
  End-to-end M1 flow through the real core processes (Session → Conversation →
  Persistence → fan-out), using the in-memory adapters and a test transport.
  Deterministic, no network.
  """
  use ExUnit.Case, async: false

  alias Chat.Adapters.InMemory.{Persistence, ConversationStore}
  alias Chat.Adapters.TestTransport
  alias Chat.{Envelope, Session}

  setup do
    # Start the adapters if they aren't already running (the body/app may have
    # started them), then wipe them so every test gets fresh durable state.
    ensure_clean(Persistence)
    ensure_clean(ConversationStore)

    # The core's sessions/conversations live under the running :chat_engine app.
    # Tear them down between tests so user/conversation ids can be reused.
    on_exit(&drain_dynamic_supervisors/0)
    :ok
  end

  defp ensure_clean(mod) do
    case start_supervised(mod) do
      {:ok, _} -> :ok
      # already started (e.g. by the body's app) — reuse it
      {:error, _} -> :ok
    end

    mod.reset()
  end

  defp connect(user, label) do
    {:ok, pid} =
      Session.connect(%{user_id: user, device_id: to_string(label), transport: {TestTransport, {self(), label}}})

    pid
  end

  defp send_msg(session, conv, id, payload) do
    Session.handle_inbound(session, %Envelope{type: :send, conversation_id: conv, id: id, payload: payload})
    Session.sync(session)
  end

  test "A → B: B receives the message; A gets server_received then delivered" do
    :ok = Chat.create_conversation("c1", ["A", "B"])
    a = connect("A", :A)
    _b = connect("B", :B)

    send_msg(a, "c1", "m1", "hi B")

    # A's own client: the server ack carrying the assigned seq
    assert_receive {:frame, :A, %Envelope{type: :ack, id: "m1", seq: 1, status: :server_received}}

    # B's client: the delivered message
    assert_receive {:frame, :B,
                    %Envelope{type: :message, id: "m1", seq: 1, sender_id: "A", payload: "hi B"}}

    # A's client: a `delivered` receipt, auto-emitted when B's edge received it
    assert_receive {:frame, :A, %Envelope{type: :receipt, status: :delivered, seq: 1, sender_id: "B"}}
  end

  test "seq increases monotonically across a back-and-forth" do
    :ok = Chat.create_conversation("c1", ["A", "B"])
    a = connect("A", :A)
    b = connect("B", :B)

    send_msg(a, "c1", "m1", "one")
    assert_receive {:frame, :A, %Envelope{type: :ack, seq: 1}}
    send_msg(b, "c1", "m2", "two")
    assert_receive {:frame, :B, %Envelope{type: :ack, seq: 2}}
    send_msg(a, "c1", "m3", "three")
    assert_receive {:frame, :A, %Envelope{type: :ack, seq: 3}}
  end

  test "read receipt flows back to the sender" do
    :ok = Chat.create_conversation("c1", ["A", "B"])
    a = connect("A", :A)
    b = connect("B", :B)

    send_msg(a, "c1", "m1", "hi")
    assert_receive {:frame, :B, %Envelope{type: :message, seq: 1}}

    Session.handle_inbound(b, %Envelope{type: :read, conversation_id: "c1", seq: 1})
    assert_receive {:frame, :A, %Envelope{type: :receipt, status: :read, seq: 1, sender_id: "B"}}
  end

  test "offline recipient: message is persisted and delivered via sync on reconnect" do
    :ok = Chat.create_conversation("c2", ["A", "B"])
    a = connect("A", :A)
    # B is NOT connected yet.
    send_msg(a, "c2", "m1", "you there?")
    assert_receive {:frame, :A, %Envelope{type: :ack, seq: 1}}

    # B connects later and asks for everything since seq 0.
    b = connect("B", :B)
    Session.handle_inbound(b, %Envelope{type: :sync, conversation_id: "c2", seq: 0})

    assert_receive {:frame, :B,
                    %Envelope{type: :sync_page, messages: [%{id: "m1", seq: 1, payload: "you there?"}]}}
  end

  test "multi-device: a user's second device also receives the message" do
    :ok = Chat.create_conversation("c3", ["A", "B"])
    a = connect("A", :A)
    _b1 = connect("B", :B1)
    _b2 = connect("B", :B2)

    send_msg(a, "c3", "m1", "hello")
    assert_receive {:frame, :B1, %Envelope{type: :message, seq: 1}}
    assert_receive {:frame, :B2, %Envelope{type: :message, seq: 1}}
  end

  test "idempotent resend: same message id ⇒ same seq, delivered once more but no new seq" do
    :ok = Chat.create_conversation("c4", ["A", "B"])
    a = connect("A", :A)
    _b = connect("B", :B)

    send_msg(a, "c4", "m1", "hi")
    assert_receive {:frame, :A, %Envelope{type: :ack, id: "m1", seq: 1}}

    # client retries the same id (e.g. after a flaky ack)
    send_msg(a, "c4", "m1", "hi")
    assert_receive {:frame, :A, %Envelope{type: :ack, id: "m1", seq: 1}}

    assert {:ok, 1} = Chat.latest_seq("c4")
  end

  defp drain_dynamic_supervisors do
    for sup <- [Chat.Session.Supervisor, Chat.Conversation.Supervisor],
        {_, pid, _, _} <- DynamicSupervisor.which_children(sup),
        is_pid(pid) do
      DynamicSupervisor.terminate_child(sup, pid)
    end
  end
end
