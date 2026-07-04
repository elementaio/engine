defmodule Chat.AuditFixesTest do
  @moduledoc """
  Regression tests for the 2026-07 lead-review assessment fixes
  (`docs/ASSESSMENT-2026-07.md`): the membership gate (B1), the contiguous
  delivery-cursor invariant (B3/B4/B6), the ephemeral payload cap (B11), the
  user-targeted authz contract (B10), drain-aware placement (B12), and the
  graceful-stop primitive (B16).
  """
  use ExUnit.Case, async: false

  alias Chat.Adapters.InMemory.{Persistence, ConversationStore, CursorStore}
  alias Chat.Adapters.TestTransport
  alias Chat.{Cluster, Envelope, Health, Session}

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

  defp send_msg(session, conv, id, payload) do
    Session.handle_inbound(session, %Envelope{
      type: :send,
      conversation_id: conv,
      id: id,
      payload: payload
    })

    Session.sync(session)
  end

  # ── B1 — membership gate ──────────────────────────────────────────────────────

  describe "B1 enforce_membership" do
    setup do
      Application.put_env(:chat_engine, :enforce_membership, true)
      on_exit(fn -> Application.delete_env(:chat_engine, :enforce_membership) end)
    end

    test "a non-member cannot sync another conversation's history" do
      :ok = Chat.create_conversation("secret", ["alice", "bob"])
      a = connect("alice", :alice)
      send_msg(a, "secret", "m1", "top secret")

      # eve is authenticated in the tenant but NOT a member of "secret".
      eve = connect("eve", :eve)
      Session.handle_inbound(eve, %Envelope{type: :sync, conversation_id: "secret", seq: 0})
      Session.sync(eve)

      assert_receive {:frame, :eve, %Envelope{type: :error, reason: :forbidden}}
      refute_receive {:frame, :eve, %Envelope{type: :sync_page}}
    end

    test "a non-member cannot send into a conversation" do
      :ok = Chat.create_conversation("secret", ["alice", "bob"])
      eve = connect("eve", :eve)

      Session.handle_inbound(eve, %Envelope{
        type: :send,
        conversation_id: "secret",
        id: "x",
        payload: "hi"
      })

      Session.sync(eve)

      assert_receive {:frame, :eve, %Envelope{type: :error, reason: :forbidden}}
      assert {:ok, 0} = Persistence.latest_seq("secret")
    end

    test "a member is unaffected" do
      :ok = Chat.create_conversation("room", ["alice", "bob"])
      a = connect("alice", :alice)
      send_msg(a, "room", "m1", "hello")
      assert_receive {:frame, :alice, %Envelope{type: :ack, seq: 1}}
    end
  end

  # ── B4 — :read must not move the delivery cursor ──────────────────────────────

  test "B4: a client :read ahead of delivery does NOT advance the device cursor" do
    :ok = Chat.create_conversation("c", ["A", "B"])
    a = connect("A", :A)
    b = connect("B", :B)

    send_msg(a, "c", "m1", "one")
    assert_receive {:frame, :B, %Envelope{type: :message, seq: 1}}
    assert {:ok, 1} = CursorStore.get({"B", "B"}, "c")

    # B optimistically reports reading up to seq 9 (cross-device read-sync).
    Session.handle_inbound(b, %Envelope{type: :read, conversation_id: "c", seq: 9})
    Session.sync(b)

    # Delivery cursor must stay at what was actually delivered (1), not jump to 9.
    assert {:ok, 1} = CursorStore.get({"B", "B"}, "c")
  end

  # ── B3/B6 — contiguous high-water cursor ──────────────────────────────────────

  test "B3: an out-of-order delivery does not leapfrog the cursor past a gap" do
    :ok = Chat.create_conversation("c", ["A", "B"])
    b = connect("B", :B)
    ref = {"B", "B"}

    # Simulate a shed lower-seq delivery: seq 3 arrives while 1 and 2 have not.
    deliver_message(b, "c", 3)
    assert {:ok, 0} = CursorStore.get(ref, "c"), "cursor must not advance over the gap"

    # The gap fills: 1 then 2 arrive; the cursor now folds in the buffered 3.
    deliver_message(b, "c", 1)
    assert {:ok, 1} = CursorStore.get(ref, "c")

    deliver_message(b, "c", 2)
    assert {:ok, 3} = CursorStore.get(ref, "c"), "cursor absorbs the contiguous run 1,2,3"
  end

  defp deliver_message(session, conv, seq) do
    Session.deliver(session, %Envelope{
      type: :message,
      conversation_id: conv,
      id: "s#{seq}",
      sender_id: "A",
      seq: seq,
      payload: "p#{seq}",
      receipts: false
    })

    Session.sync(session)
  end

  # ── B11 — ephemeral payload cap ───────────────────────────────────────────────

  test "B11: broadcast_ephemeral enforces the payload-size cap" do
    Application.put_env(:chat_engine, :max_payload_bytes, 16)
    on_exit(fn -> Application.delete_env(:chat_engine, :max_payload_bytes) end)

    small = %Chat.Message{id: "e1", sender_id: "A", payload: "tiny"}
    big = %Chat.Message{id: "e2", sender_id: "A", payload: String.duplicate("x", 100)}

    assert {:ok, :ephemeral} = Chat.broadcast_ephemeral("c", small)
    assert {:error, :too_large} = Chat.broadcast_ephemeral("c", big)
  end

  # ── B10 — user-targeted authz contract ────────────────────────────────────────

  test "B10: presence_query authorizes on a {:user, id} resource, not a bare id" do
    Application.put_env(:chat_engine, :auth_adapter, __MODULE__.CapturingAuth)
    __MODULE__.CapturingAuth.capture_to(self())

    on_exit(fn ->
      Application.put_env(:chat_engine, :auth_adapter, Chat.Adapters.InMemory.Auth)
    end)

    q = connect("querier", :q)
    Session.handle_inbound(q, %Envelope{type: :presence_query, user_id: "target"})
    Session.sync(q)

    assert_receive {:authorized, :presence_query, {:user, "target"}}
  end

  defmodule CapturingAuth do
    @behaviour Chat.Auth.Port
    def capture_to(pid), do: :persistent_term.put({__MODULE__, :sink}, pid)
    @impl true
    def authenticate(_), do: {:error, :unused}
    @impl true
    def authorize(action, _user, resource) do
      case :persistent_term.get({__MODULE__, :sink}, nil) do
        nil -> :ok
        pid -> send(pid, {:authorized, action, resource})
      end

      :ok
    end
  end

  # ── B12 / B16 — drain-aware placement + graceful-stop primitive ───────────────

  test "B12: a draining node advertises itself and still owns when it is the only node" do
    refute MapSet.member?(Cluster.draining_nodes(), Node.self())

    Health.drain()
    assert MapSet.member?(Cluster.draining_nodes(), Node.self())
    # Single node: placement must fall back to it rather than return an empty set.
    assert Cluster.owner_node("anything") == Node.self()

    Health.resume()
    refute MapSet.member?(Cluster.draining_nodes(), Node.self())
  end

  test "B16: await_drained returns :ok once sessions are gone" do
    a = connect("A", :A)
    assert Health.live_session_count() >= 1

    Session.disconnect(a)
    assert :ok = Chat.await_drained(2_000, 25)
    assert Health.live_session_count() == 0
    Health.resume()
  end

  defp drain do
    for sup <- [Chat.Session.Supervisor, Chat.Conversation.Supervisor],
        {_, pid, _, _} <- DynamicSupervisor.which_children(sup),
        is_pid(pid) do
      DynamicSupervisor.terminate_child(sup, pid)
    end
  end
end
