defmodule Chat.ResilienceTest do
  @moduledoc """
  P0 reliability: backpressure (overload shedding), payload size cap, and
  degrade-not-crash on `{:error, _}` port returns.
  """
  use ExUnit.Case, async: false

  alias Chat.Adapters.InMemory.{Persistence, ConversationStore}
  alias Chat.Adapters.TestTransport
  alias Chat.{Envelope, Session}

  defmodule FaultyReceiptStore do
    @behaviour Chat.ReceiptStore.Port
    @impl true
    def set_read(_c, _u, _s), do: {:error, :down}
    @impl true
    def read_watermarks(_c), do: {:error, :down}
  end

  setup do
    ensure_clean(Persistence)
    ensure_clean(ConversationStore)

    saved =
      for k <- [:max_mailbox, :max_payload_bytes, :receipt_store_adapter] do
        {k, Application.get_env(:chat_engine, k)}
      end

    on_exit(fn ->
      for {k, v} <- saved do
        if is_nil(v),
          do: Application.delete_env(:chat_engine, k),
          else: Application.put_env(:chat_engine, k, v)
      end

      drain_dynamic_supervisors()
    end)

    :ok
  end

  test "an overloaded session sheds inbound with {:error, :overloaded}" do
    # A negative bound makes any queue length (≥0) count as overloaded, so the
    # shedding guard is exercised deterministically.
    Application.put_env(:chat_engine, :max_mailbox, -1)
    a = connect("A", :A)

    assert {:error, :overloaded} =
             Session.handle_inbound(a, %Envelope{
               type: :send,
               conversation_id: "c",
               id: "m",
               payload: "x"
             })
  end

  test "an oversized payload is rejected with :too_large and not persisted" do
    Application.put_env(:chat_engine, :max_payload_bytes, 4)
    :ok = Chat.create_conversation("c", ["A"])
    a = connect("A", :A)

    Session.handle_inbound(a, %Envelope{
      type: :send,
      conversation_id: "c",
      id: "m1",
      payload: "way too big"
    })

    Session.sync(a)

    assert_receive {:frame, :A, %Envelope{type: :error, id: "m1", reason: :too_large}}
    assert {:ok, 0} = Chat.latest_seq("c")
  end

  test ":read_state degrades to count 0 when the receipt store errors (no crash)" do
    Application.put_env(:chat_engine, :receipt_store_adapter, FaultyReceiptStore)
    :ok = Chat.create_conversation("c", ["A"])
    a = connect("A", :A)

    Session.handle_inbound(a, %Envelope{type: :read_state, conversation_id: "c", seq: 1})
    Session.sync(a)

    assert_receive {:frame, :A, %Envelope{type: :read_state, count: 0, readers: []}}
    assert Process.alive?(a)
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

  defp ensure_clean(mod) do
    case start_supervised(mod) do
      {:ok, _} -> :ok
      {:error, _} -> :ok
    end

    mod.reset()
  end

  defp drain_dynamic_supervisors do
    for sup <- [Chat.Session.Supervisor, Chat.Conversation.Supervisor],
        {_, pid, _, _} <- DynamicSupervisor.which_children(sup),
        is_pid(pid) do
      DynamicSupervisor.terminate_child(sup, pid)
    end
  end
end
