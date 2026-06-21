defmodule Chat.FencingTest do
  @moduledoc """
  P0 distributed safety (DF-1): when the persistence CP-fence rejects the owner's
  write (it was overtaken — split-brain), the owner must NOT fan out a divergent
  message; it steps down so a fresh owner is re-elected after the heal. A
  non-fence durability error must NOT ack (preserving at-least-once) but must keep
  the owner alive.

  Single-node note: the bundled in-memory adapter is per-node, so real split-brain
  cannot be reproduced here — this exercises the OWNER's reaction to the fence
  interface via a fault-injecting adapter. The real split-brain test belongs in a
  body's shared-backend (linearizable) adapter suite.
  """
  use ExUnit.Case, async: false

  alias Chat.Adapters.InMemory.{Persistence, ConversationStore}
  alias Chat.Adapters.TestTransport
  alias Chat.{Envelope, Session}

  # Delegates to the in-memory singleton but can be armed (via app env) to fail
  # the next append — either with a fence or a generic durability error.
  defmodule FaultyPersistence do
    @behaviour Chat.Persistence.Port

    @impl true
    def append(conv, msg), do: do_append(fn -> Persistence.append(conv, msg) end)

    @impl true
    def append(conv, msg, expected),
      do: do_append(fn -> Persistence.append(conv, msg, expected) end)

    @impl true
    def read_after(conv, after_seq, limit), do: Persistence.read_after(conv, after_seq, limit)

    @impl true
    def latest_seq(conv), do: Persistence.latest_seq(conv)

    defp do_append(real) do
      case Application.get_env(:chat_engine, :test_fault) do
        nil -> real.()
        :fenced -> {:error, {:fenced, 99}}
        reason -> {:error, reason}
      end
    end
  end

  setup do
    ensure_clean(Persistence)
    ensure_clean(ConversationStore)
    prev = Application.get_env(:chat_engine, :persistence_adapter)
    Application.put_env(:chat_engine, :persistence_adapter, FaultyPersistence)

    on_exit(fn ->
      Application.delete_env(:chat_engine, :test_fault)
      Application.put_env(:chat_engine, :persistence_adapter, prev)
      drain_dynamic_supervisors()
    end)

    :ok
  end

  test "fenced submit: sender gets :error (not :ack), no fan-out, owner steps down" do
    :ok = Chat.create_conversation("f1", ["A", "B"])
    a = connect("A", :A)
    _b = connect("B", :B)

    Application.put_env(:chat_engine, :test_fault, :fenced)

    Session.handle_inbound(a, %Envelope{
      type: :send,
      conversation_id: "f1",
      id: "m1",
      payload: "x"
    })

    Session.sync(a)

    assert_receive {:frame, :A, %Envelope{type: :error, id: "m1", reason: :fenced}}
    refute_received {:frame, :A, %Envelope{type: :ack}}
    refute_received {:frame, :B, %Envelope{type: :message}}
    # The owner stepped down; Registry prunes via its monitor asynchronously.
    assert eventually(fn -> Registry.lookup(Chat.ConversationRegistry, "f1") == [] end)
  end

  test "non-fence persist error: sender gets :error (not :ack), owner stays alive" do
    :ok = Chat.create_conversation("f2", ["A"])
    a = connect("A", :A)

    Application.put_env(:chat_engine, :test_fault, :db_down)

    Session.handle_inbound(a, %Envelope{
      type: :send,
      conversation_id: "f2",
      id: "m1",
      payload: "x"
    })

    Session.sync(a)

    assert_receive {:frame, :A, %Envelope{type: :error, id: "m1", reason: :db_down}}
    refute_received {:frame, :A, %Envelope{type: :ack}}
    assert [{_pid, _}] = Registry.lookup(Chat.ConversationRegistry, "f2")
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

  defp eventually(fun, retries \\ 50) do
    cond do
      fun.() -> true
      retries == 0 -> false
      true -> Process.sleep(10) && eventually(fun, retries - 1)
    end
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
