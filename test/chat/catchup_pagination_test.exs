defmodule Chat.CatchupPaginationTest do
  @moduledoc """
  P0 correctness (CC-4/REL-1): automatic catch-up must drain the ENTIRE durable
  backlog, not just the first 100 messages.
  """
  use ExUnit.Case, async: false

  alias Chat.Adapters.InMemory.{Persistence, ConversationStore}
  alias Chat.Adapters.TestTransport
  alias Chat.{Message, Session}

  setup do
    ensure_clean(Persistence)
    ensure_clean(ConversationStore)
    on_exit(&drain_dynamic_supervisors/0)
    :ok
  end

  test "a device missing >100 messages receives ALL of them, in order" do
    :ok = Chat.create_conversation("big", ["U"])

    for i <- 1..250 do
      {:ok, _} =
        Persistence.append("big", %Message{id: "m#{i}", sender_id: "src", payload: "p#{i}"})
    end

    u = connect("U", :U)
    # handle_continue(:catch_up) runs before any call is served, so once sync/1
    # returns, every catch-up frame has already been pushed.
    Session.sync(u)

    seqs = collect_message_seqs(:U)
    assert seqs == Enum.to_list(1..250)
  end

  test "cursor is advanced to the end so the SAME device replays nothing on reconnect" do
    :ok = Chat.create_conversation("big2", ["U"])

    for i <- 1..150,
        do:
          {:ok, _} =
            Persistence.append("big2", %Message{id: "m#{i}", sender_id: "s", payload: "p"})

    # Cursors are keyed by {user_id, device_id}, so the reconnect reuses the same
    # device_id; only the transport label differs (to tell the frames apart).
    u1 = connect("U", "dev", :U1)
    Session.sync(u1)
    assert length(collect_message_seqs(:U1)) == 150

    u2 = connect("U", "dev", :U2)
    Session.sync(u2)
    assert collect_message_seqs(:U2) == []
  end

  defp connect(user, label), do: connect(user, to_string(label), label)

  defp connect(user, device_id, label) do
    {:ok, pid} =
      Session.connect(%{
        user_id: user,
        device_id: device_id,
        transport: {TestTransport, {self(), label}}
      })

    pid
  end

  defp collect_message_seqs(label, acc \\ []) do
    receive do
      {:frame, ^label, %Chat.Envelope{type: :message, seq: seq}} ->
        collect_message_seqs(label, [seq | acc])
    after
      0 -> Enum.reverse(acc)
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
