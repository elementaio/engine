defmodule Chat.ConcurrentOrderingTest do
  @moduledoc """
  Property test for the single-writer invariant (TST-4): no matter how many
  senders hit ONE conversation concurrently, the owner process assigns a
  gap-free, unique, monotonic `seq` (a total order), and idempotency on
  `message.id` holds even when the duplicates race.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  alias Chat.Adapters.InMemory.{
    Persistence,
    ConversationStore,
    CursorStore,
    PresenceStore,
    ReceiptStore
  }

  alias Chat.Message

  setup do
    Enum.each([Persistence, ConversationStore, CursorStore, PresenceStore, ReceiptStore], fn m ->
      case start_supervised(m) do
        {:ok, _} -> :ok
        {:error, _} -> :ok
      end

      m.reset()
    end)

    on_exit(fn ->
      for {_, p, _, _} <- DynamicSupervisor.which_children(Chat.Conversation.Supervisor),
          is_pid(p),
          do: DynamicSupervisor.terminate_child(Chat.Conversation.Supervisor, p)
    end)

    :ok
  end

  # Fresh conversation per iteration ⇒ iterations don't interfere, no reset needed.
  defp conv_id, do: "concur-#{System.unique_integer([:positive, :monotonic])}"

  # Inject every id concurrently; return %{id => assigned_seq}.
  defp inject_all(conv, ids) do
    ids
    |> Task.async_stream(
      fn id ->
        {:ok, seq} = Chat.inject(conv, %Message{id: id, sender_id: "s", payload: id})
        {id, seq}
      end,
      max_concurrency: 16,
      ordered: false
    )
    |> Map.new(fn {:ok, kv} -> kv end)
  end

  property "concurrent senders to one conversation get a gap-free, unique total order" do
    check all(
            ids <-
              uniq_list_of(string(:alphanumeric, min_length: 1), min_length: 1, max_length: 30),
            max_runs: 60
          ) do
      conv = conv_id()
      assigned = inject_all(conv, ids)

      # Every id got a distinct seq, and the set of seqs is EXACTLY 1..N (no gap,
      # no dup, no overshoot) — the owner serialized all the concurrent writes.
      assert Enum.sort(Map.values(assigned)) == Enum.to_list(1..length(ids))

      # The durable log reads back as a strict ascending run containing each id once.
      {:ok, log} = Chat.history(conv, 0, length(ids) + 1)
      assert Enum.map(log, & &1.seq) == Enum.to_list(1..length(ids))
      assert MapSet.new(log, & &1.id) == MapSet.new(ids)
    end
  end

  property "idempotency holds under concurrency: a racing replay reuses the same seqs" do
    check all(
            ids <-
              uniq_list_of(string(:alphanumeric, min_length: 1), min_length: 1, max_length: 20),
            max_runs: 50
          ) do
      conv = conv_id()

      first = inject_all(conv, ids)
      # A second concurrent round with the SAME ids must assign NO new seqs.
      second = inject_all(conv, ids)

      assert first == second
      assert {:ok, length(ids)} == Chat.latest_seq(conv)
    end
  end
end
