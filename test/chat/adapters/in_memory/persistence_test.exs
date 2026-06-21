defmodule Chat.Adapters.InMemory.PersistenceTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Chat.Adapters.InMemory.Persistence
  alias Chat.Message

  defp msg(id, payload \\ "x"), do: %Message{id: id, sender_id: "u1", payload: payload}

  describe "basic contract" do
    setup do
      srv = start_supervised!({Persistence, name: nil})
      %{srv: srv}
    end

    test "assigns monotonic per-conversation seq starting at 1", %{srv: s} do
      assert {:ok, 1} = Persistence.append(s, "c1", msg("m1"))
      assert {:ok, 2} = Persistence.append(s, "c1", msg("m2"))
      assert {:ok, 3} = Persistence.append(s, "c1", msg("m3"))
    end

    test "seq is independent per conversation", %{srv: s} do
      assert {:ok, 1} = Persistence.append(s, "c1", msg("m1"))
      assert {:ok, 1} = Persistence.append(s, "c2", msg("m1"))
      assert {:ok, 2} = Persistence.append(s, "c1", msg("m2"))
    end

    test "append is idempotent on message id (same id ⇒ same seq)", %{srv: s} do
      assert {:ok, 1} = Persistence.append(s, "c1", msg("m1"))
      assert {:ok, 1} = Persistence.append(s, "c1", msg("m1"))
      assert {:ok, 1} = Persistence.append(s, "c1", msg("m1", "different payload"))
      assert {:ok, 2} = Persistence.append(s, "c1", msg("m2"))
    end

    test "read_after returns messages strictly after the cursor, ascending", %{srv: s} do
      for i <- 1..5, do: Persistence.append(s, "c1", msg("m#{i}"))
      assert {:ok, msgs} = Persistence.read_after(s, "c1", 2, 100)
      assert Enum.map(msgs, & &1.seq) == [3, 4, 5]
    end

    test "read_after honors the limit", %{srv: s} do
      for i <- 1..10, do: Persistence.append(s, "c1", msg("m#{i}"))
      assert {:ok, msgs} = Persistence.read_after(s, "c1", 0, 3)
      assert Enum.map(msgs, & &1.seq) == [1, 2, 3]
    end

    test "stored message carries its assigned seq and a server timestamp", %{srv: s} do
      assert {:ok, 1} = Persistence.append(s, "c1", msg("m1"))
      assert {:ok, [stored]} = Persistence.read_after(s, "c1", 0, 10)
      assert stored.seq == 1
      assert is_integer(stored.server_ts)
    end

    test "latest_seq tracks the highest assigned seq", %{srv: s} do
      assert {:ok, 0} = Persistence.latest_seq(s, "c1")
      Persistence.append(s, "c1", msg("m1"))
      Persistence.append(s, "c1", msg("m2"))
      assert {:ok, 2} = Persistence.latest_seq(s, "c1")
    end
  end

  # ── CP fencing (append/4 — the optional compare-and-set) ────────────────────
  describe "CP fencing (append/4)" do
    setup do
      srv = start_supervised!({Persistence, name: nil})
      %{srv: srv}
    end

    test "a matching expected_seq commits the next seq", %{srv: s} do
      assert {:ok, 1} = Persistence.append(s, "c", msg("m1"), 0)
      assert {:ok, 2} = Persistence.append(s, "c", msg("m2"), 1)
    end

    test "a stale expected_seq is fenced with the actual current latest", %{srv: s} do
      assert {:ok, 1} = Persistence.append(s, "c", msg("m1"), 0)
      assert {:ok, 2} = Persistence.append(s, "c", msg("m2"), 1)
      assert {:error, {:fenced, 2}} = Persistence.append(s, "c", msg("m3"), 0)
    end

    test "idempotency beats fencing: a replayed id returns its seq despite a stale expected", %{
      srv: s
    } do
      assert {:ok, 1} = Persistence.append(s, "c", msg("m1"), 0)
      assert {:ok, 2} = Persistence.append(s, "c", msg("m2"), 1)
      # m1 replayed with a now-stale expected_seq ⇒ original seq, NOT a fence error
      assert {:ok, 1} = Persistence.append(s, "c", msg("m1"), 0)
    end

    test "two writers racing at the same expected_seq: exactly one wins", %{srv: s} do
      assert {:ok, 1} = Persistence.append(s, "c", msg("a"), 0)
      # both owners believe the latest is 1
      assert {:ok, 2} = Persistence.append(s, "c", msg("b"), 1)
      assert {:error, {:fenced, 2}} = Persistence.append(s, "c", msg("c"), 1)
    end
  end

  # ── Property: the crown-jewel guarantee ─────────────────────────────────────
  # For ANY interleaving of appends across conversations, with arbitrary
  # duplicate ids, the adapter must keep per-conversation seqs contiguous and
  # idempotent. This is the invariant the whole engine rests on.
  property "per-conversation seqs are contiguous & idempotent under arbitrary interleavings" do
    convs = ["c1", "c2", "c3"]
    ids = ["a", "b", "c", "d", "e"]

    check all(ops <- list_of(tuple({member_of(convs), member_of(ids)}), max_length: 60)) do
      {:ok, srv} = Persistence.start_link(name: nil)

      # expected[conv] = %{next: n, seen: %{id => seq}}
      final =
        Enum.reduce(ops, %{}, fn {conv, id}, expected ->
          {:ok, seq} =
            Persistence.append(srv, conv, %Message{id: id, sender_id: "u", payload: "p"})

          st = Map.get(expected, conv, %{next: 1, seen: %{}})

          case Map.get(st.seen, id) do
            nil ->
              # first time we see this id in this conv ⇒ it must get the next seq
              assert seq == st.next
              Map.put(expected, conv, %{next: st.next + 1, seen: Map.put(st.seen, id, seq)})

            prev ->
              # duplicate id ⇒ same seq, and `next` must NOT advance
              assert seq == prev
              expected
          end
        end)

      # read-back is sorted, complete, and matches latest_seq per conversation
      for {conv, st} <- final do
        {:ok, msgs} = Persistence.read_after(srv, conv, 0, 1000)
        seqs = Enum.map(msgs, & &1.seq)

        assert seqs == Enum.sort(seqs)
        assert length(seqs) == map_size(st.seen)
        assert {:ok, map_size(st.seen)} == Persistence.latest_seq(srv, conv)
      end

      GenServer.stop(srv)
    end
  end
end
