defmodule Chat.Persistence.PortTest do
  @moduledoc """
  Shared contract test-kit for `Chat.Persistence.Port` implementers (DOC-5).

  The persistence port is the load-bearing contract of the whole engine — its
  guarantees (idempotency on `id`, monotonic gap-free `seq`, ordered `read_after`,
  durable-before-ack, and the optional CP fence) are what every other invariant
  rests on. A prose description isn't enough; this module is the **executable
  spec**. Point it at your adapter and it asserts the contract for you:

      defmodule MyApp.Persistence.ContractTest do
        use Chat.Persistence.PortTest, adapter: MyApp.Persistence
      end

  Options:

    * `:adapter` (required) — the module implementing `Chat.Persistence.Port`.
    * `:setup` (optional) — a zero-arity fun run in an ExUnit `setup` block (start
      the adapter, truncate tables, etc.). The adapter must be ready to serve
      calls once it returns.

  Each test uses a fresh, unique `conversation_id`, so the kit is safe to run
  against a shared/persistent store without cross-test interference. If your
  adapter implements the optional `append/3` fence, the fencing tests run
  automatically; otherwise they are skipped.
  """

  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      use ExUnit.Case, async: false

      @adapter Keyword.fetch!(opts, :adapter)
      @setup_fun Keyword.get(opts, :setup)

      if @setup_fun do
        setup do
          @setup_fun.()
          :ok
        end
      end

      defp new_conv_id, do: "contract-conv-#{System.unique_integer([:positive, :monotonic])}"

      defp msg(id, payload \\ "hi") do
        %Chat.Message{id: id, sender_id: "u-#{id}", payload: payload}
      end

      test "latest_seq of an empty conversation is 0" do
        assert {:ok, 0} = @adapter.latest_seq(new_conv_id())
      end

      test "append assigns a monotonic, gap-free seq starting at 1" do
        conv = new_conv_id()
        assert {:ok, 1} = @adapter.append(conv, msg("m1"))
        assert {:ok, 2} = @adapter.append(conv, msg("m2"))
        assert {:ok, 3} = @adapter.append(conv, msg("m3"))
        assert {:ok, 3} = @adapter.latest_seq(conv)
      end

      test "append is idempotent on message id — a replay returns the SAME seq" do
        conv = new_conv_id()
        assert {:ok, 1} = @adapter.append(conv, msg("dup", "first"))
        # Same id, even with a different payload, must NOT assign a new seq.
        assert {:ok, 1} = @adapter.append(conv, msg("dup", "second"))
        assert {:ok, 1} = @adapter.latest_seq(conv)
      end

      test "seq is per-conversation, not global" do
        a = new_conv_id()
        b = new_conv_id()
        assert {:ok, 1} = @adapter.append(a, msg("a1"))
        assert {:ok, 1} = @adapter.append(b, msg("b1"))
        assert {:ok, 2} = @adapter.append(a, msg("a2"))
      end

      test "read_after returns messages strictly after the cursor, in ascending seq" do
        conv = new_conv_id()
        for i <- 1..5, do: {:ok, _} = @adapter.append(conv, msg("m#{i}"))

        assert {:ok, msgs} = @adapter.read_after(conv, 2, 100)
        assert Enum.map(msgs, & &1.seq) == [3, 4, 5]
        assert Enum.map(msgs, & &1.id) == ["m3", "m4", "m5"]
      end

      test "read_after honours the limit and preserves the opaque payload" do
        conv = new_conv_id()
        for i <- 1..5, do: {:ok, _} = @adapter.append(conv, msg("m#{i}", "p#{i}"))

        assert {:ok, msgs} = @adapter.read_after(conv, 0, 2)
        assert Enum.map(msgs, & &1.seq) == [1, 2]
        assert Enum.map(msgs, & &1.payload) == ["p1", "p2"]
      end

      test "read_after past the end is empty" do
        conv = new_conv_id()
        {:ok, _} = @adapter.append(conv, msg("only"))
        assert {:ok, []} = @adapter.read_after(conv, 1, 100)
      end

      # ── Optional CP fence (append/3) ────────────────────────────────────────
      describe "append/3 CP fence" do
        @describetag :fence

        setup do
          unless function_exported?(@adapter, :append, 3) do
            # Adapter opts out of split-brain protection — skip the fence suite.
            :ok
          end

          :ok
        end

        @tag :fence
        test "commits at the expected seq and rejects a stale writer" do
          if function_exported?(@adapter, :append, 3) do
            conv = new_conv_id()
            # Log is empty ⇒ latest seq is 0; a writer expecting 0 wins.
            assert {:ok, 1} = @adapter.append(conv, msg("f1"), 0)
            # A second writer still believing the log is at 0 is fenced out.
            assert {:error, {:fenced, 1}} = @adapter.append(conv, msg("f2"), 0)
            # A writer with the current view (1) commits.
            assert {:ok, 2} = @adapter.append(conv, msg("f2"), 1)
          end
        end

        @tag :fence
        test "idempotency beats fencing — a replayed id returns its seq even when expected is stale" do
          if function_exported?(@adapter, :append, 3) do
            conv = new_conv_id()
            assert {:ok, 1} = @adapter.append(conv, msg("dup"), 0)
            # Replay the same id with a now-stale expected_seq: must return the
            # original {:ok, 1}, NOT {:error, {:fenced, _}} (a retry is not a loss).
            assert {:ok, 1} = @adapter.append(conv, msg("dup"), 0)
          end
        end
      end
    end
  end
end
