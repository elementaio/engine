# Adapter tests against a REAL Locus server (../locus/target/release/locus,
# spawned per test run on a free port). Tagged :locus — test_helper excludes
# the tag automatically when the binary isn't present, so the engine suite
# stays self-contained on machines without a Locus checkout.

defmodule Chat.Adapters.LocusBoot do
  @moduledoc false
  # Boots one Locus + the adapter pool for the whole test run, idempotently
  # (both the contract kit's setup and the direct suites call ensure/0).

  alias Chat.Adapters.Locus

  def binary do
    Path.expand("../locus/target/release/locus", File.cwd!())
  end

  def ensure do
    case :persistent_term.get(__MODULE__, nil) do
      nil -> boot()
      _ -> :ok
    end

    :ok
  end

  defp boot do
    port = free_port()
    rdb = Path.join(System.tmp_dir!(), "vox-locus-test-#{System.os_time(:millisecond)}.rdb")

    spawned =
      Port.open({:spawn_executable, binary()}, [
        :binary,
        :exit_status,
        env: [
          {~c"LOCUS_PORT", String.to_charlist(Integer.to_string(port))},
          {~c"LOCUS_RDB", String.to_charlist(rdb)}
        ]
      ])

    os_pid = spawned |> Port.info(:os_pid) |> elem(1)
    await_ready(port, 100)

    Application.put_env(:chat_engine, :locus, host: "127.0.0.1", port: port, pool_size: 2)

    # Start the pool from an immortal holder process — start_link from a test's
    # setup would tie the supervisor's life to the FIRST test that runs it.
    holder = self()

    spawn(fn ->
      {:ok, _} = Locus.start_link([])
      send(holder, :pool_up)
      Process.sleep(:infinity)
    end)

    receive do
      :pool_up -> :ok
    after
      5_000 -> raise "adapter pool never started"
    end

    System.at_exit(fn _ ->
      System.cmd("kill", ["-9", Integer.to_string(os_pid)], stderr_to_stdout: true)
      File.rm(rdb)
    end)

    :persistent_term.put(__MODULE__, %{port: port, os_pid: os_pid})
  end

  defp free_port do
    {:ok, sock} = :gen_tcp.listen(0, [])
    {:ok, port} = :inet.port(sock)
    :gen_tcp.close(sock)
    port
  end

  defp await_ready(_port, 0), do: raise("locus never came up")

  defp await_ready(port, n) do
    case :gen_tcp.connect(~c"127.0.0.1", port, [:binary, active: false], 200) do
      {:ok, sock} ->
        :gen_tcp.close(sock)

      {:error, _} ->
        Process.sleep(50)
        await_ready(port, n - 1)
    end
  end
end

# ── The executable persistence spec, pointed at the Locus adapter ─────────────

defmodule Chat.Adapters.Locus.PersistenceContractTest do
  alias Chat.Adapters.LocusBoot

  use Chat.Persistence.PortTest,
    adapter: Chat.Adapters.Locus.Persistence,
    setup: &LocusBoot.ensure/0

  @moduletag :locus
end

# ── Direct suites for the other five ports ───────────────────────────────────

defmodule Chat.Adapters.Locus.StoresTest do
  use ExUnit.Case, async: false

  @moduletag :locus

  alias Chat.Adapters.Locus
  alias Chat.Adapters.LocusBoot

  alias Chat.Adapters.Locus.{
    ConversationStore,
    CursorStore,
    OfflineQueue,
    PresenceStore,
    ReceiptStore
  }

  setup do
    LocusBoot.ensure()
    :ok
  end

  defp uniq(tag), do: "#{tag}-#{System.unique_integer([:positive, :monotonic])}"

  test "conversation membership, reverse index, O(1) count, paged roster" do
    conv = uniq("conv")
    other = uniq("conv")

    refute ConversationStore.member?(conv, "alice")
    assert :ok = ConversationStore.add_member(conv, "alice")
    assert :ok = ConversationStore.add_member(conv, "bob")
    assert :ok = ConversationStore.add_member(other, "alice")
    assert ConversationStore.member?(conv, "alice")
    assert {:ok, 2} = ConversationStore.member_count(conv)

    {:ok, convs} = ConversationStore.conversations_for("alice")
    assert Enum.sort(convs) |> Enum.filter(&(&1 in [conv, other])) == Enum.sort([conv, other])

    assert :ok = ConversationStore.remove_member(conv, "bob")
    refute ConversationStore.member?(conv, "bob")
    assert {:ok, 1} = ConversationStore.member_count(conv)

    # Paged roster streams every member exactly once, never the whole set.
    big = uniq("big")
    members = for i <- 1..250, do: "user-#{i}"
    Enum.each(members, &ConversationStore.add_member(big, &1))
    assert {:ok, 250} = ConversationStore.member_count(big)

    collected = collect_pages(big, nil, [])
    assert Enum.sort(collected) == Enum.sort(members)
  end

  defp collect_pages(conv, cursor, acc) do
    {:ok, page, next} = ConversationStore.stream_members(conv, cursor, 50)
    acc = acc ++ page
    if next, do: collect_pages(conv, next, acc), else: acc
  end

  test "cursor advance is monotonic max per (device, conversation)" do
    conv = uniq("conv")
    ref = {"alice", "phone-1"}

    assert {:ok, 0} = CursorStore.get(ref, conv)
    assert :ok = CursorStore.advance(ref, conv, 5)
    assert {:ok, 5} = CursorStore.get(ref, conv)
    # Never moves backwards.
    assert :ok = CursorStore.advance(ref, conv, 3)
    assert {:ok, 5} = CursorStore.get(ref, conv)
    assert :ok = CursorStore.advance(ref, conv, 9)
    assert {:ok, 9} = CursorStore.get(ref, conv)

    # Another device of the same user is independent.
    assert {:ok, 0} = CursorStore.get({"alice", "laptop"}, conv)
  end

  test "presence last-seen is monotonic and nil for strangers" do
    user = uniq("user")
    assert {:ok, nil} = PresenceStore.last_seen(user)
    assert :ok = PresenceStore.touch(user, 1_000)
    assert :ok = PresenceStore.touch(user, 500)
    assert {:ok, 1_000} = PresenceStore.last_seen(user)
  end

  test "read watermarks aggregate per conversation, monotonic per user" do
    conv = uniq("conv")
    assert {:ok, %{}} = ReceiptStore.read_watermarks(conv)

    assert :ok = ReceiptStore.set_read(conv, "alice", 4)
    assert :ok = ReceiptStore.set_read(conv, "bob", 2)
    # stale — must not regress
    assert :ok = ReceiptStore.set_read(conv, "alice", 3)

    assert {:ok, %{"alice" => 4, "bob" => 2}} = ReceiptStore.read_watermarks(conv)
  end

  test "offline wake lands as a JSON job a BLPOP worker can drain" do
    user = uniq("user")
    conv = uniq("conv")

    msg = %Chat.Message{id: "m-1", sender_id: "bob", payload: "secret", seq: 7, server_ts: 123}
    assert :ok = OfflineQueue.notify(user, conv, msg)

    {:ok, [_key, job]} = Locus.command(["BLPOP", Locus.wake_key(), "1"])

    assert job =~ ~s("user_id":"#{user}")
    assert job =~ ~s("conversation_id":"#{conv}")
    assert job =~ ~s("message_id":"m-1")
    assert job =~ ~s("seq":7)
    # A wake is ids + seq only — never the payload.
    refute job =~ "secret"
  end

  test "fenced appends from two racing writers keep the log gap-free" do
    # Simulates the split-brain scenario the fence exists for: two "owners"
    # interleave fenced appends; every accepted seq is unique and dense, every
    # loser learns the real seq to catch up from.
    conv = uniq("conv")
    adapter = Chat.Adapters.Locus.Persistence

    results =
      for i <- 1..20 do
        Task.async(fn ->
          msg = %Chat.Message{id: "race-#{i}", sender_id: "u", payload: "p"}
          # Each writer reads the seq it believes is current, then fences on it.
          {:ok, believed} = adapter.latest_seq(conv)
          adapter.append(conv, msg, believed)
        end)
      end
      |> Task.await_many(10_000)

    won = for {:ok, seq} <- results, do: seq
    assert won != []
    assert won == Enum.uniq(won)

    # The log is dense 1..N with no holes regardless of who lost.
    {:ok, latest} = adapter.latest_seq(conv)
    {:ok, msgs} = adapter.read_after(conv, 0, 100)
    assert Enum.map(msgs, & &1.seq) == Enum.to_list(1..latest)
    assert Enum.sort(won) == Enum.uniq(Enum.sort(won))
  end
end
