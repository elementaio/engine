defmodule Chat.Adapters.Locus.Persistence do
  @moduledoc """
  `Chat.Persistence.Port` on Locus — the conversation log as a Locus **stream**
  whose entry ids *are* the seqs.

  ## Data model (per conversation `c`)

    * `vox:{c}:seq` — the latest assigned seq (string int; the fence authority).
    * `vox:{c}:log` — a stream; entry id `<seq>-0`, fields = the message.
    * `vox:{c}:ids` — hash `message.id -> seq` (idempotency).

  ## Why the contract holds

    * **Atomicity** — seq bump + log append + idempotency record commit in ONE
      `MULTI`/`EXEC` (one Locus hub turn). There is no crash window where the
      seq advanced but the message is missing, or the message landed without
      its idempotency record — so seqs stay **gap-free** and retries stay
      idempotent.
    * **Fence (`append/3`)** — the transaction `WATCH`es the seq key and is
      built against the read seq; if any other writer moved it, `EXEC` refuses
      (nil) and the caller gets `{:error, {:fenced, current}}`. The check and
      the commit are one linearizable conditional write *in the store*, which
      is exactly what the port demands (a read-then-blind-write would not be).
      The `WATCH … EXEC` conversation runs inside `Client.exclusive/2`, so no
      other caller can interleave on that socket and disturb the watch.
    * **Belt and braces** — entry ids must strictly increase, so even a buggy
      writer cannot make the stream disagree with the seq key inside a
      transaction that commits both.
    * **Idempotency beats fencing** — a replayed `message.id` returns its
      original `{:ok, seq}` before the fence is consulted, as required.

  `read_after/3` exploits gap-free seqs: messages after `s` limited to `n` are
  exactly the entries in `[s+1, s+n]`, one bounded `XRANGE`, no `COUNT` needed.
  """

  @behaviour Chat.Persistence.Port

  alias Chat.Adapters.Locus
  alias Chat.Message

  @retries 32

  # ── Port callbacks ──────────────────────────────────────────────────────────

  @impl true
  def append(cid, %Message{} = message), do: do_append(cid, message, :any, @retries)

  @impl true
  def append(cid, %Message{} = message, expected_seq),
    do: do_append(cid, message, expected_seq, 1)

  @impl true
  def read_after(cid, after_seq, limit) do
    log = Locus.conv_key(cid, "log")
    from = "(#{after_seq}-0"
    to = "#{after_seq + limit}-0"

    case Locus.command(["XRANGE", log, from, to]) do
      {:ok, entries} when is_list(entries) -> {:ok, Enum.map(entries, &decode_entry/1)}
      {:ok, {:error, msg}} -> {:error, {:locus, msg}}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def latest_seq(cid) do
    case Locus.command(["GET", Locus.conv_key(cid, "seq")]) do
      {:ok, nil} -> {:ok, 0}
      {:ok, seq} when is_binary(seq) -> {:ok, String.to_integer(seq)}
      {:ok, {:error, msg}} -> {:error, {:locus, msg}}
      {:error, reason} -> {:error, reason}
    end
  end

  # ── The transactional core ──────────────────────────────────────────────────

  defp do_append(_cid, _message, _expected, 0), do: {:error, :too_much_contention}

  defp do_append(cid, message, expected, attempts) do
    keys = %{
      seq: Locus.conv_key(cid, "seq"),
      log: Locus.conv_key(cid, "log"),
      ids: Locus.conv_key(cid, "ids")
    }

    case Locus.exclusive(fn run -> attempt(run, keys, message, expected) end) do
      :retry -> do_append(cid, message, expected, attempts - 1)
      other -> other
    end
  end

  # One optimistic attempt, on an exclusively-held connection.
  defp attempt(run, keys, message, expected) do
    with {:ok, [_watch, known, cur]} <-
           run.([["WATCH", keys.seq], ["HGET", keys.ids, message.id], ["GET", keys.seq]]) do
      cur = if cur, do: String.to_integer(cur), else: 0

      cond do
        # Idempotency beats fencing: a replay returns its original seq.
        is_binary(known) -> unwatch(run, {:ok, String.to_integer(known)})
        expected != :any and cur != expected -> unwatch(run, {:error, {:fenced, cur}})
        true -> commit(run, keys, message, expected, cur + 1)
      end
    end
  end

  defp unwatch(run, result) do
    run.([["UNWATCH"]])
    result
  end

  defp commit(run, keys, message, expected, n) do
    txn = [
      ["MULTI"],
      ["SET", keys.seq, n],
      ["XADD", keys.log, "#{n}-0" | encode_fields(message)],
      ["HSET", keys.ids, message.id, n],
      ["EXEC"]
    ]

    case run.(txn) do
      {:ok, [_m, _q1, _q2, _q3, nil]} ->
        # The watched seq moved between our read and EXEC: someone else won.
        if expected == :any, do: :retry, else: {:error, {:fenced, refetch(run, keys.seq)}}

      {:ok, [_m, _q1, _q2, _q3, [_, {:error, msg}, _]]} ->
        {:error, {:log_desync, msg}}

      {:ok, [_m, _q1, _q2, _q3, [_, _, _]]} ->
        {:ok, n}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp refetch(run, seq_k) do
    case run.([["GET", seq_k]]) do
      {:ok, [nil]} -> 0
      {:ok, [seq]} when is_binary(seq) -> String.to_integer(seq)
      _ -> 0
    end
  end

  # ── Message (de)serialization ───────────────────────────────────────────────
  #
  # Fields are stored individually (not one opaque term blob) so the log stays
  # readable to non-BEAM tooling and survives struct evolution. `meta` is the
  # one engine-internal map; it rides as term_to_binary.

  defp encode_fields(%Message{} = m) do
    [
      "id",
      m.id,
      "from",
      m.sender_id,
      "payload",
      m.payload,
      "ts",
      to_string(m.server_ts || ""),
      "kind",
      Atom.to_string(m.kind),
      "meta",
      :erlang.term_to_binary(m.meta)
    ]
  end

  defp decode_entry([entry_id, fields]) do
    seq = entry_id |> String.split("-") |> hd() |> String.to_integer()
    f = fields |> Enum.chunk_every(2) |> Map.new(fn [k, v] -> {k, v} end)

    %Message{
      id: Map.fetch!(f, "id"),
      sender_id: Map.fetch!(f, "from"),
      payload: Map.fetch!(f, "payload"),
      seq: seq,
      server_ts: parse_ts(Map.get(f, "ts", "")),
      kind: parse_kind(Map.get(f, "kind", "chat")),
      meta: :erlang.binary_to_term(Map.get(f, "meta", :erlang.term_to_binary(%{})))
    }
  end

  defp parse_ts(""), do: nil
  defp parse_ts(ts), do: String.to_integer(ts)

  defp parse_kind(kind) do
    String.to_existing_atom(kind)
  rescue
    ArgumentError -> :chat
  end
end
