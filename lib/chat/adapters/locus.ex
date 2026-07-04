defmodule Chat.Adapters.Locus do
  @moduledoc """
  Locus adapter set — the engine's memory, on [Locus](https://github.com/elementaio/locus).

  Implements six of the seven ports over one small dependency-free RESP client
  (`Chat.Adapters.Locus.Client`; `auth` stays app-side, where it belongs):

    * `Chat.Adapters.Locus.Persistence`       — per-conversation message log on a
      Locus **stream** whose entry ids *are* the seqs (`<seq>-0`), with the
      seq/idempotency bookkeeping committed atomically (`WATCH`/`MULTI`/`EXEC`) —
      including the optional split-brain **fence** (`append/3`).
    * `Chat.Adapters.Locus.ConversationStore` — membership as sets (+ a per-user
      reverse index), `SSCAN`-paged rosters, O(1) `SCARD` counts.
    * `Chat.Adapters.Locus.CursorStore`       — per-device delivery cursors via
      the monotonic-max primitive (`SETMAX`, or portable CAS — see below).
    * `Chat.Adapters.Locus.PresenceStore`     — last-seen via monotonic-max.
    * `Chat.Adapters.Locus.ReceiptStore`      — read watermarks via monotonic-max
      + a readers set.
    * `Chat.Adapters.Locus.OfflineQueue`      — push-wake jobs `RPUSH`ed onto a
      list the body's workers drain with `BLPOP` (a Locus work queue).

  ## Runs on stock Redis too

  Every command this adapter set issues is standard RESP **except** `SETMAX`
  (Locus's atomic monotonic-max verb). Configure `monotonic: :cas` and the
  cursor/presence/receipt stores use a portable `WATCH`/`GET`/`MULTI`/`SET` loop
  instead, so the *same* adapter runs unchanged against Redis / Valkey / KeyDB.
  The default (`:setmax`) keeps the one-round-trip fast path on Locus.

  ## Wiring (the body does this)

      # config.exs
      config :chat_engine,
        persistence_adapter: Chat.Adapters.Locus.Persistence,
        conversation_store_adapter: Chat.Adapters.Locus.ConversationStore,
        cursor_store_adapter: Chat.Adapters.Locus.CursorStore,
        presence_store_adapter: Chat.Adapters.Locus.PresenceStore,
        receipt_store_adapter: Chat.Adapters.Locus.ReceiptStore,
        offline_queue_adapter: Chat.Adapters.Locus.OfflineQueue

      config :chat_engine, :locus,
        host: "127.0.0.1", port: 6379, password: nil, pool_size: 4, prefix: "vox"

      # in the body's supervision tree, before the engine is used:
      children = [Chat.Adapters.Locus, ...]

  ## Durability note

  `Chat.Persistence.Port` requires appends to be durable before they return.
  With Locus that is a deployment dial: run with `LOCUS_AOF` enabled —
  `LOCUS_APPENDFSYNC=always` for strict durable-before-ack, or the `everysec`
  default to bound loss to ≤1 s of acknowledged messages after a hard crash
  (the usual Redis-style trade-off; pick per product).

  ## Keys

  Everything conversation-scoped hashtags the conversation id — `vox:{<cid>}:…`
  — so a future clustered Locus keeps a conversation's log, members, cursors,
  and receipts on one shard (and `MULTI`/`EXEC` stays single-slot).
  """

  use Supervisor

  alias Chat.Adapters.Locus.Client

  # ── Supervision ─────────────────────────────────────────────────────────────

  def start_link(opts \\ []), do: Supervisor.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(_opts) do
    cfg = config()

    children =
      for i <- 0..(cfg.pool_size - 1) do
        Supervisor.child_spec(
          {Client, name: client_name(i), host: cfg.host, port: cfg.port, password: cfg.password},
          id: {Client, i}
        )
      end

    Supervisor.init(children, strategy: :one_for_one)
  end

  # ── Config / pool ───────────────────────────────────────────────────────────

  @doc false
  def config do
    env = Application.get_env(:chat_engine, :locus, [])

    %{
      host: Keyword.get(env, :host, "127.0.0.1"),
      port: Keyword.get(env, :port, 6379),
      password: Keyword.get(env, :password),
      pool_size: Keyword.get(env, :pool_size, 4),
      prefix: Keyword.get(env, :prefix, "vox"),
      # `:setmax` uses Locus's atomic monotonic-max verb (one round trip).
      # `:cas` emulates it with a portable `WATCH`/`GET`/`MULTI`/`SET` loop, so
      # the same adapter runs unchanged on stock Redis / Valkey / KeyDB, which
      # have no SETMAX. Everything else the adapter uses is standard RESP.
      monotonic: Keyword.get(env, :monotonic, :setmax)
    }
  end

  defp client_name(i), do: :"#{__MODULE__}.Client#{i}"

  @doc false
  def conn do
    # Sticky per caller process — spreads load without a shared counter.
    client_name(:erlang.phash2(self(), config().pool_size))
  end

  # ── Convenience used by the adapter modules ────────────────────────────────

  # A blocking command on a SHARED pool member wedges every caller hashed to it
  # (found live: a parked BLPOP made the wake-RPUSH queue behind it until the
  # pop timed out). Blocking consumers start their own Client instead.
  @blocking ~w(BLPOP BRPOP BLMOVE BZPOPMIN BZPOPMAX)

  @doc false
  def command(cmd), do: Client.command(conn(), guard_blocking!(cmd))

  @doc false
  def pipeline(cmds), do: Client.pipeline(conn(), Enum.map(cmds, &guard_blocking!/1))

  defp guard_blocking!([name | _] = cmd) do
    if String.upcase(IO.iodata_to_binary([name])) in @blocking do
      raise ArgumentError,
            "blocking commands must not run on the shared Locus pool (they wedge " <>
              "every caller sharing the connection) — start a dedicated " <>
              "Chat.Adapters.Locus.Client for BLPOP-style consumers"
    end

    cmd
  end

  @doc false
  def exclusive(fun), do: Client.exclusive(conn(), fun)

  # ── Monotonic max (SETMAX, or a portable CAS for stock Redis) ────────────────

  @doc """
  Set `key` to `value` only if `value` is greater than the current value
  (atomic monotonic max) — the primitive behind cursors, presence, and read
  watermarks. Uses `SETMAX` on Locus, or a `WATCH`/`MULTI` CAS loop when the
  adapter is configured `monotonic: :cas` (so it runs on stock Redis).

  Returns `:ok` or `{:error, term}`.
  """
  def set_max(key, value) do
    case config().monotonic do
      :cas -> set_max_cas(key, value, 32)
      _ -> set_max_setmax(key, value)
    end
  end

  defp set_max_setmax(key, value) do
    case command(["SETMAX", key, value]) do
      {:ok, n} when is_integer(n) -> :ok
      {:ok, {:error, msg}} -> {:error, {:locus, msg}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp set_max_cas(_key, _value, 0), do: {:error, :too_much_contention}

  defp set_max_cas(key, value, attempts) do
    case exclusive(fn run -> cas_max_once(run, key, value) end) do
      :retry -> set_max_cas(key, value, attempts - 1)
      other -> other
    end
  end

  # One WATCH/GET → conditional SET pass. Returns :ok, :retry (watched key changed
  # under us), or {:error, _}.
  defp cas_max_once(run, key, value) do
    with {:ok, [_watch, cur]} <- run.([["WATCH", key], ["GET", key]]) do
      current = if cur, do: String.to_integer(cur), else: 0
      if value <= current, do: unwatch_ok(run), else: commit_max(run, key, value)
    end
  end

  defp unwatch_ok(run) do
    run.([["UNWATCH"]])
    :ok
  end

  defp commit_max(run, key, value) do
    case run.([["MULTI"], ["SET", key, value], ["EXEC"]]) do
      # EXEC == nil ⇒ the watched key changed under us; retry.
      {:ok, [_multi, _queued, nil]} -> :retry
      {:ok, [_multi, _queued, _exec]} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  # ── Keys ────────────────────────────────────────────────────────────────────

  @doc false
  def k(parts) when is_list(parts), do: IO.iodata_to_binary([config().prefix | parts])

  @doc false
  def conv_key(cid, suffix), do: k([":{", cid, "}:", suffix])

  @doc false
  def user_convs_key(uid), do: k([":u:", uid, ":convs"])

  @doc false
  def seen_key(uid), do: k([":seen:", uid])

  @doc false
  def wake_key, do: k([":wake"])

  @doc false
  def b64(term), do: Base.url_encode64(:erlang.term_to_binary(term), padding: false)
end
