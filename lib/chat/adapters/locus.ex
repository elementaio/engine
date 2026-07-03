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
      `SETMAX` (atomic monotonic advance).
    * `Chat.Adapters.Locus.PresenceStore`     — last-seen via `SETMAX`.
    * `Chat.Adapters.Locus.ReceiptStore`      — read watermarks via `SETMAX` + a
      readers set.
    * `Chat.Adapters.Locus.OfflineQueue`      — push-wake jobs `RPUSH`ed onto a
      list the body's workers drain with `BLPOP` (a Locus work queue).

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
      prefix: Keyword.get(env, :prefix, "vox")
    }
  end

  defp client_name(i), do: :"#{__MODULE__}.Client#{i}"

  @doc false
  def conn do
    # Sticky per caller process — spreads load without a shared counter.
    client_name(:erlang.phash2(self(), config().pool_size))
  end

  # ── Convenience used by the adapter modules ────────────────────────────────

  @doc false
  def command(cmd), do: Client.command(conn(), cmd)

  @doc false
  def pipeline(cmds), do: Client.pipeline(conn(), cmds)

  @doc false
  def exclusive(fun), do: Client.exclusive(conn(), fun)

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
