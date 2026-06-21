defmodule Chat.Persistence.Port do
  @moduledoc """
  PORT: the authoritative, ordered, idempotent message log.

  This is the most important contract in the engine. The body implements it
  over a real database; the engine ships only an in-memory adapter for tests.

  Guarantees the adapter MUST provide:

    * `append/2` is IDEMPOTENT on `message.id`. A retry with the same id returns
      the SAME `{:ok, seq}` it returned the first time — never a new seq.
    * `append/2` assigns a per-conversation, MONOTONIC, gap-free `seq`.
    * `read_after/3` returns messages in strictly increasing `seq` order.
    * `append/2` is durable before it returns `:ok` (the engine persists BEFORE
      it acknowledges the sender — this is the at-least-once hinge).

  Payloads are OPAQUE binaries; the adapter MUST NOT inspect them.

  ## Split-brain fencing (`append/3`, optional)

  HRW owner placement (`Chat.Cluster`) is a *liveness/locality* optimization, not
  a safety mechanism: during a network partition each side's `Node.list/0`
  differs, so two nodes can elect two owners for the same conversation and each
  assign `seq` into a divergent log. The ONLY place that can prevent this is the
  store, because it is the one component both owners share.

  `append/3` is an OPTIONAL compare-and-set: the owner passes the `seq` it
  believes is current (`expected_seq`), and the adapter commits only if that is
  still the latest — otherwise it returns `{:error, {:fenced, current_seq}}` and
  the losing owner steps down (see `Chat.Conversation`). The engine feature-detects
  it via `function_exported?/3` and falls back to `append/2` (NO split-brain
  protection) when an adapter does not implement it.

  A conforming `append/3` MUST be a SINGLE linearizable conditional write against
  the shared store (e.g. Postgres `INSERT ... WHERE NOT EXISTS (SELECT 1 ... seq =
  $expected + 1)`, or a Redis Lua CAS). A read-then-write of `latest_seq` then
  `append/2` is NON-conforming — it reintroduces the race. Idempotency on
  `message.id` MUST be checked BEFORE the fence, so a retried message returns its
  original `{:ok, seq}` and is never reported as a fence loss.
  """
  alias Chat.{Message, Types}

  @doc "Durably append a message and assign its per-conversation seq."
  @callback append(Types.conversation_id(), Message.t()) ::
              {:ok, Types.seq()} | {:error, term()}

  @doc """
  Durably append with an optimistic fence on `expected_seq` (the seq the caller
  believes is current). Commit and return `{:ok, expected_seq + 1}` only if the
  log is still at `expected_seq`; if a known `message.id` is replayed, return its
  original `{:ok, seq}` (idempotency beats fencing); otherwise return
  `{:error, {:fenced, current_seq}}`. OPTIONAL — see the moduledoc.
  """
  @callback append(Types.conversation_id(), Message.t(), expected_seq :: Types.seq()) ::
              {:ok, Types.seq()} | {:error, {:fenced, Types.seq()}} | {:error, term()}

  @doc "Read up to `limit` messages with seq strictly greater than `after_seq`, ascending."
  @callback read_after(Types.conversation_id(), after_seq :: Types.seq(), limit :: pos_integer()) ::
              {:ok, [Message.t()]} | {:error, term()}

  @doc "Highest assigned seq for a conversation; 0 if empty."
  @callback latest_seq(Types.conversation_id()) :: {:ok, Types.seq()} | {:error, term()}

  @optional_callbacks append: 3
end
