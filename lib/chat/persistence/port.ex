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
  """
  alias Chat.{Message, Types}

  @doc "Durably append a message and assign its per-conversation seq."
  @callback append(Types.conversation_id(), Message.t()) ::
              {:ok, Types.seq()} | {:error, term()}

  @doc "Read up to `limit` messages with seq strictly greater than `after_seq`, ascending."
  @callback read_after(Types.conversation_id(), after_seq :: Types.seq(), limit :: pos_integer()) ::
              {:ok, [Message.t()]} | {:error, term()}

  @doc "Highest assigned seq for a conversation; 0 if empty."
  @callback latest_seq(Types.conversation_id()) :: {:ok, Types.seq()} | {:error, term()}
end
