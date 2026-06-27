defmodule Chat.Message do
  @moduledoc """
  A chat message as it flows through the engine.

  Two identifiers, on purpose:

    * `:id`  — client-generated, the dedup / idempotency key.
    * `:seq` — engine-assigned by the persistence port, the ordering authority.
               `nil` until the message has been durably appended.

  `:payload` is an OPAQUE binary. The engine never inspects it — this is what
  lets end-to-end encryption be layered on later with zero engine changes.
  """
  alias Chat.Types

  @enforce_keys [:id, :sender_id, :payload]
  defstruct [
    :id,
    :sender_id,
    :payload,
    :seq,
    :server_ts,
    kind: :chat,
    meta: %{}
  ]

  @type t :: %__MODULE__{
          id: Types.message_id(),
          sender_id: Types.user_id(),
          payload: binary(),
          seq: Types.seq() | nil,
          server_ts: integer() | nil,
          kind: atom(),
          meta: map()
        }
end
