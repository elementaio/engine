defmodule Chat.Types do
  @moduledoc "Shared type aliases used across the engine's contracts."

  @typedoc "Stable identifier of a conversation (1:1 or group). Opaque to the engine."
  @type conversation_id :: String.t()

  @typedoc "Stable identifier of a user/principal. Opaque to the engine."
  @type user_id :: String.t()

  @typedoc "Stable identifier of a device/session belonging to a user."
  @type device_id :: String.t()

  @typedoc """
  Client-generated message identifier (e.g. a ULID / UUIDv7). This is the
  IDEMPOTENCY / DEDUP key — re-sending the same id must NOT create a new message.
  """
  @type message_id :: String.t()

  @typedoc """
  Engine-assigned, per-conversation, monotonically increasing sequence number.
  The SOLE authority for ordering within a conversation. `0` means "none yet".
  """
  @type seq :: non_neg_integer()
end
