defmodule Chat.CursorStore.Port do
  @moduledoc """
  PORT: per-device delivery cursor — the highest `seq` a device has been
  delivered, per conversation.

  This is what makes offline store-and-forward automatic and exactly-once: on
  reconnect a device is caught up from its cursor by reading the durable log
  (no per-recipient queue, so it scales to huge groups — plan Part 10/11).

  `device_ref` is an opaque term identifying a device (the engine uses
  `{user_id, device_id}`). `advance/3` MUST be monotonic — it may only move a
  cursor forward (`max`).
  """
  alias Chat.Types

  @doc "Current cursor for a device in a conversation (0 if none)."
  @callback get(device_ref :: term(), Types.conversation_id()) ::
              {:ok, Types.seq()} | {:error, term()}

  @doc "Move the cursor forward to `seq` (monotonic max). Never moves backwards."
  @callback advance(device_ref :: term(), Types.conversation_id(), Types.seq()) ::
              :ok | {:error, term()}
end
