defmodule Chat.OfflineQueue.Port do
  @moduledoc """
  PORT: the offline push-notification hook (OPTIONAL).

  When a durable message is appended and fanned out, every conversation member
  who has **no online session** at that moment misses the live delivery. This
  port is how the engine tells the body "wake user U about conversation C" so the
  body can fire a push (APNs / FCM / web-push / e-mail digest / SMS / a webhook).

  ## Why this is only a *wake*, not a store-and-forward queue

  The engine does NOT rely on this port for message *recovery*. Every message is
  in the durable log (`Chat.Persistence.Port`), and each device has a monotonic
  cursor, so a reconnecting device catches up **exactly once by `seq`** regardless
  of whether a push was delivered, duplicated, or dropped. That makes this hook
  best-effort by design: a lost notification costs a late wake, never a lost
  message. The body owns push tokens, per-user device fan-out, de-duplication,
  rate-limiting, and digest/batching policy — none of which belong in the core.

  ## Contract

  `notify/3` is called (off the hub, asynchronously) once per offline recipient,
  with the message carrying its assigned `seq`. It MUST NOT block on a slow
  external service in a way that wedges the caller — do the actual push work in
  the adapter's own process/queue and return promptly. Return `:ok` (or
  `{:error, term}`, which the engine logs via telemetry and otherwise ignores).

  Leave `:offline_queue_adapter` unset to disable offline pushes entirely.
  """
  alias Chat.{Message, Types}

  @doc """
  Wake `user_id` about a new message in `conversation_id`. Best-effort: the engine
  has already durably stored the message, so the recipient will catch up by cursor
  on reconnect even if this push is lost.
  """
  @callback notify(Types.user_id(), Types.conversation_id(), Message.t()) ::
              :ok | {:error, term()}
end
