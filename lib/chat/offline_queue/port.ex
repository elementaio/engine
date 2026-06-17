defmodule Chat.OfflineQueue.Port do
  @moduledoc """
  PORT: per-recipient store-and-forward queue (offline delivery + catch-up).

  Defined now as part of the M0 "firewall" (the contract); implemented in M3.
  FIFO per {recipient_device, conversation}; at-least-once via drain-then-ack
  (items are removed only after the recipient acks).
  """
  alias Chat.{Message, Types}

  @doc "Enqueue a message for a recipient device that was offline / slow."
  @callback enqueue(Types.device_id(), Types.conversation_id(), Message.t()) ::
              :ok | {:error, term()}

  @doc "Drain up to `limit` queued items for a device (WITHOUT removing them)."
  @callback drain(Types.device_id(), limit :: pos_integer()) ::
              {:ok, [Message.t()]} | {:error, term()}

  @doc "Acknowledge delivery up to `up_to_seq`, removing acked items."
  @callback ack(Types.device_id(), Types.conversation_id(), up_to_seq :: Types.seq()) ::
              :ok | {:error, term()}
end
