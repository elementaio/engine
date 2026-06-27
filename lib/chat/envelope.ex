defmodule Chat.Envelope do
  @moduledoc """
  The normalized message that flows in and out of the engine, both directions.

  The edge codec translates between this struct and the wire format (JSON in M1;
  a binary prefix + Protobuf later — see plan Part 3). The core only ever deals
  in this struct, never in bytes.

  `type` is one of:

    * inbound  (client → engine): `:send`, `:read`, `:sync`, `:typing`,
      `:presence_query`, `:read_state`
    * outbound (engine → client): `:message`, `:ack`, `:receipt`, `:sync_page`,
      `:error`, `:system`, `:presence`, `:typing`, `:read_state`

  `:payload` is an OPAQUE binary — the engine never inspects it.
  """
  @type type ::
          :send
          | :read
          | :sync
          | :typing
          | :presence_query
          | :read_state
          | :message
          | :ack
          | :receipt
          | :sync_page
          | :error
          | :system
          | :presence

  defstruct [
    :type,
    :conversation_id,
    # client-generated message id (dedup key)
    :id,
    :sender_id,
    # engine-assigned ordering authority
    :seq,
    # opaque payload
    :payload,
    # for :ack / :receipt / :system → :server_received | :delivered | :read | :join | :leave
    :status,
    # set on :message — whether recipients should emit delivery/read receipts
    # (true only for 1:1; suppressed in groups to avoid receipt storms)
    :receipts,
    # for :sync_page → list of plain maps
    :messages,
    # for :error
    :reason,
    :ts,
    # subject user for :presence / :presence_query
    :user_id,
    # for :read_state → "seen by" count + (capped) reader list
    :count,
    :readers,
    # for :sync_page → is there at least one more message beyond this page
    # (paginate by re-issuing :sync with this page's `seq` until `more` is false)
    :more,
    # for inbound :send → `:ephemeral` requests a live-only (no-persist) broadcast;
    # nil/`:chat` (default) is the normal durable path
    :kind
  ]

  @type t :: %__MODULE__{type: type()}
end
