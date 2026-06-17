defmodule Chat.ReceiptStore.Port do
  @moduledoc """
  PORT: per-(conversation, user) read watermark — the highest `seq` a user has
  read in a conversation.

  This is what lets group read state be **aggregated** ("seen by N up to seq X")
  and pulled on demand, instead of fanning a receipt from every reader to every
  member — which would be O(n²) in big groups (plan Part 10/11). 1:1 still gets
  live receipts; groups record here and answer aggregate queries.
  """
  alias Chat.Types

  @doc "Record that `user` has read up to `seq` in a conversation (monotonic)."
  @callback set_read(Types.conversation_id(), Types.user_id(), Types.seq()) ::
              :ok | {:error, term()}

  @doc "All read watermarks for a conversation: `%{user_id => seq}`."
  @callback read_watermarks(Types.conversation_id()) ::
              {:ok, %{Types.user_id() => Types.seq()}} | {:error, term()}
end
