defmodule Chat.Receipts do
  @moduledoc """
  Read-receipt bookkeeping over the `Chat.ReceiptStore.Port`.

  Every `:read` records a per-user watermark. For 1:1 the session also fans out a
  live receipt (see `Chat.Conversation`); for groups we DON'T fan out — clients
  pull the aggregate ("seen by N") via `aggregate/2`. This keeps group read state
  O(1) per read instead of O(members) (plan Part 10).
  """
  require Logger
  alias Chat.Types

  @readers_cap 50

  @spec record_read(Types.conversation_id(), Types.user_id(), Types.seq()) :: :ok
  def record_read(_conversation_id, _user_id, nil), do: :ok

  def record_read(conversation_id, user_id, seq) do
    case Chat.Ports.receipt_store().set_read(conversation_id, user_id, seq) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning(
          "receipt set_read failed (conv=#{inspect(conversation_id)} user=#{inspect(user_id)}): #{inspect(reason)}"
        )

        :ok
    end
  end

  @doc """
  Aggregate read state at `seq`: `{count, readers}` where `readers` is the list
  of users who have read at least up to `seq` (capped — big groups want only the
  count). Degrades to `{0, []}` if the store errors (never crashes the caller).
  """
  @spec aggregate(Types.conversation_id(), Types.seq()) :: {non_neg_integer(), [Types.user_id()]}
  def aggregate(conversation_id, seq) do
    case Chat.Ports.receipt_store().read_watermarks(conversation_id) do
      {:ok, watermarks} ->
        readers = for {user, wm} <- watermarks, wm >= seq, do: user
        {length(readers), Enum.take(readers, @readers_cap)}

      {:error, reason} ->
        Logger.warning(
          "receipt read_watermarks failed (conv=#{inspect(conversation_id)}): #{inspect(reason)}"
        )

        {0, []}
    end
  end
end
