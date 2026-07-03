defmodule Chat.Adapters.Locus.ReceiptStore do
  @moduledoc """
  `Chat.ReceiptStore.Port` on Locus: one integer watermark key per
  (conversation, user) advanced with `SETMAX` (atomic, monotonic), plus a
  readers set so `read_watermarks/1` is one `SMEMBERS` + one `MGET` — the
  aggregate group-read query the port exists for, with no O(n²) fan-out.
  """

  @behaviour Chat.ReceiptStore.Port

  alias Chat.Adapters.Locus

  @impl true
  def set_read(cid, uid, seq) do
    case Locus.pipeline([
           ["SETMAX", read_key(cid, uid), seq],
           ["SADD", Locus.conv_key(cid, "readers"), uid]
         ]) do
      {:ok, replies} ->
        case Enum.find(replies, &match?({:error, _}, &1)) do
          nil -> :ok
          {:error, msg} -> {:error, {:locus, msg}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @impl true
  def read_watermarks(cid) do
    case Locus.command(["SMEMBERS", Locus.conv_key(cid, "readers")]) do
      {:ok, []} ->
        {:ok, %{}}

      {:ok, readers} when is_list(readers) ->
        case Locus.command(["MGET" | Enum.map(readers, &read_key(cid, &1))]) do
          {:ok, seqs} when is_list(seqs) ->
            {:ok,
             readers
             |> Enum.zip(seqs)
             |> Enum.reject(fn {_u, s} -> is_nil(s) end)
             |> Map.new(fn {u, s} -> {u, String.to_integer(s)} end)}

          {:ok, {:error, msg}} ->
            {:error, {:locus, msg}}

          {:error, reason} ->
            {:error, reason}
        end

      {:ok, {:error, msg}} ->
        {:error, {:locus, msg}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp read_key(cid, uid), do: Locus.conv_key(cid, "read:" <> uid)
end
