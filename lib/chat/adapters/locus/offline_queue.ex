defmodule Chat.Adapters.Locus.OfflineQueue do
  @moduledoc """
  `Chat.OfflineQueue.Port` on Locus: each offline-recipient wake becomes one
  JSON job `RPUSH`ed onto the `vox:wake` list — a durable work queue the body's
  push workers drain with `BLPOP` (blocking, FIFO-fair across workers, in any
  language that speaks RESP).

  Deliberately a *wake*, not a message copy: the job carries ids and the seq,
  never the payload — the device catches up from the durable log by cursor, and
  push content policy stays in the body. `notify/3` is one `RPUSH` (fast,
  non-wedging); the slow APNs/FCM work happens in the workers.
  """

  @behaviour Chat.OfflineQueue.Port

  alias Chat.Adapters.Locus
  alias Chat.Message

  @impl true
  def notify(uid, cid, %Message{} = message) do
    job =
      json(%{
        "user_id" => uid,
        "conversation_id" => cid,
        "message_id" => message.id,
        "seq" => message.seq,
        "server_ts" => message.server_ts
      })

    case Locus.command(["RPUSH", Locus.wake_key(), job]) do
      {:ok, n} when is_integer(n) -> :ok
      {:ok, {:error, msg}} -> {:error, {:locus, msg}}
      {:error, reason} -> {:error, reason}
    end
  end

  # A tiny flat-map JSON encoder — the firewall forbids JSON deps, and a wake
  # job is only strings/ints/nulls.
  defp json(map) do
    inner =
      map
      |> Enum.sort()
      |> Enum.map_join(",", fn {k, v} -> ~s("#{k}":#{value(v)}) end)

    "{" <> inner <> "}"
  end

  defp value(nil), do: "null"
  defp value(i) when is_integer(i), do: Integer.to_string(i)
  defp value(s) when is_binary(s), do: inspect(s, binaries: :as_strings)
end
