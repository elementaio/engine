defmodule Chat.Adapters.Locus.PresenceStore do
  @moduledoc """
  `Chat.PresenceStore.Port` on Locus: `vox:seen:<user>` holds the last-seen
  epoch-ms, written with `SETMAX` so out-of-order touches (two nodes, clock
  skew, retries) can never move last-seen backwards.
  """

  @behaviour Chat.PresenceStore.Port

  alias Chat.Adapters.Locus

  @impl true
  def touch(uid, ts) do
    case Locus.command(["SETMAX", Locus.seen_key(uid), ts]) do
      {:ok, n} when is_integer(n) -> :ok
      {:ok, {:error, msg}} -> {:error, {:locus, msg}}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def last_seen(uid) do
    case Locus.command(["GET", Locus.seen_key(uid)]) do
      {:ok, nil} -> {:ok, nil}
      {:ok, ts} when is_binary(ts) -> {:ok, String.to_integer(ts)}
      {:ok, {:error, msg}} -> {:error, {:locus, msg}}
      {:error, reason} -> {:error, reason}
    end
  end
end
