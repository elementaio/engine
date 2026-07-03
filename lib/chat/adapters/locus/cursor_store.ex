defmodule Chat.Adapters.Locus.CursorStore do
  @moduledoc """
  `Chat.CursorStore.Port` on Locus: one integer key per (device, conversation),
  advanced with `SETMAX` — Locus's atomic monotonic-max verb — so `advance/3`
  can never move a cursor backwards, with no read-modify-write race. The opaque
  `device_ref` term is url-base64-encoded into the key.
  """

  @behaviour Chat.CursorStore.Port

  alias Chat.Adapters.Locus

  @impl true
  def get(device_ref, cid) do
    case Locus.command(["GET", key(device_ref, cid)]) do
      {:ok, nil} -> {:ok, 0}
      {:ok, seq} when is_binary(seq) -> {:ok, String.to_integer(seq)}
      {:ok, {:error, msg}} -> {:error, {:locus, msg}}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def advance(device_ref, cid, seq) do
    case Locus.command(["SETMAX", key(device_ref, cid), seq]) do
      {:ok, n} when is_integer(n) -> :ok
      {:ok, {:error, msg}} -> {:error, {:locus, msg}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp key(device_ref, cid), do: Locus.conv_key(cid, "cur:" <> Locus.b64(device_ref))
end
