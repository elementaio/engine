defmodule Chat.Adapters.Locus.CursorStore do
  @moduledoc """
  `Chat.CursorStore.Port` on Locus: one integer key per (device, conversation),
  advanced with the atomic monotonic-max primitive (`SETMAX`, or a portable CAS
  on stock Redis — see `Chat.Adapters.Locus.set_max/2`) so `advance/3` can never
  move a cursor backwards, with no read-modify-write race. The opaque
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
  def advance(device_ref, cid, seq), do: Locus.set_max(key(device_ref, cid), seq)

  defp key(device_ref, cid), do: Locus.conv_key(cid, "cur:" <> Locus.b64(device_ref))
end
