defmodule Chat.PresenceStore.Port do
  @moduledoc """
  PORT: durable presence hint (last-seen).

  Live presence is kept in-memory in the engine; this port persists only the
  coarse "last seen at" for offline display. Defined now (M0 contract),
  implemented for M4. Declared lossy-tolerant: presence is a UX signal, not a
  correctness invariant.
  """
  alias Chat.Types

  @doc "Record that a user was seen at `ts` (epoch milliseconds)."
  @callback touch(Types.user_id(), ts :: integer()) :: :ok | {:error, term()}

  @doc "Get the last-seen timestamp (epoch ms) for a user, or `nil`."
  @callback last_seen(Types.user_id()) :: {:ok, integer() | nil} | {:error, term()}
end
