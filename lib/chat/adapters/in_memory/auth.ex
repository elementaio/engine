defmodule Chat.Adapters.InMemory.Auth do
  @moduledoc """
  Trivial "allow-all" auth adapter for dev/test. Treats the supplied credential
  AS the user id. A real body verifies a token/JWT/session here and returns the
  authenticated principal. `authorize/3` always permits.
  """
  @behaviour Chat.Auth.Port

  @impl true
  def authenticate(%{user_id: user_id}) when is_binary(user_id), do: {:ok, user_id}
  def authenticate(user_id) when is_binary(user_id) and user_id != "", do: {:ok, user_id}
  def authenticate(_), do: {:error, :unauthenticated}

  @impl true
  def authorize(_action, _user_id, _conversation_id), do: :ok
end
