defmodule Chat.Auth.Port do
  @moduledoc """
  PORT (hook): authentication + authorization.

  The engine NEVER owns identity; it only enforces the body's verdicts. Defined
  now (M0 contract), wired in M1. `authorize/3` works on metadata only, so
  message payloads stay opaque and E2E-friendly.
  """
  alias Chat.Types

  @doc """
  Authenticate a connection's credentials (opaque to the engine), returning the
  principal (`user_id`) on success.
  """
  @callback authenticate(credentials :: term()) ::
              {:ok, Types.user_id()} | {:error, term()}

  @doc "Authorize an action (e.g. `:send`, `:join`) by a user on a conversation."
  @callback authorize(action :: atom(), Types.user_id(), Types.conversation_id()) ::
              :ok | {:error, :forbidden}
end
