defmodule Chat.Adapters.TestTransport do
  @moduledoc """
  Test transport: instead of a socket, it forwards each outbound envelope to a
  collector process as `{:frame, label, envelope}`. Tests use a `{pid, label}`
  ref (e.g. `{self(), :alice}`) so `assert_receive` can tell clients apart.
  """
  @behaviour Chat.Transport

  @impl true
  def push({pid, label}, envelope) do
    send(pid, {:frame, label, envelope})
    :ok
  end

  @impl true
  def close({pid, label}, reason) do
    send(pid, {:closed, label, reason})
    :ok
  end
end
