defmodule Chat.FirewallTest do
  @moduledoc """
  The architectural firewall: the pure core must not depend on transport, web,
  or database libraries. If this fails, someone added a forbidden dep to
  `mix.exs` — move it to a body app instead.
  """
  use ExUnit.Case, async: true

  @forbidden [
    :bandit,
    :cowboy,
    :plug,
    :phoenix,
    :phoenix_pubsub,
    :grpc,
    :ecto,
    :ecto_sql,
    :postgrex,
    :redix
  ]

  test "core declares no transport/web/database dependencies" do
    Application.load(:chat_engine)
    deps = Application.spec(:chat_engine, :applications) || []
    leaked = Enum.filter(@forbidden, &(&1 in deps))
    assert leaked == [], "core leaked forbidden deps: #{inspect(leaked)}"
  end
end
