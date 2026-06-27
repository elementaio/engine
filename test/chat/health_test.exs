defmodule Chat.HealthTest do
  use ExUnit.Case, async: false

  alias Chat.Adapters.TestTransport
  alias Chat.Session

  setup do
    # Always leave the node accepting connections, even if an assertion fails.
    on_exit(fn -> Chat.resume() end)
    :ok
  end

  defp transport(label), do: {TestTransport, {self(), label}}

  test "a fresh node is ready and not draining" do
    Chat.resume()
    assert Chat.ready?()
    refute Chat.Health.draining?()
  end

  test "drain flips readiness off and resume restores it" do
    Chat.drain()
    assert Chat.Health.draining?()
    refute Chat.ready?()

    Chat.resume()
    refute Chat.Health.draining?()
    assert Chat.ready?()
  end

  test "a draining node refuses NEW sessions" do
    Chat.drain()

    assert {:error, :draining} =
             Session.connect(%{user_id: "drain-u", device_id: "d1", transport: transport(:drain)})
  end

  test "existing sessions survive a drain; new ones resume cleanly" do
    {:ok, session} =
      Session.connect(%{user_id: "keep-u", device_id: "d1", transport: transport(:keep)})

    Chat.drain()
    # The already-open session is untouched by a drain.
    assert Process.alive?(session)
    assert :ok = Session.sync(session)

    Chat.resume()

    assert {:ok, _} =
             Session.connect(%{user_id: "keep-u", device_id: "d2", transport: transport(:keep2)})
  end
end
