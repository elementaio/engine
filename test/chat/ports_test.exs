defmodule Chat.PortsTest do
  use ExUnit.Case, async: false

  # These tests mutate global application env, so they are not async and they
  # restore whatever was there before.
  setup do
    prev = Application.get_env(:chat_engine, :persistence_adapter)

    on_exit(fn ->
      case prev do
        nil -> Application.delete_env(:chat_engine, :persistence_adapter)
        val -> Application.put_env(:chat_engine, :persistence_adapter, val)
      end
    end)

    :ok
  end

  test "persistence/0 returns the configured adapter module" do
    Application.put_env(:chat_engine, :persistence_adapter, SomeAdapter)
    assert Chat.Ports.persistence() == SomeAdapter
  end

  test "persistence/0 raises a helpful error when unconfigured" do
    Application.delete_env(:chat_engine, :persistence_adapter)

    assert_raise RuntimeError, ~r/No :persistence_adapter configured/, fn ->
      Chat.Ports.persistence()
    end
  end
end
