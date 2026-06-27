defmodule Chat.ConfigTest do
  use ExUnit.Case, async: false

  # Helper: swap one :chat_engine key for the duration of a test, then restore.
  defp with_env(key, value, fun) do
    original = Application.fetch_env(:chat_engine, key)

    Application.put_env(:chat_engine, key, value)

    try do
      fun.()
    after
      case original do
        {:ok, v} -> Application.put_env(:chat_engine, key, v)
        :error -> Application.delete_env(:chat_engine, key)
      end
    end
  end

  test "the default (in-memory) wiring is valid" do
    assert :ok = Chat.Config.validate!()
    assert Chat.Config.valid?()
  end

  test "a missing required adapter is rejected" do
    with_env(:persistence_adapter, nil, fn ->
      refute Chat.Config.valid?()

      assert_raise ArgumentError, ~r/persistence_adapter is not configured/, fn ->
        Chat.Config.validate!()
      end
    end)
  end

  test "an adapter that does not implement the behaviour is rejected" do
    # `Enum` is loadable but exports none of the Persistence.Port callbacks.
    with_env(:persistence_adapter, Enum, fn ->
      refute Chat.Config.valid?()

      assert_raise ArgumentError, ~r/does not implement Chat.Persistence.Port/, fn ->
        Chat.Config.validate!()
      end
    end)
  end

  test "an unloadable adapter module is rejected" do
    with_env(:conversation_store_adapter, NoSuchAdapterModule, fn ->
      assert_raise ArgumentError, ~r/cannot be loaded/, fn -> Chat.Config.validate!() end
    end)
  end

  test "a non-positive numeric knob is rejected" do
    with_env(:max_payload_bytes, 0, fn ->
      refute Chat.Config.valid?()

      assert_raise ArgumentError, ~r/max_payload_bytes must be a positive integer/, fn ->
        Chat.Config.validate!()
      end
    end)
  end

  test "the optional offline_queue port is fine when unset" do
    assert Application.get_env(:chat_engine, :offline_queue_adapter) == nil
    assert Chat.Config.valid?()
  end
end
