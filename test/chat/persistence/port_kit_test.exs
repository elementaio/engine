defmodule Chat.Persistence.InMemoryContractTest do
  @moduledoc """
  Runs the shared `Chat.Persistence.Port` contract test-kit against the bundled
  in-memory reference adapter. This both guards the reference adapter and is the
  executable proof that the kit itself is correct — an implementer who points the
  same `use` at their adapter gets the identical contract assertions.

  The adapter is already started for the whole suite by `test/test_helper.exs`,
  so no `:setup` is needed here.
  """
  use Chat.Persistence.PortTest, adapter: Chat.Adapters.InMemory.Persistence
end
