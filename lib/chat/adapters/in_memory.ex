defmodule Chat.Adapters.InMemory do
  @moduledoc """
  Convenience to start/reset all in-memory adapters on a node.

  Starts them UNLINKED (so they survive an `:peer.call` that returns), which is
  what the multi-node test needs to wire adapters on a peer node. For ordinary
  single-node tests, prefer `start_supervised/1` per adapter.
  """
  alias Chat.Adapters.InMemory.{
    ConversationStore,
    CursorStore,
    Persistence,
    PresenceStore,
    ReceiptStore
  }

  @agents [ConversationStore, CursorStore, PresenceStore, ReceiptStore]
  @all [Persistence | @agents]

  @doc "Start every in-memory adapter on the current node (idempotent, unlinked)."
  def start_all do
    ensure(Persistence, fn -> GenServer.start(Persistence, [], name: Persistence) end)
    Enum.each(@agents, fn mod -> ensure(mod, fn -> Agent.start(fn -> %{} end, name: mod) end) end)
    :ok
  end

  @doc "Reset every in-memory adapter on the current node."
  def reset_all, do: Enum.each(@all, & &1.reset())

  defp ensure(mod, starter) do
    case Process.whereis(mod) do
      nil -> starter.()
      _pid -> :ok
    end
  end
end
