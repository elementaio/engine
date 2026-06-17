defmodule Chat.Adapters.InMemory.PresenceStore do
  @moduledoc """
  In-memory reference adapter for `Chat.PresenceStore.Port` (durable last-seen).
  `touch/2` keeps the latest timestamp. Tests/demos only.
  """
  use Agent
  @behaviour Chat.PresenceStore.Port

  def start_link(_opts \\ []), do: Agent.start_link(fn -> %{} end, name: __MODULE__)

  @doc "Wipe all last-seen state (test helper)."
  def reset, do: Agent.update(__MODULE__, fn _ -> %{} end)

  @impl true
  def touch(user_id, ts) do
    Agent.update(__MODULE__, fn state -> Map.update(state, user_id, ts, &max(&1, ts)) end)
  end

  @impl true
  def last_seen(user_id) do
    {:ok, Agent.get(__MODULE__, &Map.get(&1, user_id))}
  end
end
