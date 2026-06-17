defmodule Chat.Adapters.InMemory.CursorStore do
  @moduledoc """
  In-memory reference adapter for `Chat.CursorStore.Port`. Keyed by
  `{device_ref, conversation_id}`; `advance/3` is monotonic. Tests/demos only.
  """
  use Agent
  @behaviour Chat.CursorStore.Port

  def start_link(_opts \\ []) do
    Agent.start_link(fn -> %{} end, name: __MODULE__)
  end

  @doc "Wipe all cursors (test helper)."
  def reset, do: Agent.update(__MODULE__, fn _ -> %{} end)

  @impl true
  def get(device_ref, conversation_id) do
    {:ok, Agent.get(__MODULE__, &Map.get(&1, {device_ref, conversation_id}, 0))}
  end

  @impl true
  def advance(device_ref, conversation_id, seq) do
    Agent.update(__MODULE__, fn state ->
      Map.update(state, {device_ref, conversation_id}, seq, &max(&1, seq))
    end)
  end
end
