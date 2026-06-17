defmodule Chat.Adapters.InMemory.ReceiptStore do
  @moduledoc """
  In-memory reference adapter for `Chat.ReceiptStore.Port`. Stores a monotonic
  read watermark per `{conversation, user}`. Tests/demos only.
  """
  use Agent
  @behaviour Chat.ReceiptStore.Port

  def start_link(_opts \\ []), do: Agent.start_link(fn -> %{} end, name: __MODULE__)

  @doc "Wipe all watermarks (test helper)."
  def reset, do: Agent.update(__MODULE__, fn _ -> %{} end)

  @impl true
  def set_read(conversation_id, user_id, seq) do
    Agent.update(__MODULE__, fn state ->
      conv = Map.get(state, conversation_id, %{})
      Map.put(state, conversation_id, Map.update(conv, user_id, seq, &max(&1, seq)))
    end)
  end

  @impl true
  def read_watermarks(conversation_id) do
    {:ok, Agent.get(__MODULE__, &Map.get(&1, conversation_id, %{}))}
  end
end
