defmodule Chat.Adapters.InMemory.ConversationStore do
  @moduledoc """
  In-memory reference adapter for `Chat.ConversationStore.Port` (membership).
  Backed by an Agent (a process that just holds state). Tests/demos only.
  """
  use Agent
  @behaviour Chat.ConversationStore.Port

  def start_link(_opts \\ []) do
    Agent.start_link(fn -> %{} end, name: __MODULE__)
  end

  @doc "Wipe all membership state (test helper)."
  def reset, do: Agent.update(__MODULE__, fn _ -> %{} end)

  @impl true
  def member?(conversation_id, user_id) do
    Agent.get(__MODULE__, fn state ->
      state |> Map.get(conversation_id, MapSet.new()) |> MapSet.member?(user_id)
    end)
  end

  @impl true
  def add_member(conversation_id, user_id) do
    Agent.update(__MODULE__, fn state ->
      Map.update(state, conversation_id, MapSet.new([user_id]), &MapSet.put(&1, user_id))
    end)
  end

  @impl true
  def remove_member(conversation_id, user_id) do
    Agent.update(__MODULE__, fn state ->
      Map.update(state, conversation_id, MapSet.new(), &MapSet.delete(&1, user_id))
    end)
  end

  @impl true
  def stream_members(conversation_id, _cursor, _limit) do
    members =
      Agent.get(__MODULE__, fn state ->
        state |> Map.get(conversation_id, MapSet.new()) |> MapSet.to_list()
      end)

    # M1: one page is enough. A real adapter pages with a cursor.
    {:ok, members, nil}
  end

  @impl true
  def conversations_for(user_id) do
    convs =
      Agent.get(__MODULE__, fn state ->
        for {conv, members} <- state, MapSet.member?(members, user_id), do: conv
      end)

    {:ok, convs}
  end

  @impl true
  def member_count(conversation_id) do
    n =
      Agent.get(__MODULE__, fn state ->
        state |> Map.get(conversation_id, MapSet.new()) |> MapSet.size()
      end)

    {:ok, n}
  end
end
