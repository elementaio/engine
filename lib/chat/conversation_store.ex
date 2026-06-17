defmodule Chat.ConversationStore do
  @moduledoc """
  Core-side helper over the `Chat.ConversationStore.Port`.

  Pages through `stream_members/3` and returns the full member list. In M1 (1:1
  and small groups) this is fine; M2 keeps the *fan-out* paged so the engine
  never materializes a huge roster (plan Part 6).
  """
  alias Chat.Types

  @spec members(Types.conversation_id()) :: [Types.user_id()]
  def members(conversation_id), do: collect(conversation_id, nil, [])

  defp collect(conversation_id, cursor, acc) do
    case Chat.Ports.conversation_store().stream_members(conversation_id, cursor, 1000) do
      {:ok, ids, nil} -> acc ++ ids
      {:ok, ids, next} -> collect(conversation_id, next, acc ++ ids)
      {:error, _} -> acc
    end
  end
end
