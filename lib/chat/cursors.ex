defmodule Chat.Cursors do
  @moduledoc """
  Core-side helper over the `Chat.CursorStore.Port`.

  A "cursor" is how far a specific device has been delivered in a conversation.
  Sessions advance it as they deliver (live or during catch-up); on reconnect
  they read it to know where to resume. This is the engine's automatic, gapless,
  exactly-once* offline store-and-forward.

  *exactly-once in the common case; the reconnect window is at-least-once and
  relies on client dedup by message id (plan Part 10).
  """
  alias Chat.Types

  @type ref :: {Types.user_id(), Types.device_id()}

  @spec get(ref(), Types.conversation_id()) :: Types.seq()
  def get(ref, conversation_id) do
    case Chat.Ports.cursor_store().get(ref, conversation_id) do
      {:ok, seq} -> seq
      _ -> 0
    end
  end

  @spec advance(ref(), Types.conversation_id(), Types.seq()) :: :ok
  def advance(ref, conversation_id, seq) do
    Chat.Ports.cursor_store().advance(ref, conversation_id, seq)
    :ok
  end
end
