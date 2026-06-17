defmodule Chat.ConversationStore.Port do
  @moduledoc """
  PORT: source of truth for conversation membership.

  Defined now (M0 contract), implemented for M2 (groups). Unlimited membership
  is handled by STREAMING members in bounded pages — the engine never asks for
  the whole roster at once.
  """
  alias Chat.Types

  @doc "Whether a user is a member of a conversation."
  @callback member?(Types.conversation_id(), Types.user_id()) :: boolean()

  @doc "Add a member."
  @callback add_member(Types.conversation_id(), Types.user_id()) :: :ok | {:error, term()}

  @doc "Remove a member."
  @callback remove_member(Types.conversation_id(), Types.user_id()) :: :ok | {:error, term()}

  @doc """
  Stream members in bounded pages. `cursor` is `nil` to start; returns the next
  page of user_ids plus a cursor (`nil` when exhausted). Never materializes the
  full roster — this is how we support unlimited group size.
  """
  @callback stream_members(
              Types.conversation_id(),
              cursor :: term() | nil,
              limit :: pos_integer()
            ) ::
              {:ok, [Types.user_id()], next_cursor :: term() | nil} | {:error, term()}

  @doc """
  The conversations a user belongs to. A connecting session uses this to
  subscribe to its conversations' online fan-out groups. A production adapter
  keeps this indexed (and may page it); the engine only needs it on connect.
  """
  @callback conversations_for(Types.user_id()) ::
              {:ok, [Types.conversation_id()]} | {:error, term()}

  @doc """
  Member count for a conversation. MUST be cheap (O(1)) — the engine uses it to
  decide receipt policy (1:1 vs group) without ever counting the roster itself.
  """
  @callback member_count(Types.conversation_id()) ::
              {:ok, non_neg_integer()} | {:error, term()}
end
