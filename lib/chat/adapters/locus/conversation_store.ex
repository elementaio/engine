defmodule Chat.Adapters.Locus.ConversationStore do
  @moduledoc """
  `Chat.ConversationStore.Port` on Locus: membership as a set per conversation
  (`vox:{c}:members`) plus a per-user reverse index (`vox:u:<u>:convs`), so
  `conversations_for/1` never scans. `member_count/1` is `SCARD` — O(1) as the
  port requires — and `stream_members/3` pages with `SSCAN`, never
  materializing a roster.
  """

  @behaviour Chat.ConversationStore.Port

  alias Chat.Adapters.Locus

  @impl true
  def member?(cid, uid) do
    case Locus.command(["SISMEMBER", Locus.conv_key(cid, "members"), uid]) do
      {:ok, 1} -> true
      _ -> false
    end
  end

  @impl true
  def add_member(cid, uid) do
    both(["SADD", Locus.conv_key(cid, "members"), uid], ["SADD", Locus.user_convs_key(uid), cid])
  end

  @impl true
  def remove_member(cid, uid) do
    both(["SREM", Locus.conv_key(cid, "members"), uid], ["SREM", Locus.user_convs_key(uid), cid])
  end

  @impl true
  def stream_members(cid, cursor, limit) do
    cursor = cursor || "0"

    case Locus.command(["SSCAN", Locus.conv_key(cid, "members"), cursor, "COUNT", limit]) do
      {:ok, [next, members]} -> {:ok, members, if(next == "0", do: nil, else: next)}
      {:ok, {:error, msg}} -> {:error, {:locus, msg}}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def conversations_for(uid) do
    case Locus.command(["SMEMBERS", Locus.user_convs_key(uid)]) do
      {:ok, convs} when is_list(convs) -> {:ok, convs}
      {:ok, {:error, msg}} -> {:error, {:locus, msg}}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def member_count(cid) do
    case Locus.command(["SCARD", Locus.conv_key(cid, "members")]) do
      {:ok, n} when is_integer(n) -> {:ok, n}
      {:ok, {:error, msg}} -> {:error, {:locus, msg}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp both(cmd_a, cmd_b) do
    case Locus.pipeline([cmd_a, cmd_b]) do
      {:ok, replies} ->
        case Enum.find(replies, &match?({:error, _}, &1)) do
          nil -> :ok
          {:error, msg} -> {:error, {:locus, msg}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end
end
