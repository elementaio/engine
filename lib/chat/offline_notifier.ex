defmodule Chat.OfflineNotifier do
  @moduledoc """
  Computes a message's offline recipients and fires the `Chat.OfflineQueue.Port`
  wake hook for each — off the conversation owner's hot path (REL-5).

  A conversation owner kicks `run/3` into `Chat.TaskSupervisor` right after it
  fans a durable message out, so the single-writer never blocks on a roster scan
  or a slow push backend. Everything here is best-effort: the message is already
  durable, so a recipient catches up by cursor on reconnect even if a push is
  skipped, dropped, or the adapter errors.

  ## Recipients

  Offline recipients = conversation members **minus** the sender **minus** anyone
  with an online session right now. Online members already got the live fan-out.

  ## Bounded by roster size

  Per-message, per-recipient push does not make sense for very large rosters (a
  broadcast channel with a million members). When `member_count` exceeds
  `:offline_push_max_members` (default 10_000) the notify is skipped and a
  `[:chat, :offline, :skipped_large]` telemetry event is emitted — never a silent
  cap. Bodies that need fan-out push at that scale should do it from their own
  pipeline keyed off the changefeed, not synchronously per message.
  """
  require Logger
  alias Chat.Message

  @default_max_members 10_000

  @doc "Notify offline members of `msg` in `conversation_id` via `mod` (an OfflineQueue adapter)."
  @spec run(module(), Chat.Types.conversation_id(), Message.t()) :: :ok
  def run(mod, conversation_id, %Message{} = msg) do
    max = max_members()

    case Chat.member_count(conversation_id) do
      {:ok, n} when n > max ->
        :telemetry.execute(
          [:chat, :offline, :skipped_large],
          %{members: n},
          %{conversation_id: conversation_id}
        )

      {:ok, _} ->
        notify_recipients(mod, conversation_id, msg)

      {:error, reason} ->
        # Unknown roster size ⇒ skip rather than risk a storm against a flaky store.
        :telemetry.execute(
          [:chat, :offline, :skipped_unknown_size],
          %{},
          %{conversation_id: conversation_id, reason: reason}
        )
    end

    :ok
  end

  defp notify_recipients(mod, conversation_id, %Message{} = msg) do
    recipients =
      conversation_id
      |> Chat.ConversationStore.members()
      |> Enum.reject(&(&1 == msg.sender_id or Chat.online?(&1)))

    Enum.each(recipients, &safe_notify(mod, &1, conversation_id, msg))

    :telemetry.execute(
      [:chat, :offline, :notified],
      %{recipients: length(recipients)},
      %{conversation_id: conversation_id}
    )
  end

  # One bad recipient (adapter error or raise) must not abort the rest of the loop.
  defp safe_notify(mod, user_id, conversation_id, msg) do
    case mod.notify(user_id, conversation_id, msg) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning(
          "offline notify failed (user=#{inspect(user_id)}, conv=#{inspect(conversation_id)}): #{inspect(reason)}"
        )
    end
  rescue
    e ->
      Logger.warning(
        "offline notify raised for user=#{inspect(user_id)}: #{Exception.message(e)}"
      )
  end

  defp max_members,
    do: Application.get_env(:chat_engine, :offline_push_max_members, @default_max_members)
end
