defmodule Chat.Ports do
  @moduledoc """
  Resolves the concrete adapter module configured for each port.

  The engine calls e.g. `Chat.Ports.persistence().append(...)`. Which module
  that returns is decided entirely by the body via config (`config/config.exs`),
  so the core stays free of any concrete database.

  All ports are **required** except one — `persistence/0`, `conversation_store/0`,
  `auth/0`, `cursor_store/0`, `presence_store/0`, and `receipt_store/0` each raise
  with guidance if unset. `offline_queue/0` is the only **optional** port (the
  offline push-notification wake hook); it returns `nil` when unset, disabling
  offline pushes. The bundled in-memory reference adapters satisfy all required
  ports out of the box (see `config/config.exs`).
  """

  @doc "The module implementing `Chat.Persistence.Port`."
  @spec persistence() :: module()
  def persistence, do: fetch!(:persistence_adapter, Chat.Persistence.Port)

  @doc "The module implementing `Chat.ConversationStore.Port`."
  @spec conversation_store() :: module()
  def conversation_store, do: fetch!(:conversation_store_adapter, Chat.ConversationStore.Port)

  @doc "The module implementing `Chat.Auth.Port`."
  @spec auth() :: module()
  def auth, do: fetch!(:auth_adapter, Chat.Auth.Port)

  @doc "The module implementing `Chat.CursorStore.Port`."
  @spec cursor_store() :: module()
  def cursor_store, do: fetch!(:cursor_store_adapter, Chat.CursorStore.Port)

  @doc "The module implementing `Chat.PresenceStore.Port`."
  @spec presence_store() :: module()
  def presence_store, do: fetch!(:presence_store_adapter, Chat.PresenceStore.Port)

  @doc "The module implementing `Chat.ReceiptStore.Port`."
  @spec receipt_store() :: module()
  def receipt_store, do: fetch!(:receipt_store_adapter, Chat.ReceiptStore.Port)

  @doc "The module implementing `Chat.OfflineQueue.Port` (push-notification hook; later milestone)."
  @spec offline_queue() :: module() | nil
  def offline_queue, do: Application.get_env(:chat_engine, :offline_queue_adapter)

  defp fetch!(key, behaviour) do
    Application.get_env(:chat_engine, key) ||
      raise """
      No #{inspect(key)} configured for :chat_engine.

      The engine needs a concrete adapter implementing #{inspect(behaviour)}.
      Wire one in your config, e.g.:

          config :chat_engine, #{key}: MyApp.SomeAdapter
      """
  end
end
