import Config

# ─────────────────────────────────────────────────────────────────────────────
# Default port wiring.
#
# The core (:chat_engine) ships ZERO database code — it only knows the behaviour
# contracts in `Chat.*.Port`. Out of the box we point each port at the in-memory
# reference adapters bundled in this library, so it runs with no setup for
# dev/test/demo.
#
# A real body OVERRIDES these (in its own config/config.exs or runtime.exs) to
# point at Postgres / Cassandra / Locus / … adapters it implements.
# ─────────────────────────────────────────────────────────────────────────────

config :chat_engine,
  persistence_adapter: Chat.Adapters.InMemory.Persistence,
  conversation_store_adapter: Chat.Adapters.InMemory.ConversationStore,
  cursor_store_adapter: Chat.Adapters.InMemory.CursorStore,
  presence_store_adapter: Chat.Adapters.InMemory.PresenceStore,
  receipt_store_adapter: Chat.Adapters.InMemory.ReceiptStore,
  auth_adapter: Chat.Adapters.InMemory.Auth,
  # above these member counts, presence/typing are pull-only (not pushed) to
  # avoid flooding huge groups with ephemeral signals.
  presence_max: 100,
  typing_max: 100
