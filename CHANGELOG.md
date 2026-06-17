# Changelog

All notable changes to `chat_engine` are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0]

Initial standalone release — extracted from the Pulsar umbrella into its own package so the engine is a
first-class, independently-versioned product.

### Added
- The pure real-time core: sessions, the per-conversation single-writer owner (ordering authority),
  cluster-global fan-out and presence via `:syn`, receipts, and offline catch-up by per-device cursor.
- The seven ports (`Chat.Persistence.Port`, `Chat.ConversationStore.Port`, `Chat.CursorStore.Port`,
  `Chat.ReceiptStore.Port`, `Chat.PresenceStore.Port`, `Chat.Auth.Port`, `Chat.OfflineQueue.Port`).
- In-memory reference adapters (`Chat.Adapters.InMemory.*`) bundled as the executable spec and zero-setup
  default, plus `Chat.Adapters.TestTransport`.
- `Chat.FirewallTest` — fails the build if the core gains a transport/web/DB dependency.

### Changed
- Flattened from two umbrella apps (`chat_engine` + `chat_engine_adapters`) into a single library: the
  in-memory reference adapters now ship inside `:chat_engine`. The firewall is preserved (the adapters use
  only stdlib).
