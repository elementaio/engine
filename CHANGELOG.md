# Changelog

All notable changes to `chat_engine` are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Hardening toward production-readiness (ENGINE_STUDY.md §5), all firewall-legal (stdlib + the existing
`:telemetry` seam only).

### Added
- **Ephemeral / no-persist channel mode** (activates the previously-dead `Message.kind`): a
  `kind: :ephemeral` message (via `Chat.inject/2`, or a client `:send` with `kind: :ephemeral`) is fanned
  out live to online subscribers but **not** persisted — no `seq` consumed, no offline wake, no cursor
  advance, never in history. Lossy by design; the path for live feeds, dashboards, presence signals, and
  IoT telemetry. Returns `{:ok, :ephemeral}`; a client `:send` gets an `:ephemeral`-status ack.
- **History pagination cursor** (API-3 / CC-5): `Chat.history_page/3` returns `%{messages, next_after,
  more?}` (a `limit + 1` look-ahead detects "more" with no extra round-trip and no port change). The
  client-facing `:sync` verb now returns ONE page with a `seq` continuation cursor + `more` flag,
  advances the device cursor over delivered pages (unifying it with auto catch-up), and bounds page size
  at `:sync_page_max`. Auto catch-up's drain loop was refactored onto the same primitive.
- **Offline push wake hook wired** (REL-5): when a durable message lands, every conversation member with
  no online session is sent through `Chat.OfflineQueue.Port.notify/3` — off the conversation owner's hot
  path (a supervised task), bounded by `:offline_push_max_members` (default 10_000, telemetered when
  exceeded). The port was redefined from an unused store-and-forward queue into a single user-level wake
  hook, matching the engine's cursor-based recovery (a lost push costs a late wake, never a lost message).
- **Boot-time config validation** (`Chat.Config.validate!/0`, run from `Chat.Application.start/2`):
  required ports must be configured, loadable, and implement their behaviour; numeric knobs must be
  positive integers — a misconfigured body fails loudly at boot.
- **Health & graceful drain**: `Chat.ready?/0` (LB probe) plus `Chat.drain/0` / `Chat.resume/0`; a
  draining node refuses new sessions (`{:error, :draining}`) while existing ones keep running.
- **`Chat.Persistence.PortTest`** — a shared, executable contract test-kit adapter authors run against
  their store (`use Chat.Persistence.PortTest, adapter: Mod`).
- **CP-fencing** of the conversation log (P0): optional `Chat.Persistence.Port.append/3` compare-and-set
  on `(conv, expected_seq)`; the losing owner steps down instead of forking the log.
- Telemetry at the security/fan-out decision points; `mix credo` + `mix dialyzer` + the multi-node suite
  now gate CI.

### Changed
- **Authenticate on connect and authorize every inbound verb** through `Chat.Auth.Port` (P0); the bundled
  in-memory adapter stays allow-all, so trusted in-VM bodies are unaffected.
- `Chat.Router.ensure_conversation/1` now returns `{:ok, pid} | {:error, {:owner_unreachable, node}}` and
  bounds the cross-node `:erpc` (5s) so a partitioned owner degrades the send path instead of hanging it.
- Catch-up drains the durable log page-by-page (no silent 100-message cap); the hot path degrades instead
  of crashing on legal `{:error, _}` port returns; `member_count` failures fail closed (P0).
- Completed the `Chat.Envelope.type` union (added `:typing`/`:system`/`:presence`/`:presence_query`/
  `:read_state`) so the struct the code builds satisfies `Envelope.t()`.

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
