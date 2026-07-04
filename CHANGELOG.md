# Changelog

All notable changes to `chat_engine` are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.0] — 2026-07-04

Lead-review assessment hardening (`docs/ASSESSMENT-2026-07.md`) — every confirmed core-side
finding fixed, each with a regression test. All firewall-legal (stdlib + the `:telemetry` seam).

### Fixed
- **Silent message loss — the cursor over-advance family (B2/B3/B4/B6).** The per-device delivery
  cursor now advances **only across the contiguous delivered prefix** (`Session.note_delivered/3`):
  a delivery whose seq sits above an undelivered gap is buffered and folded in only once the gap
  closes. So a backpressure-shed delivery, a swallowed catch-up error, an unvalidated client `:read`
  seq, or a send-ack racing a lower-seq peer deliver can no longer leapfrog the cursor past a message
  the device never received. A catch-up read error now **stops the session** (clean reconnect from the
  un-advanced cursor) and emits `[:chat, :session, :catch_up_failed]` instead of logging and going
  live. `:read` records only the read watermark. An overgrown gap forces a resync.
- **Membership authorization gap (B1).** New opt-in `enforce_membership: true` gates every
  conversation-scoped verb (`:send/:read/:sync/:read_state/:typing`) on the store's `member?/2` — so an
  allow-all `Auth` adapter no longer lets an authenticated user read or write any conversation in its
  tenant. Fails closed on a store fault; emits `[:chat, :session, :membership_denied]`.
- **Cross-node size-cache staleness (B5).** `invalidate_size` now routes to the owner node (like every
  other owner op) instead of a node-local `Registry.lookup`, so a membership change from a non-owner
  node actually reaches the owner (correct receipt policy, no O(n) receipt storm / read-state leak).
- **OfflineNotifier task pileup (B7).** The offline-wake `Task.Supervisor` is now bounded
  (`offline_max_inflight`, default 10k); over the cap it sheds with `[:chat, :offline, :shed]` (the
  message is already durable) instead of growing unbounded under a slow push/store backend.
- **Presence single-point-of-failure (B8).** Presence port I/O (`touch`, broadcast) is wrapped so an
  adapter raise/timeout degrades to a logged no-op instead of crashing Presence and orphaning **every**
  live session's monitor.
- **Presence never converging offline across nodes (B9).** A suppressed offline (a peer's not-yet-pruned
  `:syn` pid) now schedules a delayed re-check (`offline_recheck_ms`, default 3s) that declares offline
  once `:syn` has converged.
- **`presence_query` authz slot confusion (B10).** User-targeted actions now authorize on a tagged
  `{:user, id}` resource, not a bare user id in the `conversation_id` slot.
- **Uncapped ephemeral fast lane (B11).** `Chat.broadcast_ephemeral/2` now applies the
  `:max_payload_bytes` cap (returns `{:error, :too_large}`) — it was the only send lane with no size cap.
- **Overload shedding of `:system` control frames (B14).** Membership-control frames (join/leave/removal)
  are exempt from backpressure shedding — they are not in the durable log, so a dropped one can't be
  recovered by catch-up.
- **`Router.ensure_local/1` non-exhaustive case (B15).** An unexpected `start_child` result no longer
  raises `CaseClauseError` into the calling session; it degrades to `{:error, {:owner_unreachable, _}}`,
  and the local owner path is wrapped symmetrically with the remote one.

### Added
- **Drain-aware placement (B12).** A draining node advertises itself in a cluster-global `:syn` marker
  (auto-pruned on death); HRW placement excludes it, so its owned conversations pre-migrate to healthy
  nodes **while it is still up** rather than dying on stop. Falls back to the full node set so placement
  is never empty.
- **Graceful-stop primitive (B16).** `Chat.await_drained/2` (+ `Chat.Health.live_session_count/0`) drains
  then waits for live sessions to disconnect — the primitive for a body's pre-stop hook.
- **`require_fence: true` boot check (B17).** Refuses to boot clustered on a persistence adapter lacking
  the `append/3` CP fence, instead of silently falling back to unprotected `append/2`.
- **Contract-kit concurrency case (B13).** `Chat.Persistence.PortTest` now races N writers at a contested
  `expected_seq` and asserts exactly one commit + N−1 `{:fenced, current}` — catching a read-then-write
  fence (no advisory lock / `SELECT … FOR UPDATE`) that every single-process test passes.
- **Error-path telemetry** on the store-and-forward hinge: `[:chat, :cursor, :error]`,
  `[:chat, :receipt, :error]`, `[:chat, :presence, :broadcast_error]`,
  `[:chat, :session, :conversations_for_error]` — previously log-only blind spots.
- **Gated Locus contract CI lane** (`locus-contract`) that builds Locus and runs the real-adapter
  durability/fence suite with `LOCUS_REQUIRED=1`; the local suite now **loudly** excludes `:locus` when
  the binary is absent instead of skipping it silently (test gap #1).

### Notes
- **Body follow-ups** (out of the firewall, tracked in the assessment): koine's allow-all
  `Koine.Auth.authorize/3` should consult membership (+ delete its false "engine enforces membership"
  comment); vox's Ecto `append/3` should hold a `pg_advisory_xact_lock` (or map the unique violation to
  `{:fenced, current}`) — the new contract-kit concurrency case will now fail until it does. B18 (owner
  step-down backoff, LOW/PLAUSIBLE) is deferred.

## [0.3.0] — 2026-07-04

### Added
- `Chat.broadcast_ephemeral/2` — a fast lane that fans a live-only message straight to
  online `:syn` subscribers, bypassing the per-conversation writer, membership store, and
  owner GenServer entirely. For very high-volume ephemeral traffic (WebRTC call signaling,
  presence, telemetry) where funnelling every message through the single ordered writer
  serializes the burst behind durable work. `inject/2` with `kind: :ephemeral` is still the
  ordered path.

## [0.2.0] — 2026-07-03

Hardening toward production-readiness (ENGINE_STUDY.md §5), all firewall-legal (stdlib + the existing
`:telemetry` seam only).

### Added
- **Stock-Redis compatibility for the Locus adapter set.** The only non-standard command it used
  was `SETMAX` (a Locus verb), in the cursor/presence/receipt stores. A new `monotonic: :cas`
  option (`config :chat_engine, :locus, monotonic: :cas`) swaps it for a portable
  `WATCH`/`GET`/`MULTI`/`SET` loop, so the *same* adapter runs unchanged on Redis / Valkey /
  KeyDB. Default stays `:setmax` (the one-round-trip fast path on Locus). Every port was driven
  against a real vanilla Redis to confirm; a `:cas`-mode test is in `locus_test.exs`.
- **Locus adapter set** (`Chat.Adapters.Locus.*`) — a production implementation of six ports
  (persistence with the CP fence, conversations, cursors, presence, receipts, offline queue) on
  [Locus](https://github.com/elementaio/locus), over a bundled dependency-free RESP2 client
  (`:gen_tcp`; the firewall stays green). Message logs are Locus streams whose entry ids are the
  seqs; seq + log + idempotency commit in one `MULTI`/`EXEC`; monotonic watermarks use `SETMAX`;
  offline wakes are `BLPOP` jobs. Verified by the persistence contract kit + five direct suites
  against a real Locus (auto-skipped when the sibling binary is absent). This is the state plane
  of **Vox** — the productized engine+body+Locus bundle.
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
