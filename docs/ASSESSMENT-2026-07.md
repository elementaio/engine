# chat_engine — Lead Review Assessment

> Produced by a multi-agent swarm (2026-07): 4 context mappers (engine + koine/vox/motus) → 6 dimensioned
> finders → adversarial per-finding verification → synthesis. 32 findings survived of 35 raised.

> **STATUS — IMPLEMENTED 2026-07-04 (chat_engine v0.4.0).** Every confirmed *core-side* finding is fixed
> with a regression test; `mix test` green (116 core + 9 new audit-fix tests), `mix credo` / `mix format`
> / `--warnings-as-errors` clean. See `CHANGELOG.md` [0.4.0] and `test/chat/audit_fixes_test.exs`.
>
> - **Done (core):** B1 (opt-in `enforce_membership`), the cursor over-advance family **B2/B3/B4/B6**
>   (contiguous high-water invariant + `catch_up_failed` stop), B5, B7, B8, B9, B10, B11, B12, B13
>   (contract-kit concurrency race), B14, B15, B16 (`Chat.await_drained/2`), B17 (`require_fence`),
>   store-error telemetry, and **test gap #1** (loud `:locus` exclusion + a gated `locus-contract` CI lane).
> - **Deferred:** B18 (owner step-down backoff — LOW/PLAUSIBLE).
> - **Body follow-ups (out of the firewall, NOT in this repo):** the koine `Koine.Auth.authorize/3`
>   membership fix + false-comment deletion (B1 immediate); the vox Ecto `append/3` advisory-lock fix
>   (B13) — the new contract-kit concurrency case will fail until vox does this. Distributed test gaps
>   (#3–#6) still want a multi-node harness.

## How the three "clients" actually relate to the engine (premise corrections)

- **koine** — deep embedder; git dep `chat_engine` tag **v0.2.0**; implements all 7 ports over **Locus**
  (`Chat.Adapters.Locus`). Multi-tenant, plaintext, JWT + WS gateway.
- **vox** — **also a deep embedder**, not a pure JS wire client: `apps/server` is an Elixir/Phoenix relay
  that embeds chat_engine (Ecto/Postgres + SQLite); the JS/native pieces are its edge/SDK. Uses
  `broadcast_ephemeral` for WebRTC signaling.
- **motus** — **does NOT use chat_engine at all.** It talks to **Locus directly** over the Redis wire in
  Go and re-implements single-writer + fan-out ideas independently. No dependency anywhere. (No action for
  the engine; but worth deciding whether motus *should* share the contract.)

---

## 1. Executive summary

The engine is a well-factored ports-and-adapters core with sound single-node foundations: persist-before-ack,
monotonic gap-free `seq`, cursor catch-up, and a genuinely linearizable CP-fence in the real (Locus) adapter.
Most defects are not in the happy path — they cluster in three places: (a) **authorization is a hole the core
silently delegates to bodies that no-op it**, (b) several **error/backpressure paths silently over-advance the
delivery cursor**, defeating the gap-free guarantee the whole product is sold on, and (c) **multi-node
correctness features (size-cache invalidation, presence convergence, owner drain) are shipped, advertised, and
untested/unexercised**. The three biggest things to fix, in order: **(1) the membership-authorization gap
(critical, exploitable in koine today)**, **(2) the cursor over-advance family — catch-up-error-swallow +
backpressure-shed + client-supplied `:read` seq — which cause silent permanent message loss**, and **(3) the
Locus contract-kit auto-exclusion in CI, which means the load-bearing durability/fence contract is never
enforced by default.** Nothing here is a happy-path data-corruption bug, but the guarantees degrade silently
and invisibly under load, faults, and multi-node — exactly where a messaging core is judged.

---

## 2. Confirmed bugs (ranked)

### B1 — No membership check on any inbound verb → intra-tenant read/write of every conversation `[CRITICAL, CONFIRMED]`
`lib/chat/session.ex:152` (and `:207`, `:230`, `conversation.ex:122`)
The core gates `:send/:sync/:read/:read_state/:typing/:presence_query` **only** on the `Auth` port's
`authorize/3`; it never checks the authenticated user is a member of `env.conversation_id`. Both shipped
bodies return `authorize -> :ok`, and koine's adapter carries a **false comment** claiming "the engine
enforces membership."
- **Failure:** koine user `alice` sends `{"t":"sync","conv":"secret-room","after":0}` and receives the full
  plaintext history + sender ids of a room she was never added to; `send`/`read` let her inject messages and
  poison/forge receipts across every conversation in her tenant.
- **Bites:** koine (directly exploitable). vox not currently reachable (its channel never forwards `:sync/:read`
  to the session) but any future frame inherits it.
- **Fix:** add an opt-in `enforce_membership: true` that gates those verbs on `conversation_store().member?/2`
  (the primitive already exists, wired into stores, just not called) and push `:error :forbidden` like the
  authorize-deny path. Immediately: fix `Koine.Auth.authorize/3` to consult membership and delete the false comment.

### B2 — Catch-up store error swallowed as success → silent permanent gap `[HIGH, CONFIRMED]`
`lib/chat/session.ex:438`
On reconnect `drain/3` turns a mid-catch-up `{:error, _}` (transient store blip/pool timeout/partition) into
`Logger.warning` + `:ok`, indistinguishable from "no more pages." The session goes live; the next live
`:message` advances the cursor (`:303`) past the un-fetched range; the next reconnect reads the raised cursor
and never re-fetches the skipped seqs. **Zero telemetry** — Logger only.
- **Bites:** koine/vox — a momentary store hiccup during reconnect drops a window of messages for that device forever.
- **Fix:** on catch-up read error emit `[:chat,:session,:catch_up_failed]` and stop the session (temporary → clean
  reconnect from same cursor) or push `:sync_incomplete`; never let live delivery advance the cursor while a
  catch-up gap is outstanding.

### B3 — Backpressure shedding of a durable delivery permanently loses it `[HIGH, CONFIRMED]`
`lib/chat/session.ex:303` (shed at `:467-483`)
`deliver/2` sheds via `cast_unless_overloaded` above `:max_mailbox` (default 10k) **without enqueuing**, but
every delivered `:message` advances the cursor to its own `seq`. If a lower-seq delivery is shed while a later
one gets through, the cursor leapfrogs the gap and catch-up never re-reads it. Contradicts the module's own
"dropped durable deliveries are recovered by catch-up" claim — recovery only holds if shed messages are always
the highest in flight, which shedding doesn't guarantee.
- **Bites:** koine group fan-out to slow WebSocket clients under load.
- **Fix:** stop advancing the cursor on live delivery (rely on ordered catch-up + client dedup), or advance only
  to the contiguous high-water mark, or mark the session "gapped" and force a resync when a durable deliver is shed.

> **B2/B3 + B6/B7 share a root cause:** the delivery cursor is advanced from non-contiguous, unvalidated, or
> best-effort-lossy sources. A single "advance only to the contiguous delivered prefix" invariant fixes most of
> the family.

### B4 — `:read` advances the delivery cursor from an unvalidated client seq `[MEDIUM, CONFIRMED]`
`lib/chat/session.ex:207`
The `:read` handler conflates the per-user read watermark with the per-device delivery cursor:
`Cursors.advance(device_ref, conv, env.seq)` on a **client-supplied** seq with no clamp. A device that reports
reading ahead of delivery (cross-device read-sync, optimistic "mark all read") moves its own catch-up cursor
past messages it never received.
- **Bites:** koine + vox (both map a `:read` frame).
- **Fix:** on `:read` only record the receipt; do not advance the delivery cursor, or clamp to `min(env.seq, current_cursor)`.

### B5 — Cross-node membership change never invalidates the remote owner's size cache `[MEDIUM (was high), CONFIRMED]`
`lib/chat.ex:224`
`invalidate_size/1` does a **node-local** `Registry.lookup`; the owner lives on the HRW node. A membership change
from any non-owner node never reaches the owner, which keeps a stale `member_count` and thus the wrong
`receipts?` policy indefinitely. A 1:1 grown to 3 keeps emitting per-user receipts (O(n) storm **+ read-state
privacy leak** to the group); a group shrunk to 2 emits none. Persists until owner restart.
- **Bites:** multi-node koine only (single-node is immune; no consumer runs multi-node in prod today — hence
  medium, not high). This is the one op that bypasses the `Router`/`owner_node` routing every other owner call uses.
- **Fix:** route invalidation to the owner via `Cluster.owner_node` + `:erpc.cast` (mirror the send path), or key
  the size cache to a membership epoch fetched with `member_count`.

### B6 — Sender ack advances its own cursor past concurrently-queued lower-seq messages `[MEDIUM, CONFIRMED]`
`lib/chat/session.ex:174`
On `:send` ack the session advances its cursor to its assigned `seq`. A peer's lower-seq `:deliver` cast can be
queued behind the ack while the session was blocked in the `submit` owner-call. If the session crashes in that
window (node roll, transport error), catch-up resumes from the higher cursor and the peer's lower-seq message is
never redelivered to that device.
- **Bites:** koine/vox during node rolls under concurrent group traffic. Narrow window, permanent loss.
- **Fix:** don't advance the cursor on the sender's own ack (the sender is excluded from its own fan-out, so
  catch-up covers it idempotently), or advance only to the contiguous prefix.

### B7 — OfflineNotifier task pileup — uncapped supervisor, ignored `start_child` result `[MEDIUM (was high), CONFIRMED]`
`lib/chat/conversation.ex:249`; supervisor `application.ex:38`
Every durable append spawns one `OfflineNotifier` task into a `:max_children: :infinity` `Task.Supervisor`; the
return is discarded, no backpressure. Each task does O(roster) store scan + per-member `:syn` lookup + push. Under
sustained durable-write load with a slow push/store backend, in-flight tasks grow unbounded → memory +
connection-pool exhaustion taking down all sessions on the node. (Downgraded to medium: per-conversation spawn
rate is naturally capped by synchronous append latency, so it needs a compound abnormal condition. The
ephemeral/GPS firehose does **not** hit this path.)
- **Fix:** cap `max_children`, inspect `start_child`, shed with `[:chat,:offline,:shed]` telemetry (the message is
  already durable), or replace with one bounded per-conversation coalescing worker.

### B8 — Presence crash silently orphans all existing monitors `[MEDIUM (was high), CONFIRMED]`
`lib/chat/presence.ex:39`
The single Presence GenServer does unguarded port I/O inline (`touch`, `conversations_for`, `dispatch`). Any
adapter **raise/exit** (e.g. the Locus client `GenServer.call` timeout exiting the caller) crashes Presence;
`one_for_one` restarts it with empty state, dropping every `Process.monitor` for currently-connected sessions.
Their later disconnects fire no `:DOWN`, so `last_seen` is never written and the offline delta never broadcast.
(Medium not high: the primary online signal reads live `:syn` and self-heals; only `last_seen` + reactive offline
deltas degrade, recovering per-user on reconnect.)
- **Fix:** wrap port calls in try/rescue (mirror `OfflineNotifier.safe_notify`) so an adapter fault degrades to a
  logged no-op; rebuild monitors from `:syn` `:users` after restart.

### B9 — Presence never converges to offline across nodes within the `:syn` prune window `[MEDIUM (was high), CONFIRMED]`
`lib/chat/presence.ex:78`
`online_elsewhere?` excludes only the **local** dying pid. When a user's last two sessions (one per node) die
close together, each node still sees the other's not-yet-pruned pid in `:syn` and both suppress the offline
broadcast; no `last_seen`, user stuck "online." (Medium: multi-node only, narrow timing, and fresh `presence_of`
queries self-heal once `:syn` converges — only pushed deltas + `last_seen` durably degrade.)
- **Fix:** schedule a delayed re-check after suppression, or drive offline off `:syn` `:users` membership-change
  events (fires once on true last leave).

### B10 — `presence_query` leaks any user's online/last-seen + passes a user id into the `conversation_id` authz slot `[MEDIUM, CONFIRMED]`
`lib/chat/session.ex:283`
`authorize(:presence_query, user_id, env.user_id)` puts the **target user** into the arg the port documents as
`conversation_id`. With allow-all bodies, any authenticated user resolves any same-tenant user's online state +
exact `last_seen` with no relationship gate; and any body that *did* implement membership authz would mis-evaluate
(user id treated as conversation id). Within-tenant, metadata-only.
- **Fix:** distinct authz contract for user-targeted actions (`{:user, id}` vs `{:conversation, id}` or
  `authorize_presence/2`); document presence has no default restriction.

### B11 — `broadcast_ephemeral/2` skips the payload-size cap (uncapped fast lane) `[MEDIUM, CONFIRMED]`
`lib/chat.ex:160`
`broadcast_ephemeral` calls `Fanout.dispatch` directly, bypassing the owner where `check_payload`/
`:max_payload_bytes` lives. It is the **only** send lane with no size cap. A vox client sending
`{"ephemeral":true,"envelope":<10 MB>}` gets it fanned out uncapped. (The "sender echo / no authz" framing is
**down-ranked/refuted**: no send lane does membership/authz — that's the body's job by design — and vox's
single-member inboxes avoid the echo. The real, confirmed defect is the missing size cap.)
- **Fix:** run `check_payload` (or inline `byte_size` check) before dispatch; size-check vox's ephemeral branch too.

### B12 — `drain()` doesn't migrate conversation owners → ~5s send stall per owned conversation on node roll `[MEDIUM, CONFIRMED]`
`lib/chat/health.ex:41`
`drain()` only flips a node-local flag consulted by `Session.connect`; the draining node stays in
`Cluster.nodes()`, keeps winning HRW for its ~1/N conversations, and those owners just die on stop, incurring up
to the 5s `owner_unreachable` window before re-election. Undermines the advertised "roll a node without dropping
live traffic." (Worst-case 5s; typically faster once peers see `nodedown`. Bounded, self-healing, no data loss.)
- **Fix:** exclude drained nodes from `Cluster.nodes()`/`owner_node` so ownership pre-migrates while the node is
  still up; proactively stop quiesced owners.

### B13 — Contract kit never tests `append/3` under concurrency; vox shipped a non-conforming read-then-write fence that passes green `[MEDIUM, CONFIRMED]`
`lib/chat/persistence/port_test.ex:100`; vox `.../ports/db/persistence.ex:32-46`
Both `append/3` kit tests are single-process, so a read-then-write fence (the exact non-conformance
`port.ex:34-39` forbids) passes. Vox's Ecto adapter is precisely that (no advisory lock, no `SELECT … FOR
UPDATE`). On multi-node Postgres a fence loss surfaces as a **generic** error, not `{:error, {:fenced, current}}`,
so `Chat.Conversation`'s step-down never fires. (Medium not high: vox's `UNIQUE(conversation_id, seq)` index
prevents a divergent commit — no corruption — and it self-heals on the next append; latent until vox runs
multi-node Postgres.)
- **Fix:** add a concurrency case to `PortTest` (N tasks, same `expected_seq` → exactly one `{:ok}`, N-1
  `{:fenced,_}`); fix vox to hold `pg_advisory_xact_lock` (as koine's does) or convert the unique-violation to
  `{:fenced, current}`.

### B14 — Overload shedding drops non-durable control envelopes with no recovery `[LOW, CONFIRMED]`
`lib/chat/session.ex:68`
The shed path also drops `:system` (group join/**leave**/removal), `:receipt`, `:typing`, `:presence` — none in
the durable log, so catch-up can't replay them. A client overloaded during its own removal can keep showing a
group it was kicked from. (Low: cosmetic — the user is truly removed and unsubscribed, and `authorize` still
denies all actions; receipts already documented best-effort; trigger is a 10k-deep mailbox.)
- **Fix:** exempt `:system` membership control frames from the cap, or reconcile membership via a periodic
  state-sync frame; document the non-durable lane is lossy under overload.

### B15 — `Router.ensure_local/1` non-exhaustive case kills the calling session on the local path `[LOW, PLAUSIBLE]`
`lib/chat/router.ex:58`
Matches only `{:ok, pid}` / `{:already_started, pid}`; any other `start_child` result (`:max_children`, `:ignore`,
an init crash) raises `CaseClauseError`. The local owner path isn't wrapped in the try/catch the remote path uses,
so it propagates into and kills the session. Latent today (supervisor is `:infinity`, `init` can't fail) —
reachable only under process-table/memory exhaustion.
- **Fix:** add a catch-all mapping to `{:error, :owner_unavailable}`; wrap the local branch symmetrically with the
  remote path.

### B16 — No shutdown-drain coordination; SIGTERM hard-kills sessions and drops Presence `last_seen` `[LOW, CONFIRMED]`
`lib/chat/application.ex:26`
No `terminate`/`trap_exit` anywhere; `drain/0` only blocks new sessions. On SIGTERM, `Session.Supervisor` is torn
down before Presence, flooding (then killing) Presence mid-flush. Only genuine loss is `last_seen` for users
online at shutdown, overwritten seconds later on reconnect elsewhere. This is a **BODY responsibility** — the
engine should just provide the primitive.
- **Fix:** provide a graceful-stop helper (`drain()` + bounded busy-wait on live session count via
  `Session.Supervisor` child count) for the body's pre-stop hook.

### B17 — Split-brain fence is documentation-enforced, not boot-enforced `[LOW, PLAUSIBLE]`
`lib/chat/conversation.ex:269`
HRW elects two owners under a partition; `fenced_append` silently falls back to unprotected `append/2` when
`append/3` isn't exported, and the bundled in-memory adapter is node-local so its fence is per-node.
`Config.validate!` never requires `append/3`. **But** the only shipped production adapter (Locus) is a conforming
shared fence, koine's real deploy is safe, and the hazard is documented in ~5 places + auto-tested by the contract
kit for real adapters. (Refuted as critical: no supported/shipped config exhibits the loss; the fenced-telemetry
the fix asks for already exists.)
- **Fix (defense-in-depth):** if a clustering signal is available, refuse to boot clustered on an adapter lacking
  `append/3`; otherwise keep as a documented integrator contract.

### B18 — Fenced owner step-down → retry churn on the minority side of a partition `[LOW, PLAUSIBLE]`
`lib/chat/conversation.ex:299`
A fenced owner stops and returns a retriable error; the router re-elects the same local node and starts a fresh
owner. **Not** the "tight livelock" originally claimed: `fenced_append` re-seeds `latest` from the shared store on
each new owner, so the retry usually **succeeds** unless the majority is sustaining high-frequency writes to the
same conversation; progress is made whenever the node wins the CAS. Real residual is owner-teardown-per-fence churn
+ no engine-side backoff.
- **Fix:** short negative-TTL cache on `owner_node` after a fence + surface `{:error, :owner_unreachable}`;
  exponential backoff on repeated fences.

### API-shape findings (all LOW, CONFIRMED — non-defects, ergonomics)
- **`broadcast_ephemeral` discards the recipient count it already computes** (`chat.ex:159`) — callers can't tell
  "delivered to a live peer" from "dropped to nobody." Cheap win: return `{:ok, {:ephemeral, count}}`. Caveat: the
  count is a `:syn`-membership snapshot, not confirmed delivery, so only the `0` case is dependable.
- **`kind: :ephemeral` on the session path still serializes through the owner GenServer** (`session.ex:161`) — the
  fast lane exists only on the `Chat` facade, so a wire client's ephemeral send can inherit a durable-append
  head-of-line stall. Add inbound handling that routes `kind: :ephemeral` straight to `Fanout.dispatch`.
- **No `read_before`/descending read; `history/3` has no `more?`** (`port.ex:57`) — koine (which owns server-side
  history) must hand-roll `latest_seq`-based newest-first paging. Add `read_before/3` + `history_before/3`;
  interim: point koine's endpoint at `history_page/3`.
- **Cursor/receipt store failures are telemetry blind spots** (`cursors.ex:41`, `receipts.ex:23`) — degraded store
  → fleet-wide redelivery storms or lost receipts with only a `Logger.warning`, no metric. The engine already
  emits 16 `[:chat,…]` events, several on error paths — this is an inconsistent omission. Add `[:chat,:cursor,:error]`
  / `[:chat,:receipt,:error]`.
- **Durable-before-ack advertised as kit-verified but untested** (`port_test.ex:8`) — the kit can't portably assert
  fsync durability; the reference in-memory adapter is non-durable and passes. Koine (Locus `everysec`) and vox
  (SQLite pragma) can silently downgrade at-least-once to "≤1s loss." Add a kit doc caveat + optional `durable?/0`
  probe or a `Chat.Config` boot assertion.

---

## 3. Gaps by client need

**koine (deep embedder, Locus/Postgres, multi-tenant plaintext):**
- No membership backstop in the core (B1) — koine's allow-all `authorize` is the whole reason B1 is exploitable.
  **Highest-priority gap.**
- No first-class tenant namespace (`fanout.ex:49`) — isolation rests entirely on koine hand-scoping every id in 6
  per-verb codec clauses; one forgotten `Tenancy.scope` on a future verb = cross-tenant bleed with no engine error.
  *(PLAUSIBLE, low — no current breach; hardening.)* Add an engine tenant field or a single body choke-point + a
  test asserting every inbound verb is scoped.
- Multi-node correctness gaps (B5, B9, B12) bite koine specifically when it ships clustering — all currently untested.
- No `read_before`/`more?` for its history-owning HTTP API.

**vox (deep embedder over an Ecto relay body; E2E chat + voice/video):**
- Non-conforming fence unverified by the kit (B13).
- Ephemeral lane gives no delivery signal (recipient-count discard) and no at-least-once/replay — vox carries all
  signaling reliability (pendingIce buffering, re-offer) in the SDK.
- `broadcast_ephemeral` uncapped size (B11) — vox's `ephemeral` branch takes client-sized envelopes.
- Cross-relay ephemeral isn't on a fast lane — federated call signaling pays durable-outbox latency (this is a vox
  `federation.ex` design choice, not an engine bug, but the engine offers no primitive to help).

**motus:** none — motus does not use chat_engine at all (talks to Locus directly in Go). No action, unless you
decide motus *should* adopt the shared engine/contract rather than re-implementing fan-out.

**all:**
- Cursor over-advance family (B2/B3/B4/B6) — the gap-free/exactly-once guarantee is defeatable via error-swallow,
  backpressure, client-supplied seq, or crash-in-window.
- Telemetry blind spots on the store-and-forward hinge.
- No graceful-stop primitive (B16).

---

## 4. Improvements (ranked by leverage)

1. **Introduce a "contiguous-delivered high-water" cursor invariant.** One change closes B2, B3, B4, and B6 — the
   entire silent-loss family. Advance the persisted cursor only across the contiguous delivered prefix; never from
   an ack, a client `:read` seq, or a lone live delivery that leapfrogs a gap.
2. **Add an opt-in `enforce_membership` core gate** (B1). The `member?/2` primitive already exists in every store —
   it just isn't called on the message path.
3. **Emit error-path telemetry everywhere the engine currently only logs** (catch-up failure, cursor/receipt/
   conversations_for store errors, offline shed). High operational leverage, trivial, consistent with conventions.
4. **Route `invalidate_size` through the owner** (B5) — reuse the exact `Router`/`owner_node` pattern every other
   owner op already uses.
5. **Cap and inspect the OfflineNotifier supervisor** (B7); shed-with-telemetry since the message is already durable.
6. **Guard Presence's port I/O + rebuild monitors from `:syn`** (B8) — removes a per-node single-point-of-failure.
7. **Make placement drain-aware** (B12) and **provide a graceful-stop helper** (B16) — turns rolling deploys from
   "stall + reconnect storm" into clean handoff.
8. **Apply `check_payload` on the ephemeral fast lane** (B11) and **return the recipient count** — two tiny wins.

---

## 5. Test gaps (highest-value first)

1. **Locus contract kit + the sole split-brain fence-race test are auto-excluded when the Locus binary is absent**
   (`test_helper.exs:6`) `[HIGH, CONFIRMED]`. Default `mix test` and CI (`.github/workflows/ci.yml` builds no Locus)
   **never run** the real adapter's durability/idempotency/gap-free/CP-fence proof — yet the README claims
   `mix test` covers it. Add a committed CI lane that builds/downloads Locus, runs `mix test --include locus`, and
   **fails if the `:locus` tag was excluded** rather than silently skipping. Gate releases on the fence-race test.
2. **`append/3` under concurrency is never tested** (B13) — N-task same-`expected_seq` race asserting exactly one
   `{:ok}` + N-1 `{:fenced,_}`, and that a losing write surfaces as `{:fenced,_}` not a generic error.
3. **Owner-unreachable degradation has zero coverage** (`router.ex:34`, `conversation.ex:88`) `[MEDIUM]`.
   Distributed test: sender on A, owner on B, `:peer.stop` mid-submit, assert an `:error` envelope within ~5s;
   plus a unit test forcing `call_owner`'s exit catch.
4. **Cross-node size-cache / receipt-policy transition untested** (B5).
5. **Sender-ack concurrent-crash cursor edge untested** (B6) — all concurrency tests use `Chat.inject` (no session/
   cursor); none drives two live sessions with a mid-flight crash.
6. **`broadcast_ephemeral` self-echo + payload-cap bypass untested** — the current test connects only a non-member watcher.
7. **Durable-before-ack survival untested** (kill-and-reopen probe).

---

## 6. Prioritized action list

**Bugs first (core / firewall-safe):**
1. **B1** — add `enforce_membership` core gate + fix koine's allow-all `authorize` and false comment. **[M]** (core + koine body)
2. **Cursor over-advance family (B2, B3, B4, B6)** — implement the contiguous high-water invariant; add
   `catch_up_failed` telemetry + stop-on-error. **[M]**
3. **Test gap #1** — CI lane that builds Locus and fails on silent `:locus` exclusion; fix the README overclaim. **[S]**
4. **B5** — route `invalidate_size` to the owner node. **[S]**
5. **B7** — cap OfflineNotifier supervisor, inspect `start_child`, shed-with-telemetry. **[S]**
6. **B8** — guard Presence port I/O + rebuild monitors from `:syn`. **[M]**
7. **B13** — add `append/3` concurrency case to the contract kit; then fix vox's fence in the **vox body**. **[M]** (kit=core, fix=body)
8. **B11** — `check_payload` on `broadcast_ephemeral`. **[S]**
9. **B10** — distinct authz contract for user-targeted actions. **[S]**
10. **B15** — make `ensure_local/1` total + wrap the local branch. **[S]**

**Then multi-node hardening (only exercised once koine/vox cluster):**
11. **B9** presence convergence, **B12** drain-aware placement, **B17** boot-enforce fence, **B18** fence backoff. **[M–L]**
12. Test gaps #3–#6. **[M]**

**Ergonomics / observability:**
13. Store-error telemetry everywhere (cursor/receipt/conversations_for). **[S]**
14. `read_before`/`history_before` + `more?`; recipient-count return; `kind: :ephemeral` wire fast-lane. **[S–M]**

**Belongs in a BODY, not the core (firewall):**
- **B16** graceful-stop orchestration — the engine ships only the drain primitive + a session-count query.
- **B13 fix** (advisory lock) — the vox adapter, not the engine; the engine's job is only the contract-kit concurrency test.
- Tenant namespacing *may* stay body-side, but if so add a single choke-point + a scoped-every-verb test.

**Effort legend:** S = <½ day, M = 1–3 days, L = multi-day (needs distributed test harness).
