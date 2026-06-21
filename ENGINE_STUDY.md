# chat_engine — Code Study & Evaluation

> **Date:** 2026-06-19
> **Scope:** Full read of `lib/` (1,586 LOC) and `test/`, plus build/CI/docs.
> **Method:** Multi-agent study — 5 subsystem readers, 9 dimension evaluators, adversarial
> per-finding verification — reconciled against a complete manual read of the source. **73 findings.**
> Companion document: [PLUGGABILITY_PLAN.md](PLUGGABILITY_PLAN.md).

---

## 1. Executive summary

`chat_engine` is a **real-time messaging core** in Elixir/BEAM with a deliberately small, opinionated
surface: a pure ports-and-adapters core (only runtime dep is `:syn`), a single-writer-per-conversation
ordering authority, rendezvous-hashing (HRW) owner placement, and O(online) cluster-wide fan-out via
`:syn`. Payloads are opaque binaries (E2E-friendly), and a build-enforced **firewall test** keeps
transport/web/DB dependencies out of the core.

**The design is senior-grade and worth preserving.** The gaps are concentrated in the *last mile to
production*:

1. **Security is unwired (critical).** The `Auth` port and `ConversationStore.member?/2` both exist but
   are **never called**. There is no authentication on connect and no authorization on any inbound verb —
   a connected client can read and write **any** conversation in the cluster.
2. **No partition strategy (critical).** A network split yields two conversation owners assigning `seq`
   independently into divergent logs. There is no fencing in the persistence contract.
3. **Several reliability claims are overstated vs. the code.** Automatic catch-up is silently capped at
   100 messages; "exactly-once" is really at-least-once + client dedup; there is no backpressure.
4. **Distributed failure modes are neither handled nor tested.** The hot send path blocks on an un-timed
   `:erpc.call`; the cross-node size cache is never invalidated; the distributed suite is excluded from CI.
5. **Operability is absent.** Zero telemetry, no structured logging, no health surface, no graceful drain.

None of these are architectural dead-ends — they are additive work against clean seams. The rest of this
document is the evidence and the plan.

---

## 2. Architecture overview

### Runtime topology
```
Chat.Application (root supervisor)
├── :syn.add_node_to_scopes([:users, :conv_subs])     # cluster-global process groups
├── Registry  Chat.ConversationRegistry               # node-local owner index
├── Chat.Presence (GenServer)                          # monitor-based online/last-seen
├── DynamicSupervisor  Chat.Conversation.Supervisor    # one owner per ACTIVE conversation
└── DynamicSupervisor  Chat.Session.Supervisor         # one process per connected device
```

### Message lifecycle (`:send`)
1. Client → `Chat.Session.handle_inbound/2` (`cast`) → `:send` clause builds a `Chat.Message`.
2. `Chat.Conversation.submit/3` routes to the **owner** via `Chat.Router.ensure_conversation/1`
   (local, or `:erpc.call` to the HRW node) and `GenServer.call`s it.
3. The owner **persists first** (`Persistence.append/2` — durable, monotonic, idempotent `seq`),
   *then* acks the sender. This is the at-least-once hinge.
4. The owner fans out to **online** subscribers via `Chat.Fanout.dispatch/3` (the `:conv_subs` `:syn`
   group), coalescing one `:erpc.cast` per remote node.
5. Each recipient session pushes the envelope through its `{transport_mod, ref}` and advances its
   per-device cursor.

### Cluster model
- **Owner placement** = `Enum.max_by(nodes, &:erlang.phash2({conversation_id, node}))` (HRW). Every node
  computes the same answer; on join/leave only ~1/N conversations move. No ring process, no extra dep.
- **Discovery** = `:syn` groups: `:users` (a user's sessions, cluster-wide) and `:conv_subs` (a
  conversation's online members, cluster-wide).
- **Node formation** (libcluster/k8s/`Node.connect`) is explicitly the *body's* job.

### Ports (the contract)
`Persistence` · `ConversationStore` · `CursorStore` · `ReceiptStore` · `PresenceStore` · `Auth` ·
`OfflineQueue`. `Chat.Ports` resolves the configured adapter module per port; bundled
`Chat.Adapters.InMemory.*` are the executable spec (and explicitly "tests/demos only").

---

## 3. Strengths (preserve these)

- **Enforced purity.** `Chat.FirewallTest` fails the build if a transport/web/DB dep enters the core.
  This is a real, testable architectural boundary, not a convention.
- **Single-writer ordering authority.** One `Chat.Conversation` GenServer per conversation is a clean,
  lock-free serialization point for `seq`. Persist-before-ack is correct.
- **HRW placement.** Coordination-free, rebalance-cheap, dependency-free owner location.
- **O(online) fan-out, cluster-wide.** Cost scales with online members and with the number of nodes that
  have online members (one `:erpc.cast` per node) — not with roster size. A 1M-member group with 50
  online costs ~50 sends.
- **Opaque-binary payloads.** The engine never inspects content → E2E encryption layers on for free.
- **Cursor-based offline catch-up.** Per-device cursor + durable log = no per-recipient queue; scales to
  huge groups. The *shape* is right (the cap is a bug, see REL-1).
- **Monitor-based presence with cluster-aware "online elsewhere?" check** before declaring a user offline.

---

## 4. Gaps & risks

Severity legend: 🔴 critical · 🟠 high · 🟡 medium · ⚪ low. "Verdict" notes whether the finding's
adversarial verifier completed (`confirmed`/`partial`) or was cut short by API rate-limiting
(`rate-limited`); rate-limited items were re-checked manually and are not refuted.

### 4.1 Security & abuse resistance — *the headline theme*

| ID | Sev | Finding | Where | Fix direction |
|---|---|---|---|---|
| SEC-1 | 🔴 | **Auth is a dead contract** — `authenticate`/`authorize` never invoked anywhere in `lib/`. The port is *required* (raises if unset) yet unused. | `auth/port.ex:15,19`; no callers | Call `authenticate` on connect (derive `user_id` from `{:ok, principal}`, refuse on error); call `authorize`/`member?` on every inbound side-effect. |
| SEC-2 | 🟠 | `:send` accepts **any** `conversation_id` with no membership/authorization check. | `session.ex:95-97` → `Conversation.submit` | Gate on `member?`/`authorize`; push `:error` on deny. |
| SEC-3 | 🟠 | `:sync` and `:read_state` let any session read full history / read-state of any conversation. | `session.ex:128-138, 159-171` | Same authz gate. |
| SEC-4 | 🟡 | `:presence_query` leaks online/last-seen of arbitrary users with no relationship check. | `session.ex:174-183` | Gate behind a shared-conversation or contact check. |
| SEC-5 | 🟡 | No payload size bound — unbounded `binary()` enables memory-exhaustion DoS. | `message.ex`, `session.ex:95` | Cap payload bytes in core `submit`/`inject` + at the edge. |
| SEC-6 | 🟡 | No rate limiting / flood control on any inbound verb. | `session.ex` all `:inbound` | Token-bucket per session/principal (primarily edge, with a core overload signal). |
| SEC-7 | 🟡 | No per-tenant isolation — `conversation_id`/`user_id` are one flat global namespace; two tenants sharing `"general"` collide. | `types.ex`, `:syn` keys | Cheap path: Auth adapter returns namespaced principal (`"acme:alice"`), edge rewrites every `conversation_id` → `"acme:<id>"`. |
| SEC-8 | 🟡 | `Fanout.subscribe` re-checks nothing; subscription trust rests on a one-time `init` read of `conversations_for/1`. | `session.ex:54-61`, `fanout.ex:29` | Re-validate membership on (re)subscribe, or revoke on membership change (the leave path exists but is one-directional). |

### 4.2 Distributed systems & failure modes

| ID | Sev | Finding | Where | Fix direction |
|---|---|---|---|---|
| DF-1 | 🔴 | **No split-brain fencing** — a partition gives two owners (each side's `Node.list()` differs) assigning `seq` independently into divergent logs. | `cluster.ex:23-25`, `router.ex:15-23` | Decide CP vs AP (see §6). Minimum: persistence `append` does compare-and-set on `(conv, seq)` so the losing writer is rejected. |
| DF-2 | 🟠 | Hot send path blocks the session on an **un-timed, un-rescued** `:erpc.call` to the owner node. | `router.ex:21` | Add timeout + `try/rescue`; degrade to `:error`; consider async submit. |
| DF-3 | 🟠 | Owner is **re-derived, not migrated**, on node join/leave — a new owner starts on the new node, the old one is orphaned with its cached state; locality lost. | `cluster.ex`, `router.ex:28-42` | Document the rehome semantics; rebuild owner state from ports on start; consider handoff/draining. |
| DF-4 | 🟡 | `member_count` size-cache invalidation never reaches a **remote** owner (see CC-1). | `chat.ex:134-139` | Invalidate via the owner pid (route through `Router`), not a node-local `Registry.lookup`. |
| DF-5 | 🟡 | Cross-node fan-out is fire-and-forget — silent loss to an unreachable node, no timeout/retry/metric. | `fanout.ex:57` | Acceptable for ephemeral, but emit telemetry on failed casts; offline members already recover via cursor. |
| DF-7 | 🟡 | Owner crash loses all in-memory state; `seq` authority then depends on the persistence adapter surviving independently (true, but undocumented). | `conversation.ex:63` | Document; ensure `current_size` rebuilds (it does, lazily) and that restart can't double-assign `seq` (it can't — persistence is the authority). |
| DF-6 | ⚪ | HRW tie-break relies on `Enum.max_by` ordering + 32-bit `phash2`; no explicit deterministic tie-break. | `cluster.ex:24` | Tie-break on `{hash, node}` so all nodes agree under collisions. |
| DF-8 | 🟡 | Distributed failure modes are entirely **unexercised in CI** (see TST-1). | `.github/workflows/ci.yml` | Run `--include distributed`; add partition tests. |

### 4.3 Correctness & concurrency

| ID | Sev | Finding | Where | Fix direction |
|---|---|---|---|---|
| CC-1 | 🟠 | Size-cache invalidation is **node-local** (`Registry.lookup` on the calling node); if the owner is remote it never receives `:invalidate_size`, leaving receipt policy permanently stale. | `chat.ex:134-139`, `conversation.ex:130` | Route invalidation to the owner pid via `Router`. |
| CC-4 | 🟠 | **Auto catch-up capped at 100, no pagination loop** — a device missing >100 messages silently gets only the first 100 (cursor advances to #100; the rest never arrive live). Contradicts the "delivers everything missed" moduledoc. | `session.ex:209-226` → `chat.ex:91` (`limit \\ 100`) | Loop `read_after` until drained (or stream pages with backpressure). |
| CC-8 | 🟡 | Core **crashes on legal `{:error,_}`** port returns — `{:ok, x} = ...` hard matches on `append`/`conversations_for`/`read_watermarks`. A transient store error kills the owner/session. | `conversation.ex:71`, `session.ex:59`, `receipts.ex:29` | Match `{:error,_}` and degrade (push `:error`, retry, or `{:reply, {:error,_}}`). |
| CC-2 | 🟡 | `member_count` error/garbage falls back to `2` (assume 1:1, receipts ON) → receipt storms in large groups on transient store errors. | `conversation.ex:134-141`, `session.ex:233-238` | Fail closed (treat unknown as "large", receipts off) or propagate error. |
| CC-3 | ⚪ | Size-cache read in `:submit` races a concurrent `:invalidate_size` cast even same-node (cast vs call ordering). | `conversation.ex:66, 130` | Minor; accept or make invalidation a `call`. |
| CC-5 | ⚪ | Explicit `:sync` is also capped at 100, does **not** advance the cursor, and returns no continuation token — divergent semantics from auto catch-up. | `session.ex:128-138` | Unify with catch-up; add a continuation cursor. |
| CC-6 | ⚪ | Fan-out delivery order isn't guaranteed to match `seq`; recipients must reorder, but nothing sorts and the contract leans on undocumented client behavior. | `fanout.ex:46-62` | Document the client reorder-by-`seq` requirement (it's a real requirement). |
| CC-7 | ⚪ | `:read` with `nil` seq records nothing yet still relays a receipt envelope carrying `seq: nil`. | `session.ex:112-125` | Drop the relay when `seq` is nil. |
| CC-9 | ⚪ | Sender cursor advances to its own `seq` while an interleaved lower-`seq` message may still be in flight; correctness rests on monotonic-`max` cursor (which holds). | `session.ex:98`, `cursor_store/port.ex:20` | No fix needed; note the dependency. |

### 4.4 Reliability & delivery semantics

| ID | Sev | Finding | Where | Fix direction |
|---|---|---|---|---|
| REL-1 | 🟠 | (= CC-4) Catch-up delivers at most 100 missed messages with no pagination. | `session.ex:211` | Paginate to completion. |
| REL-2 | 🟠 | **No backpressure** — inbound and outbound are unbounded `GenServer.cast` over a *synchronous* transport `push`; a slow client grows the mailbox without bound. | `session.ex:34, 37, 228` | Bounded mailbox / `message_queue_len` guard → `{:error, :overloaded}`; demand-driven delivery. |
| REL-3 | 🟡 | "Exactly-once" is overstated — the reconnect window is at-least-once and relies on client dedup by message id. | `cursors.ex` moduledoc, `session.ex` moduledoc | Reword docs to "exactly-once in steady state; at-least-once across reconnect (client dedups by id)". |
| REL-5 | 🟡 | OfflineQueue / push-notification hook is fully unwired — no way to wake a disconnected device. | `ports.ex:40` (optional, nil); no callers | Invoke `offline_queue` when a target user has no online sessions at fan-out time. |
| REL-4 | ⚪ | Subscribe-before-catch-up race can double-deliver a message that arrives during connect (subscribe in `init`, catch-up in `handle_continue`). | `session.ex:54-78` | Catch up first, then subscribe; or dedup the boundary by `seq`. |
| REL-6 | ⚪ | (= CC-5) `:sync` capped at 100, no cursor advance, no continuation. | `session.ex:128` | As CC-5. |
| REL-7 | ⚪ | Cursor advance and watermark writes ignore store errors → delivery progress can be silently lost. | `cursors.ex:25-29`, `receipts.ex:17-20` | Surface/log errors; consider retry. |
| REL-8 | ⚪ | No delivered-receipt for store-and-forward (offline) messages; catch-up sets `receipts: false`. | `session.ex:214-225` | Optionally emit delivered receipts on catch-up for 1:1. |

### 4.5 API & contract design

| ID | Sev | Finding | Where | Fix direction |
|---|---|---|---|---|
| API-1 | 🟡 | (= CC-8) Core crashes on legal `{:error,_}` it claims to handle. | see CC-8 | Handle the error half of every port contract. |
| API-2 | 🟡 | (= CC-2) `member_count` error → fallback `2` silently flips receipt policy. | see CC-2 | Fail closed. |
| API-3 | 🟡 | `history`/`read_after` has no continuation cursor; callers can't paginate deterministically. | `chat.ex:91`, `persistence/port.ex:26` | Add `{:ok, msgs, next_after_seq}` or document last-seq paging. |
| API-7 | ⚪ | `member?/2` returns a bare `boolean()` while every sibling callback returns `{:ok,_}|{:error,_}` — inconsistent error contract. | `conversation_store/port.ex:12` | Normalize to `{:ok, boolean}` \| `{:error,_}`. |
| API-8 | ⚪ | `Transport.push/2` is typed `:: :ok` with no failure/backpressure shape. | `transport.ex:11` | Allow `{:error,_}` so the engine can react to a dead/slow socket. |
| API-4 | ⚪ | No delete / edit / redact / tombstone in the message-log contract. | `persistence/port.ex` | Add a tombstone op if product needs deletion (GDPR). |
| API-5 | ⚪ | `Envelope` typespec is stale and structurally untyped — `@type type` lists 8 atoms but Session handles 13; only `:type` field is specified. | `envelope.ex:16, 45` vs `session.ex` | Complete the union + type all fields; add a wire version. (Blocks the codec — see PLUGGABILITY_PLAN §3.) |
| API-6 | ⚪ | Inconsistent port indirection: member listing via `Chat.ConversationStore` helper, `member_count` via `Ports` directly. | `chat.ex:50, 54` | Pick one indirection. |

### 4.6 Observability & operability

| ID | Sev | Finding | Where | Fix |
|---|---|---|---|---|
| OBS-1 | 🟡 | Zero `:telemetry` instrumentation across the whole engine. | all | Emit events on submit/fanout/catch-up/presence with measurements. |
| OBS-3 | 🟡 | No boot-time config validation; misconfiguration fails lazily on first message. | `ports.ex:42` | Validate required adapters at app start. |
| OBS-4 | 🟡 | No graceful shutdown / connection draining for sessions or owners. | `application.ex`, `session.ex:21` | Drain hook; let clients reconnect+resync (engine supports it). |
| OBS-7 | 🟠 | Silent catch-up truncation (CC-4) is operationally invisible — no metric, log, or flag. | `session.ex:209` | Telemetry + warn when a page fills. |
| OBS-8 | 🟡 | Cross-node failures and adapter errors crash silently with no operator signal. | `fanout.ex`, `router.ex` | Log + telemetry on `:erpc` failures. |
| OBS-2 | ⚪ | No structured logging despite `:logger` declared. | `mix.exs:31` | Add `Logger` at decision points. |
| OBS-5 | ⚪ | No health / status / queue-depth introspection surface. | — | `Chat.ready?/0` + counts. |
| OBS-6 | ⚪ | Conversation owner uses default `:permanent` restart; cached size resets on crash but `seq` stays consistent (via persistence). | `conversation.ex:20` | Document; consider `:transient`. |
| OBS-9 | ⚪ | No `runtime.exs` / operability guidance for the body (node naming, clustering, runtime adapter wiring). | `config/` | Add guidance (lives mostly in the body). |

### 4.7 Performance & scalability
*(The in-memory adapters are explicitly "tests/demos only" — these flag the contract/core, not the reference adapter being in-memory.)*

| ID | Sev | Finding | Where |
|---|---|---|---|
| PERF-1 | 🟡 | `read_after` scans + sorts the entire conversation log per catch-up — O(n log n) per read. | `adapters/in_memory/persistence.ex:94-105` |
| PERF-2 | 🟡 | Presence broadcast is an N+1 over a user's conversations on every online/offline toggle (`member_count` per conv). | `presence.ex:89-105` |
| PERF-3 | 🟡 | Reference Persistence funnels ALL conversations through one GenServer, hiding the per-conversation parallelism the design implies. | `adapters/in_memory/persistence.ex` |
| PERF-4 | ⚪ | `Receipts.aggregate` reads + scans all watermarks per `:read_state` pull (O(members)). | `receipts.ex:28-32` |
| PERF-5 | ⚪ | Fan-out materializes the full online-member list + `group_by` on the owner per dispatch. | `fanout.ex:46-52` |
| PERF-6 | ⚪ | No ETS on hot read paths; cursor/receipt/conv-store reads serialize through single-process Agents. | `adapters/in_memory/*` |

### 4.8 Testing & quality

| ID | Sev | Finding |
|---|---|---|
| TST-1 | 🟠 | Distributed/cluster suite is **excluded from CI** (`mix test` without `--include distributed`) — all multi-node guarantees untested in automation. |
| TST-2 | 🟠 | Catch-up >100 pagination gap (CC-4) is untested, masked by 1–2-message fixtures. |
| TST-3 | 🟠 | Auth port required but has **zero** test coverage — the "engine enforces your verdict" claim is unverified (because it's unwired). |
| TST-4 | 🟡 | Property test appends sequentially — no concurrent-sender ordering test. |
| TST-5 | 🟡 | No test exercises the `{:error,_}` half of port contracts (the very paths that crash — CC-8). |
| TST-6 | 🟡 | No partition/split-brain, backpressure, or `:erpc`-timeout tests. |
| TST-7 | 🟡 | No dialyzer / credo / coverage in CI despite specs being present. |
| TST-8 | 🟡 | Negative assertions + distributed suite rely on timing windows (`Process.sleep`, `refute_receive` defaults). |
| TST-9 | ⚪ | In-memory `ConversationStore` doesn't model the streaming/paging contract, so paging is never exercised. |
| TST-10 | ⚪ | Behavioral suites share singleton adapters + global `:syn`, `async: false`; isolation depends on reset/drain discipline. |

### 4.9 Documentation & adoptability

| ID | Sev | Finding |
|---|---|---|
| DOC-1 | 🟠 | Auth documented as wired/enforced ("the engine enforces *your* verdict") but is a dead contract (SEC-1). |
| DOC-2 | 🟠 | Session moduledoc promises "everything missed" + "exactly-once" but catch-up caps at 100 (CC-4) and reconnect is at-least-once (REL-3). |
| DOC-5 | 🟠 | No adapter-authoring guide, no port **contract test-kit** for implementers, no real (non-in-memory) adapter example. |
| DOC-3 | 🟡 | 30+ moduledoc references to a missing `CHAT_ENGINE_PLAN.md` and `M0`–`M5` milestones not in this repo. |
| DOC-4 | 🟡 | `Chat.Ports` moduledoc misstates which ports are required vs optional. |
| DOC-6 | 🟡 | `@source_url` points at a placeholder org (`intenttext/chat-engine`); README dep snippet is path-only. |
| DOC-7 | ⚪ | `FirewallTest` guidance + README dep path reference the stale pre-flatten umbrella layout. |

---

## 5. Improvement roadmap

Phased by "what must be true before someone trusts it." Effort: S (<½ day) · M (1–2 days) · L (3–5 days).

### P0 — Correctness & security must-fix (before any real body trusts it)
| # | Change | Files | Effort |
|---|---|---|---|
| 1 | Wire `authenticate` on connect; refuse on `{:error,_}` | `session.ex:54`, `ports.ex` | M |
| 2 | Gate `:send/:sync/:read/:read_state/:typing/:presence_query` + subscribe on `member?`/`authorize`; push `:error`, don't crash | `session.ex` | M |
| 3 | Catch-up pagination loop (drain `read_after`) | `session.ex:209-226` | S |
| 4 | Backpressure: mailbox/`message_queue_len` guard + payload size cap | `session.ex`, `conversation.ex` | M |
| 5 | Stop crashing on legal `{:error,_}` (CC-8/API-1); fail-closed `member_count` (CC-2) | `conversation.ex`, `session.ex`, `receipts.ex` | M |
| 6 | Fix cross-node size-cache invalidation (route via owner pid) | `chat.ex:134`, `conversation.ex:130` | S |
| 7 | Tests for #1–#6 + a >100-message catch-up test | `test/` | M |

### P1 — Production-readiness
| # | Change | Effort |
|---|---|---|
| 8 | `:telemetry` events + structured logging at decision points (OBS-1/2/8) | M |
| 9 | `:erpc.call` timeout + rescue on the send path; degrade gracefully (DF-2) | S |
| 10 | **Partition strategy decision** + persistence CAS fencing (DF-1) — see §6 | L |
| 11 | CI: `--include distributed`, add dialyzer + credo + coverage (TST-1/7) | M |
| 12 | Graceful drain + `Chat.ready?/0` health surface (OBS-4/5) | M |
| 13 | Boot-time config validation (OBS-3) | S |
| 14 | Port **contract test-kit** (`use Chat.Persistence.PortTest`) implementers run against their adapter (DOC-5) | M |

### P2 — Scale & ergonomics
| # | Change | Effort |
|---|---|---|
| 15 | Ephemeral / no-persist channel mode (branch on dead `Message.kind`) — see PLUGGABILITY_PLAN §6 | L |
| 16 | Pagination cursor in `history`/`read_after` (API-3) | M |
| 17 | Wire `OfflineQueue` push when target has no online sessions (REL-5) | M |
| 18 | ETS hot paths; fix presence N+1 (PERF-2/6) — in real adapters | M |
| 19 | Concurrent-sender ordering property test (TST-4) | M |

### P3 — Polish, docs, tooling
| # | Change | Effort |
|---|---|---|
| 20 | Reconcile docs with code (DOC-1/2/3/4); remove `CHAT_ENGINE_PLAN.md` references | S |
| 21 | Fix `@source_url`, README dep snippet, firewall moduledoc (DOC-6/7) | S |
| 22 | Complete `Envelope` typespec + wire version (API-5) | S |
| 23 | Hex publish prep | S |

---

## 6. Open design questions (genuine judgment calls)

1. **Partition strategy (DF-1) — the biggest call.** CP fencing (persistence does compare-and-set on
   `(conv, seq)`; the losing owner is rejected and steps down) vs AP merge (accept divergence, reconcile
   on heal). Currently neither — the system is silently AP-with-no-merge. *Recommendation:* CP fencing in
   the persistence contract; it preserves the single-`seq`-authority invariant the whole design rests on.
2. **Where auth lives.** Wiring it only at a gateway leaves the in-VM `inject`/admin path and any second
   body unguarded. *Recommendation:* enforce in the **core** session path; the edge supplies the verdict.
3. **Where backpressure belongs.** Engine (bounded mailbox / overload reply) vs body (rate limiter).
   *Recommendation:* both — a core overload signal plus an edge token-bucket.
4. **Tenancy.** Cheap path: namespaced principals + edge `conversation_id` rewrite. Invasive path:
   first-class `tenant_id` in `Chat.Types` and all keys. *Recommendation:* cheap path for v1.
5. **Catch-up delivery model.** Eager drain (simple, can flood a reconnecting client) vs demand-driven
   paging (needs client cooperation). Ties into P0 #3 and P0 #4.

---

## 7. Methodology note

This study combined a multi-agent workflow (5 subsystem mappers → 9 dimension evaluators → adversarial
per-finding verification) with a complete manual read of `lib/` and `test/`. All 🔴/🟠 findings were
confirmed by hand against the source. During the verification phase, Anthropic API rate-limiting
truncated several verifier sub-agents and the workflow's own auto-synthesis; those findings carry a
`rate-limited` verdict in the raw data and were re-checked manually for this document — none were refuted.
The full machine-readable finding set (with per-finding evidence/impact/recommendation and verdicts)
lives in the workflow output and can be regenerated.
