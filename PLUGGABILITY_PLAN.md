# chat_engine — Pluggability & Packaging Plan

> **Question:** Can we package this engine so it plugs into **any** system — a Node.js chat app, a game's
> chat, a Go/Python/C# backend, browsers?
> **Date:** 2026-06-19. Companion: [ENGINE_STUDY.md](ENGINE_STUDY.md).
> **Method:** Design panel — 4 candidate architectures designed, cross-scored, and a code-level blocker
> pass — synthesized against the real source and the sibling repos (Pulsar, Locus).

---

## 1. Verdict

**Yes — with caveats, and only one way: run the engine as a clustered BEAM *service* behind a
language-agnostic protocol, fronted by a separate "body" app. You cannot embed BEAM inside Node/Go/Unity.**

The key insight: the engine is **already plug-into-any-BEAM-app today** — `Chat.Transport` +
`Chat.Session.connect/1` + `Chat.Session.handle_inbound/2` are a clean, transport-agnostic seam, and
Pulsar (sibling repo) already drives it in production-shape code. Pulsar's transport is *byte-identical*
to the gateway transport every candidate here proposes:

```elixir
def push(handler_pid, %Chat.Envelope{} = env), do: (send(handler_pid, {:push, env}); :ok)
def close(handler_pid, reason),                do: (send(handler_pid, {:close, reason}); :ok)
```

But "plug into any **runtime**" is a different problem. A Node process cannot host a BEAM scheduler; a
Unity client cannot link an OTP release. The only way a non-BEAM runtime talks to this engine is **over a
wire** (WebSocket / gRPC / unix socket) to a **running service**. So the deliverable is not "a library you
import" — it's **a deployable engine service + a wire protocol + per-language client SDKs.**

### Three hard blockers between today's code and a network-exposed service
1. **No codec exists.** Zero `encode/decode/Jason/protobuf` in `lib/`. The only API is the in-VM struct
   (`handle_inbound(pid, %Envelope{})`). The `Envelope` moduledoc *promises* "JSON in M1; binary prefix +
   Protobuf later" but nothing implements it. Nothing non-BEAM can speak to it yet.
2. **No auth/authz is wired** (study SEC-1/2/3). `Session` calls `Conversation.submit/3` with no
   membership check; `:sync`/`:read`/`:read_state`/`:presence_query` act on any attacker-supplied id. A
   connected client can read and write **any conversation in the cluster.** Fatal for a networked
   multi-tenant service.
3. **No backpressure** (study REL-2). `handle_inbound`/`deliver` are unbounded `GenServer.cast`; payload
   is unbounded `binary()`. An untrusted client can OOM a session/node.

Until those three land, the engine is safe **only** behind a trusted in-VM body. After they land, it is a
genuine drop-in chat brain for any runtime.

---

## 2. Recommended architecture

**Winner: `ws-json-gateway` for P0/P1, evolving toward the full `product-platform` as the GA target.** A
phased combination, justified by the panel scores:

| Approach | Score | Wins on | Loses on |
|---|---|---|---|
| **ws-json-gateway** | **31** | op simplicity (5), fit-to-architecture (5), time-to-first-integration (5); browser/Node/Unity-WS native; curl-able; single image; **already de-risked by Pulsar** | binary-frame latency |
| product-platform | 29 | language-agnosticism (5), security/multitenancy (5 — *only* approach that designs auth+tenancy in), evolvability (5) | op simplicity (1), time-to-first (1); XL scope |
| grpc-protobuf-polyglot | 28 | cleanest typed-polyglot story | **loses the browser** (no native gRPC); not pre-proven here |
| sidecar-ipc | 27 | latency (5, unix-socket µs), process isolation for games | time-to-first (2); single-binary BEAM packaging **unproven** (Locus precedent is a *Rust* crate, not Burrito) |

**So: ship the WS/JSON gateway first; freeze a Protobuf-derived schema for the contract; add gRPC/SDKs and
tenancy hardening as the platform matures.**

### Topology (the FirewallTest boundary stays intact)

`Chat.FirewallTest` forbids `:bandit`/`:jason`/`:plug`/`:ecto`/… in the **core** repo. Therefore the
codec, the WebSocket listener, the auth adapter, and Docker/Helm **must** live in a separate `chat_gateway`
body that depends on `chat_engine` — exactly what Pulsar already is. This is not a style preference;
adding a transport dep to the engine repo fails the build.

```
  ┌────────────┐   wss + JSON     ┌──────────────────────────────┐
  │ Node app   │─────────────────▶│  GATEWAY BODY (separate repo) │
  │ (browser/  │   {"type":...}   │  ── chat_gateway ───────────  │
  │  ws pkg)   │◀─────────────────│  • Bandit / WebSock listener  │
  └────────────┘                  │  • Codec  JSON ↔ Envelope     │
                                  │  • Transport impl (push/2)    │
  ┌────────────┐   wss + JSON     │  • Auth adapter (JWT→user_id) │
  │ Game client│─────────────────▶│  • Limits (rate/size/buckets) │
  │ (Unity/C#  │   chat channel   │  • /admin HTTP control plane  │
  │  WS module)│◀─────────────────│  └──────────┬────────────────┘│
  └────────────┘                  └─────────────│─────────────────┘
        ▲                                       │ in-VM Elixir calls
        │ UDP/RUDP (game's own netcode,         │ (same node OR
        │  NOT through here)                     │  clustered via :syn)
        ▼                                        ▼
  ┌────────────┐                  ┌──────────────────────────────┐
  │ game UDP   │                  │  ENGINE CORE (this repo)      │
  │ server     │                  │  Chat.Session (1/device)      │
  └────────────┘                  │  Chat.Conversation (HRW owner)│
                                  │  Chat.Fanout (:syn O(online)) │
                                  │  ── 7 PORTS ──                │
                                  │  Persistence│ConvStore│Cursor │
                                  │  Receipt│Presence│Auth│Offline│
                                  └──────────────┬───────────────┘
                                          ┌──────┴───────┐
                                          │ Postgres/etc │  (or in-memory)
                                          └──────────────┘
```

- **Engine core** (this repo, firewall intact): `Chat.Session`, `Chat.Conversation` (single-writer `seq`
  owner placed by HRW), `Chat.Fanout` (`:syn`, O(online) cluster-wide), the 7 ports. The only mandatory
  in-repo changes are pure-stdlib behaviour calls: wire Auth/`member?` into Session inbound, add
  backpressure, complete the Envelope typespec, add the ephemeral path. None breach the firewall.
- **Gateway body** (`chat_gateway`): Bandit listener, the codec (the one place that knows bytes), the
  `Chat.Transport` impl, the JWT auth adapter, the rate-limiter, the `/admin` control plane, and the
  concrete DB port adapters. This is what ships as Docker/release.
- **Wire:** JSON over WebSocket in P0 (universal, curl-able), with a frozen schema derived from a
  `chat.proto` so the binary path is a drop-in P1 upgrade negotiated by a 1-byte version/codec prefix.
- **SDKs** (own repos, generated from the schema): Node first, then Unity/C#, Go, Python.

Clustering is untouched: gateway pods form one BEAM cluster via libcluster (the body's job per
`Chat.Cluster`); a session on pod A receives fan-out for a conversation owned on pod B transparently via
`:syn`. Gateways are stateless and horizontally scalable.

---

## 3. The wire contract

`Chat.Envelope` is the normalized struct both directions; `:payload` stays an **opaque binary**
(E2E-friendly — the engine never inspects it). The codec maps `type` strings to engine atoms via a
**hardcoded allow-list** (never `String.to_atom` on untrusted input — it exhausts the atom table).

> ⚠️ The struct is currently almost untyped (`@type t :: %__MODULE__{type: type()}` — only `:type`
> specified, 13 other fields unspecified), and the `type` union lists **8** atoms while `Session` handles
> **13** (`:typing`, `:read_state`, `:presence_query`, `:presence`, `:system`). **Fix this first** — it
> is the de-facto spec, and it lies. (study API-5)

### Client → engine (inbound)

| `type` | JSON frame (P0) | maps to |
|---|---|---|
| `auth`* | `{"type":"auth","token":"<jwt>","device_id":"d1"}` | gateway: `Auth.authenticate` → `Session.connect` |
| `send` | `{"type":"send","conversation_id":"team","id":"<uuid>","payload":"<base64>"}` | `Conversation.submit` (persist + seq + fanout) |
| `read` | `{"type":"read","conversation_id":"team","seq":42}` | `Cursors.advance` + `Receipts.record_read` |
| `sync` | `{"type":"sync","conversation_id":"team","seq":0}` | `Chat.history` → `sync_page` |
| `typing` | `{"type":"typing","conversation_id":"team","status":"start"}` | fan-out (small convs only) |
| `read_state` | `{"type":"read_state","conversation_id":"team","seq":42}` | `Receipts.aggregate` |
| `presence_query` | `{"type":"presence_query","user_id":"alice"}` | `Chat.presence_of` |

\* `auth` is a gateway-only frame (or WS subprotocol / `?token=`), not an engine `Envelope` type.

### Engine → client (outbound)

| `type` | JSON frame (P0) |
|---|---|
| `message` | `{"type":"message","conversation_id","id","sender_id","seq","payload":"<base64>","receipts":false}` |
| `ack` | `{"type":"ack","conversation_id","id","seq","status":"server_received"}` |
| `receipt` | `{"type":"receipt","conversation_id","seq","sender_id","status":"delivered"|"read"}` |
| `sync_page` | `{"type":"sync_page","conversation_id","messages":[{"id","seq","sender_id","payload","ts"}]}` |
| `read_state` | `{"type":"read_state","conversation_id","seq","count","readers":[...]}` |
| `presence` | `{"type":"presence","user_id","status":"online"|"offline","ts"}` |
| `typing` | `{"type":"typing","conversation_id","sender_id","status"}` |
| `system` | `{"type":"system","conversation_id","status":"join"|"leave",...}` |
| `error` | `{"type":"error","reason":"forbidden"|"too_large"|"rate_limited"|...}` |

**`payload` encoding rule (freeze once):** the JSON codec base64(url)-encodes `:payload`; protobuf uses
`bytes payload = N`. Without this single rule, SDKs disagree on the most important field.

**Protobuf path (P1):** one `chat.proto` with `ClientFrame = oneof {ConnectInit, Send, Read, Sync, Typing,
ReadState, PresenceQuery}` and `ServerFrame = oneof {Message, Ack, Receipt, SyncPage, System, Presence,
Typing, ReadState, Error}`, `payload` as `bytes`. A 1-byte prefix (`'j'` JSON / `'p'` protobuf) negotiates
per connection — the binary path becomes a parallel codec, not a breaking change. Buf (lint +
breaking-change detection) makes the wire a governed artifact (the engine has none today).

### Server-to-server control API

Not on the socket — it's the in-VM control plane (`lib/chat.ex`), exposed by the body as authenticated
HTTP/JSON (P0) and gRPC (P1):

| RPC | maps to |
|---|---|
| `POST /admin/conversations` | `Chat.create_conversation/2` |
| `POST /admin/members` / `DELETE` | `Chat.add_member/2` / `remove_member/2` |
| `GET /admin/members?conv=` | `Chat.members/1`, `member_count/1` |
| `POST /admin/inject` | `Chat.inject/2` (publish with no socket — built for exactly this) |
| `GET /admin/history` | `Chat.history/3`, `latest_seq/1` |
| `GET /admin/presence` | `Chat.online?/1`, `presence_of/1`, `read_state/2` |

The Node/game backend (which owns identity, rooms, billing) uses this to provision rooms and inject system
messages ("player X joined", kill feed). `inject` is in-VM and **intentionally trusted** — keep the authz
check in the *Session inbound path*, not in `Chat.Conversation`, so server-side injection stays
unauthenticated by design (but secure the `/admin` endpoint itself).

---

## 4. CORE changes vs new BODY/SDK work

The discipline: **anything touching bytes, sockets, JWTs, Docker, or k8s lives in the BODY** (firewall).
The core only changes via stdlib-only behaviour calls to close the security/contract/game gaps.

| Item | Where | Effort | First? |
|---|---|---|---|
| Wire `Auth.authenticate` on connect (derive `user_id`, refuse on error) | **CORE** | M | ✅ |
| Gate `:send/:sync/:read/:read_state/:typing/:presence_query` + `subscribe` on `member?`/`authorize`; push `:error`, don't crash | **CORE** | M | ✅ |
| Backpressure: mailbox/`message_queue_len` guard → `{:error, :overloaded}`; payload size guard in `submit`/`inject` | **CORE** | S–M | ✅ |
| Complete `Envelope.@type type` (+5 missing) + type all 13 fields + name the `messages` map shape | **CORE** | S–M | ✅ |
| Ephemeral/no-persist channel mode: branch on `Message.kind` (field exists at `message.ex:23` but is **dead** — never read), skip `append`, fan out directly; skip cursor advance | **CORE** | L | games |
| Direct-broadcast path for ephemeral (skip the single-writer owner `GenServer.call` hop) | **CORE** | M | game latency |
| Presence flap-damping + per-sender typing rate gate (config) | **CORE** | M | game scale |
| `Chat.ready?/0` health helper (syn joined + adapters loaded) | **CORE** | S | k8s |
| Fix `@source_url` placeholder, `@version`, firewall moduledoc path; `hex.publish` dry-run | **CORE** | S | publish |
| — | | | |
| `WsGateway.Codec` (JSON↔Envelope, atom allow-list, base64, size-cap) | **BODY** | L | ✅ |
| `WsGateway.Transport` (`push/2` = send `{:push, env}`; `close/2`) — copy Pulsar's | **BODY** | S | ✅ |
| `WsGateway.Web.Socket` (WebSock: auth→connect→decode→authz→handle_inbound→encode→push) | **BODY** | M | ✅ |
| `WsGateway.Limits` (token-bucket rate, max-frame, in-flight bound) | **BODY** | M | ✅ |
| JWT/HMAC `Chat.Auth.Port` adapter (per-tenant) | **BODY** | M | ✅ |
| `/admin/*` control plane + `/healthz` `/readyz` | **BODY** | M | P1 |
| Tenant-scoped DB adapters for the 7 ports (Ecto/Postgres) | **BODY** | L | P3 |
| `config/runtime.exs` (PORT/bind/secret/adapter/cluster from env; mirror `LOCUS_BIND=0.0.0.0`) | **BODY** | M | P0 |
| `mix release` config + multi-stage Dockerfile + tag-triggered GHCR release CI | **BODY/infra** | L | P0 |
| Helm chart (gateway Deployment+HPA, engine StatefulSet, headless svc for libcluster) | **infra** | L | P3 |
| `chat.proto` + Buf module + codegen CI | **shared-schema** | L | P1 |
| Node SDK (connect/send/on/reconnect+resync) | **SDK** | L | P1 |
| Unity/C# (main-thread queue), Go, Python SDKs | **SDK** | XL | P2/P3 |

---

## 5. Phased plan

### P0 — Make it safely network-exposable (the MVP a Node team can run)
| Change | Where | Effort | Deps |
|---|---|---|---|
| Auth on connect (call `authenticate`) | CORE | M | — |
| Authz/`member?` on all inbound side-effects | CORE | M | — |
| Backpressure + payload size cap | CORE | S–M | — |
| Complete Envelope typespec (the spec the codec mirrors) | CORE | S–M | — |
| `WsGateway` app + Transport + Socket + JSON Codec + Limits + JWT adapter | BODY | L | typespec |
| `/healthz`, `runtime.exs`, `mix release`, Dockerfile, GHCR CI | BODY/infra | L | gateway |
| Freeze the JSON frame schema (public contract) | shared-schema | S | typespec |

**Exit:** `docker run`, browser/Node `new WebSocket(...)`, authenticated, membership-enforced,
rate-limited, single image. This is the de-risked Pulsar pattern generalized.

### P1 — Polyglot
| Change | Where | Effort | Deps |
|---|---|---|---|
| `chat.proto` + Buf + codegen | shared-schema | L | P0 schema |
| Protobuf codec + 1-byte version negotiation | BODY | M | proto |
| `/admin` HTTP control plane (+ optional gRPC) | BODY | M | — |
| Node SDK (typed, reconnect+resync, ack Promises) | SDK | L | schema |

### P2 — Game-fit
| Change | Where | Effort | Deps |
|---|---|---|---|
| Ephemeral/no-persist channel mode (`kind` branch) | CORE | L | — |
| Direct-broadcast path (skip owner call) for ephemeral | CORE | M | ephemeral |
| Presence flap-damping + typing rate gate | CORE | M | — |
| Unity/C# SDK (main-thread-safe event queue) | SDK | L | proto |
| (optional) `sidecar-ipc` unix-socket mode for edge/indie | BODY/infra | L | codec |

### P3 — Platform polish
| Change | Where | Effort | Deps |
|---|---|---|---|
| Tenant-scoped DB adapters + edge id-namespacing | BODY | L | authz |
| Helm/k8s, libcluster DNS topology, HPA | infra | L | release |
| Go + Python SDKs | SDK | XL | proto |
| Per-tenant rate limits/quotas, docs, schema governance | BODY/docs | L | — |

---

## 6. Game-specific guidance

**The persist-everything model is the core problem for games.** Both `Conversation.submit`
(`conversation.ex:71`) and `inject` (`conversation.ex:92`) **always** call `persistence().append/2` before
fan-out, and every `:send` routes through the single-writer owner `GenServer.call` (`session.ex:97`,
blocking, possibly cross-node) to assign a durable monotonic `seq`. For durable chat (guild/DM/whisper)
that's correct. For **lobby/proximity/global/"X is typing"/kill-feed** it's wrong: you pay a durable
write + a serialization hop + a cross-node round-trip **per line** of disposable text, polluting storage
and adding tick jitter.

**Fix (CORE, P2):** a per-conversation/per-message **ephemeral mode**. Branch on `Message.kind` (the field
already exists at `message.ex:23` — it's dead, never read in `conversation.ex`): if ephemeral, skip
`append`, fan out directly via `Chat.Fanout.dispatch`, suppress receipts, and skip/separate cursor advance
(no durable seq to resync against). For lowest latency, route ephemeral sends *around* the owner GenServer
entirely. The proto should carry a `Delivery` enum (`durable|ephemeral`) on `Send`. A no-op/TTL/ring-buffer
Persistence adapter for `"lobby:*"` prefixes is a stopgap, but it's all-or-nothing per adapter — the proper
fix is the core `kind` branch.

**Transport choice — be honest with game teams:**
- WebSocket/gRPC are **TCP** → head-of-line blocking + reconnect cost. **Fine for chat-grade RTT** (tens
  of ms WAN, dominated by network not the gateway), **wrong for gameplay netcode** (positions/hits).
  Movement/state stays on the game's own **UDP/RUDP** path. Chat rides *alongside* the sim, never replaces
  the realtime transport. Co-locate a gateway pod per region.
- For absolute-lowest cross-process latency on a co-located game server, the **`sidecar-ipc` unix-socket**
  mode (µs RTT, hard process isolation so a 1000-member guild broadcast can't steal CPU frames from the
  60 Hz tick) is the strongest story — but it's P2+/optional, and its single-binary packaging is
  **unproven on BEAM** (Burrito cross-target maturity; the Locus precedent is a Rust crate, not a BEAM
  binary).

**Patterns:** durable channels (guild/party/DM) over the normal Session/persistence path; ephemeral
channels (lobby/proximity/global) over the no-persist path; the authoritative dedicated server is the
**trusted publisher** — it uses `/admin inject` for system/broadcast messages and `create_conversation`
per match. Disable `permessage-deflate` for many small chat frames (CPU/latency); enable it only for large
history pages.

---

## 7. Risks & open questions

1. **Auth must land in CORE before any public edge — non-negotiable.** Wiring it only at the gateway
   leaves the in-VM `inject`/admin path and any second body unguarded; strong typing (gRPC) makes an
   unauthenticated API *easier* to abuse, not safer. Single highest-priority item.
2. **Stable `device_id` is load-bearing.** Pulsar mints `user_id: "web-"<>rand` and `device_id: "web"`
   (constant) — fine for an anonymous firehose, **broken for chat**: cursor-based catch-up
   (`session.ex:209`) needs a *stable* device_id per principal or clients miss/re-receive on reconnect.
   The gateway must mint/accept stable device ids tied to the authenticated principal.
3. **`String.to_atom` on the `type` field would exhaust the atom table.** Codec must use a hardcoded
   allow-list. Mandatory, not optional.
4. **Schema-freeze timing.** Once Node clients ship, the JSON frame is a public contract with no Buf-style
   governance in P0. Add the 1-byte version prefix and "ignore unknown types" rule *before* the first SDK
   ships, or P1's protobuf migration breaks deployed clients.
5. **Tenancy model — judgment call.** ids are flat global strings (no tenant dimension). Two tenants using
   `conversation_id "general"` share the same owner/fanout/presence — a cross-tenant leak. **Recommended
   cheap path:** keep core tenant-agnostic; the Auth adapter returns a namespaced principal
   (`"acme:alice"`) and the gateway rewrites every inbound `conversation_id` to `"acme:<id>"`. Then
   `member?`/`authorize` enforce isolation naturally. The invasive alternative (first-class `tenant_id` in
   `Chat.Types`, registry/`:syn`/persistence keys) is correct but L+ and probably not worth it for v1.
6. **Ephemeral ↔ cursor interaction.** Ephemeral channels have no durable seq, so reconnecting clients
   must *not* expect replay — needs explicit design so catch-up doesn't break.
7. **Session is `restart: :temporary` and socket-bound** (`session.ex:21`) — a gateway pod restart drops
   all its sockets. Clients must reconnect+resync. The engine supports it; the **SDK reconnect/resync
   logic is the single most bug-prone piece** and must be gotten right per language.
8. **Hex publish is mis-targeted:** `@source_url` is a placeholder (`intenttext/chat-engine`), and the
   firewall_test moduledoc still references an old umbrella path — metadata lifted from Pulsar and not
   re-verified. Fix before publishing.

---

**Bottom line:** the engine is one well-designed seam away from being a polyglot chat brain. The seam is
real and proven (Pulsar runs it). Ship the WS/JSON gateway (P0), but **wire auth, limits, and the codec
first** — without them you'd be exposing an OOM-able, read-any-conversation service to the internet.
