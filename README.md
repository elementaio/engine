# chat_engine

**A real-time messaging engine core, in Elixir/BEAM.** Connections, ordering, fan-out, presence,
receipts, offline catch-up, and clustering — behind clean ports. It ships **no database** and **no
product features**; you plug those in through small behaviours. Drive it from any body — a chat app, a
live-feed/dashboard, an IoT command bus, a support inbox, an ERP module.

Two delivery modes share the same fan-out: **durable** messages (the default) get a gap-free `seq`,
catch-up, and offline wake; **ephemeral** messages (`kind: :ephemeral`) are broadcast live-only to online
subscribers — no persistence, no `seq`, no history — for prices/telemetry/presence-style signals.

> Extracted from the Pulsar project to live as its own versioned package. Pulsar is one *body* built on
> this engine; this repo is the engine itself.

## Why it's structured this way

- **Pure core, enforced.** `apps`-free single library whose only dependency is `:syn` (cluster-global
  process registry). `Chat.FirewallTest` fails the build if anyone adds a transport/web/DB dependency
  (`:bandit`, `:ecto`, `:postgrex`, `:redix`, `:phoenix`, …). Those belong in a body, never here.
- **Ports & adapters.** Everything durable lives behind a behaviour; everything on a node is ephemeral
  and reconstructable. That's what makes the engine stateless-to-scale and DB-agnostic.
- **Payloads are opaque `binary()`** — the engine never inspects message content, so end-to-end
  encryption layers on with zero engine changes.

## The ports (the contract)

Each is an Elixir behaviour you implement over your store and wire in config. Bundled **in-memory
reference adapters** (`Chat.Adapters.InMemory.*`) are the executable spec and the zero-setup default.

| Port | Role |
|---|---|
| `Chat.Persistence.Port` | the durable, ordered, idempotent message log (gap-free monotonic `seq` per conversation) — the load-bearing one |
| `Chat.ConversationStore.Port` | membership (paged), `member_count` O(1), `conversations_for/1` |
| `Chat.CursorStore.Port` | per-device delivery cursor (monotonic) — powers exactly-once offline catch-up |
| `Chat.ReceiptStore.Port` | per-(conversation,user) read watermarks ("seen by N") |
| `Chat.PresenceStore.Port` | coarse durable last-seen (lossy-tolerant) |
| `Chat.Auth.Port` | authenticate/authorize — the engine enforces *your* verdict, never owns identity |
| `Chat.OfflineQueue.Port` | optional push-notification *wake* hook — `notify/3` is called (off the hub) for each member with no online session when a durable message lands; best-effort, since the recipient still catches up by cursor on reconnect |

## Use it

```elixir
# mix.exs — depend on it (path while co-developing; git tag / Hex once published)
def deps, do: [{:chat_engine, path: "../engine"}]
```

```elixir
# wire your adapters (or keep the bundled in-memory ones for dev/test)
config :chat_engine,
  persistence_adapter:        MyApp.Persistence.Postgres,
  conversation_store_adapter: MyApp.Conversations.Postgres,
  # …

# implement a transport so the core can push to your clients
defmodule MyApp.Transport do
  @behaviour Chat.Transport
  @impl true
  def push(client_ref, %Chat.Envelope{} = env), do: send(client_ref, {:chat_out, env}) && :ok
  @impl true
  def close(client_ref, reason), do: send(client_ref, {:chat_close, reason}) && :ok
end

# open a session per connected device, feed it inbound envelopes
{:ok, session} = Chat.Session.connect(%{user_id: uid, device_id: did, transport: {MyApp.Transport, self()}})
Chat.Session.handle_inbound(session, %Chat.Envelope{type: :send, conversation_id: "c1", id: "m1", payload: bytes})
```

Control API (in-VM): `Chat.create_conversation/2`, `Chat.add_member/2`, `Chat.members/1`,
`Chat.online?/1`, `Chat.read_state/2`, `Chat.append/2`, `Chat.inject/2`, `Chat.history/3`,
`Chat.history_page/3` (paged: `%{messages, next_after, more?}` — re-call with `next_after` until
`more?` is false), `Chat.latest_seq/1`.

The client-facing `:sync` verb mirrors this over the wire: each `:sync_page` reply carries a `seq`
continuation cursor and a `more` flag, so a client pages a long history by re-issuing `:sync` with that
`seq` until `more` is false (page size = the request's `count`, capped at `:sync_page_max`).

Health & lifecycle: `Chat.ready?/0` (config valid **and** not draining — wire it to a load-balancer
probe), `Chat.drain/0` / `Chat.resume/0` (graceful node roll: a draining node refuses NEW sessions with
`{:error, :draining}` but keeps existing ones running until their clients disconnect).

## Boot-time validation

The engine validates its config at boot (`Chat.Config.validate!/0`, run from `Chat.Application.start/2`):
every **required** port must be configured, loadable, and actually implement its behaviour, and the
numeric knobs (`max_payload_bytes`, `max_mailbox`, `presence_max`, `typing_max`) must be positive
integers. A misconfigured body fails **loudly at boot** rather than with a confusing crash on the first
message.

## Testing your adapters

The load-bearing port ships an **executable contract test-kit**. Point it at your adapter and it asserts
the guarantees (idempotency on `id`, monotonic gap-free `seq`, ordered `read_after`, and the optional
`append/3` CP fence — run automatically when your adapter implements it):

```elixir
defmodule MyApp.Persistence.ContractTest do
  use Chat.Persistence.PortTest, adapter: MyApp.Persistence, setup: &MyApp.TestSupport.reset/0
end
```

## Develop

```sh
mix deps.get
mix test                 # includes the firewall + adapter + contract-kit tests
mix test --include distributed   # also the multi-node tests (needs epmd)
mix credo                # static analysis (CI-gated)
mix dialyzer             # type checking (CI-gated, separate job)
mix format
```

## Clustering

Sessions and fan-out live in cluster-global `:syn` groups; each conversation's owner is placed on a
deterministic node by rendezvous hashing. **Node formation is the body's job** (libcluster). On one node
it's a cluster-of-one and behaves identically.

## License

MIT © Emad Jumaah
