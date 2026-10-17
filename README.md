# Natsbeam

A [NATS](https://nats.io)-compatible messaging server written in Elixir.

The official `nats` CLI, `nsc`, and any NATS client library talk to it unchanged.
Underneath, everything is a BEAM process: accounts, listeners and connections
are supervised, and the whole configuration (users, NKeys, operator JWTs,
listeners, auth providers) can be changed at runtime with ordinary function
calls instead of config files and reload signals.

## Status

| Area | Supported |
| --- | --- |
| Client protocol | `INFO`, `CONNECT`, `PUB`, `HPUB`, `SUB`, `UNSUB` (with max), `MSG`, `HMSG`, `PING`/`PONG`, `+OK`/`-ERR`, verbose & pedantic modes, echo off |
| Subjects | `*` / `>` wildcards, queue groups, request/reply, headers, `no_responders` 503 |
| Limits | max payload, max control line, max connections (server + per account), max subscriptions, ping/stale detection, auth timeout, slow consumer, write timeout |
| Auth: token | plaintext or bcrypt (`$2a$`…), live-editable |
| Auth: username/password | plaintext or bcrypt, per-user account and publish/subscribe permissions (allow/deny, queue filters, response permissions) |
| Auth: NKeys | Ed25519 challenge/response (`nonce` in INFO, `nkey` + `sig` in CONNECT), `nk`-compatible encoding |
| Auth: operator / JWT | operator JWTs with signing keys, account JWTs (memory resolver), user JWTs incl. signing keys, scoped signing keys, bearer tokens, expiry, revocations, `src` CIDRs, connection types, account `conn`/`subs`/`payload` limits, default permissions |
| Auth: custom | `Natsbeam.Auth.Provider` behaviour, swapped at runtime |
| System account | `$SYS` by default (nats-server's name; `SYS` or any other via config, the operator's in JWT mode): `$SYS.REQ.CLAIMS.UPDATE` / `LOOKUP` / `LIST` / `DELETE` (so `nsc push` works), `$SYS.REQ.SERVER.PING[.<Z>]` and `$SYS.REQ.SERVER.<id>.<Z>` for `STATSZ`, `IDZ`, `VARZ`, `CONNZ`, `ROUTEZ`, `LEAFZ`, `SUBSZ`, `GATEWAYZ`, `HEALTHZ`, `ACCOUNTZ`, `JSZ` with nats-server's request filters, periodic `$SYS.SERVER.<id>.STATSZ` and a `SHUTDOWN` event (so `nats server ping|list|info|report` work, also across routes and leafnodes next to Go servers) |
| TLS | standard upgrade-after-INFO and `handshake_first`, via `:ssl` |
| Clustering | Erlang distribution: accounts join a `:pg` group; publishes fan out to other nodes with cluster-wide queue-group semantics |
| JetStream | streams (memory/file, limits/interest/workqueue retention, max msgs/bytes/age/per-subject, discard old/new, dedupe, expected-* headers, rollups, purge, get/delete, direct get), pull and push consumers (ack/nak/term/in-progress, ack_wait redelivery, backoff, max_deliver, max_ack_pending, batch/expires/no_wait/heartbeat pulls, filters, all deliver policies, headers_only, pause/resume), per-message TTLs with subject delete markers, mirrors and sources, snapshot/backup and restore, persistence across restarts, KV and Object Store via the standard client libraries |
| JetStream clustering | `num_replicas` up to 5 over Erlang distribution: leader/replica streams with majority-acknowledged PubAcks, automatic re-election when a leader dies, `stream cluster step-down`, peer removal, consumers re-created on the new leader from replicated checkpoints, cluster info in stream/consumer info |
| nats-server links | joins a Go nats-server cluster as a **route** (route pooling with per-account slots and pinned-account routes such as `$SYS`, or the legacy no-pool mode; dials and accepts) and connects to or accepts **leafnodes**; interest, queue weights, headers, request/reply and JetStream deliveries cross the link; JetStream on both sides stays reachable through domains; **gateways** to other clusters (super-cluster), both connections per pair, optimistic/interest-only modes, `_GR_.` reply mapping |
| In-VM clients | `Natsbeam.subscribe/publish/request` from any process, no socket |
| Observability | `:telemetry` events for connects, auth, publishes, permission violations, slow consumers, config changes |

Not implemented (yet): WebSocket/MQTT, account imports/exports, auth callout, the
nats-server `.conf` file format, HTTP monitoring endpoints. See "Known gaps"
below for the JetStream details.

## Running

```sh
mix deps.get
iex -S mix            # listens on 0.0.0.0:4222
```

Environment overrides: `NATSBEAM_PORT`, `NATSBEAM_HOST`, `NATSBEAM_NAME`,
`NATSBEAM_TOKEN`, `NATSBEAM_PEERS` (comma-separated Erlang node names to join),
`NATSBEAM_SYSTEM_ACCOUNT` (default `$SYS`; `none` disables it), and the link
settings under "Joining a Go nats-server".

Then, from another terminal:

```sh
nats sub 'orders.>' &
nats pub orders.new 'hello' -H X-Trace:1
nats reply svc.echo 'echo {{Body}}' &
nats request svc.echo ping
```

## Dynamic configuration

Everything below takes effect immediately for new connections; removing a
user, NKey, operator or account disconnects the clients that depended on it.

```elixir
# Listeners
Natsbeam.start_listener(name: :internal, port: 4223, host: "127.0.0.1")
Natsbeam.start_listener(name: :tls, port: 4443, tls: [certfile: "cert.pem", keyfile: "key.pem"])
Natsbeam.stop_listener(:internal)
Natsbeam.listeners()                      # %{default: 4222, tls: 4443}

# Accounts (also auto-created when an identity references one)
Natsbeam.add_account("ACME", max_connections: 100)
Natsbeam.remove_account("ACME")           # kicks its clients
Natsbeam.accounts()

# Users / tokens / NKeys
Natsbeam.add_user("alice", password: "s3cret", account: "ACME",
  permissions: %{publish: ["orders.>"], subscribe: %{allow: ["orders.>", "_INBOX.>"], deny: ["orders.admin.*"]}})
Natsbeam.add_user("svc", password: Natsbeam.hash_password("x"), account: "ACME")
Natsbeam.add_nkey("UBWWOGEOBPBB2WGXNOOYEKWSCVEFN2W52ZXZP465MRJ3OTQ3M64VHCGZ", account: "ACME")
Natsbeam.set_token("shared-secret")
Natsbeam.remove_user("alice")

# Operator mode (decentralised JWT auth, as produced by nsc)
Natsbeam.add_operator(File.read!("operator.jwt"))
Natsbeam.add_account_jwt(File.read!("acme.jwt"))   # or let `nsc push` do it
Natsbeam.account_jwts()

# Custom auth: any module implementing Natsbeam.Auth.Provider
Natsbeam.set_auth_provider(MyApp.NatsAuth, tenant_lookup: &MyApp.Tenants.get/1)

# Clients
Natsbeam.clients()
Natsbeam.disconnect(client_id, "maintenance")
Natsbeam.info()
```

Static configuration uses the same shapes:

```elixir
config :natsbeam,
  listeners: [%{name: :default, port: 4222}],
  server_name: "beam-1",
  max_payload: 8_000_000,
  auth: [
    users: [
      %{user: "alice", password: "s3cret", account: "ACME", permissions: %{publish: ["orders.>"]}}
    ],
    nkeys: [%{nkey: "UB...", account: "ACME"}]
  ],
  accounts: %{"ACME" => %{max_connections: 100}}
```

## Operator mode with nsc

```sh
nsc add operator --name OP --generate-signing-key --sys
nsc add account --name ACME
nsc add user --account ACME --name alice --allow-pub 'orders.>' --allow-sub 'orders.>'
nsc edit operator --account-jwt-server-url nats://127.0.0.1:4222
```

```elixir
Natsbeam.add_operator(File.read!("~/.local/share/nats/nsc/stores/OP/OP.jwt"))
Natsbeam.add_account_jwt(File.read!(".../accounts/SYS/SYS.jwt"))   # the system account must be preloaded
```

```sh
nsc push -A                                            # pushes ACME over $SYS.REQ.CLAIMS.UPDATE
nats --creds ~/.local/share/nats/nsc/keys/creds/OP/ACME/alice.creds pub orders.new hi
nats server list --creds .../SYS/sys.creds
```

## JetStream

JetStream is on by default for the default account; file-backed streams live
under `data/jetstream` (`NATSBEAM_STORE_DIR` or `config :natsbeam, jetstream:
[store_dir: ...]`). Other accounts opt in with `Natsbeam.add_account("ACME",
jetstream: true)` (or a limits map) and JWT accounts through their
`mem_storage`/`disk_storage` limits.

```sh
nats stream add ORDERS --subjects 'orders.>' --storage file --defaults
nats pub orders.new 'hello'                       # or `nats req` to see the PubAck
nats consumer add ORDERS WORK --pull --ack explicit --defaults
nats consumer next ORDERS WORK --count 10
nats sub --stream ORDERS                          # ordered ephemeral consumer
nats kv add CFG --history 5 && nats kv put CFG color blue && nats kv watch CFG
nats bench js pub async --msgs 100000 --create --stream BENCH bench.js
```

```sh
nats stream add LOGS --subjects 'logs.>' --replicas 3 --defaults   # replicated
nats stream info LOGS                       # Cluster Information: leader + replicas
nats stream cluster step-down LOGS
nats pub logs.tmp 'expires' -H Nats-TTL:30s  # needs --allow-msg-ttl on the stream
nats consumer pause LOGS WORK 10m && nats consumer resume LOGS WORK
nats stream add MIRROR --mirror LOGS --defaults
nats backup stream LOGS ./logs-backup && nats backup restore stream ./logs-backup
```

Design: a `Natsbeam.JetStream.Stream` process per stream replica owns an ETS
message store (`Natsbeam.JetStream.Store`, an ordered set plus per-subject
indexes) and, for file storage, an append-only CRC-checked log that is
replayed at boot and compacted as deletions accumulate. One replica is the
leader: it registers a `:global` name, subscribes to the stream's subjects,
runs the consumers on its node and replicates every mutation (messages,
deletes, purges, config, consumer checkpoints) to the other replicas as plain
Erlang messages; a PubAck is released once a majority has applied the
message. Replicas monitor the leader and run a deterministic election (most
data wins, ties by node name) when it goes away; the old node rejoins as a
replica. Consumers (`Natsbeam.JetStream.Consumer`) read the ETS table directly
and deliver with `$JS.ACK` reply subjects; the per-account `$JS.API.>`
responder is an in-VM queue subscriber, so every node answers API calls; it
lives as long as the account and (un)subscribes in place when the account's
JetStream setting changes.
Mirrors and sources pull from the origin leader wherever it lives. Snapshots
are Snappy-framed archives of the whole stream plus consumer checkpoints.
Memory streams are not persisted. Durable consumer state is checkpointed every
two seconds, so after a crash the last few acks may be redelivered.

### Known gaps

* Replication is leader-driven message passing, not Raft: during a network
  partition both sides can elect a leader; `:global` kills one of them on
  heal and its unreplicated writes are lost. Nodes must be fully meshed
  (`:global`/`:pg` assume it).
* Replicas that fall far behind (or were partitioned) are resynced by a full
  snapshot of the leader's store, which is O(stream size).
* Mirrors and sources from an `external` API prefix (another domain over a
  link) work through an ephemeral push consumer on the origin. External
  mirrors follow deletes and purges by polling the origin's `STREAM.INFO`
  (`deleted_details`) through the same prefix every 2 s (and at once when a
  delivery skips sequences), so a removal shows up within a poll interval
  rather than instantly. Like local mirrors they follow the origin's
  `first_seq`, including limit-based expiry. External sources only append.
  `external.api` cannot reach another local account (there are no account
  imports/exports). Go nats-server mirrors of a natsbeam stream skip messages
  removed before they were delivered but, as with any Go mirror, do not
  follow later deletes/purges. `subject_transforms` and `republish` are
  accepted in config but not applied.
* Snapshot archives are natsbeam-specific (not nats-server tar format), so
  backups do not move between the two servers.
* The whole working set stays in memory even for file streams; stream size is
  bounded by RAM. No encryption at rest, no compression.
* Consumer pause, per-message TTLs and API level 1 are reported for client
  gating, but priority groups, pinned consumers, atomic/fast batch publish,
  message counters and `Nats-Expected-Last-*` on batch publishes are not
  implemented.
* `nats server report jetstream` needs a user in the system account (`$SYS`
  unless configured otherwise, the operator's in JWT mode); `meta_cluster` is
  synthesised (there is no meta-group leader), so next to Go servers the
  report shows natsbeam as the leader of its own meta group.

## Joining a Go nats-server

natsbeam speaks nats-server's server-to-server protocols, so it can be a
member of an existing deployment:

```sh
# route: become a member of the Go cluster "gocluster"
NATSBEAM_CLUSTER_NAME=gocluster NATSBEAM_CLUSTER_PORT=6232 \
NATSBEAM_ROUTES=nats-route://127.0.0.1:6222 iex -S mix

# leaf: hang off a Go hub's leafnode port, JetStream on both sides via domains
NATSBEAM_JS_DOMAIN=beam NATSBEAM_LEAF_REMOTES=nats-leaf://127.0.0.1:7422 iex -S mix
nats --js-domain hub stream ls      # Go's JetStream from a natsbeam client
nats -s nats://go:4222 --js-domain beam stream ls   # natsbeam's JetStream from a Go client

# accept Go spokes (nats-server `leafnodes { remotes: [{url: "nats-leaf://beam:7422"}] }`)
NATSBEAM_LEAF_PORT=7422 iex -S mix
```

The same things are live calls: `Natsbeam.add_route/2`, `Natsbeam.add_leaf_remote/2`
(with `credentials:`, `nkey:`, `token:` or `user:`/`pass:`),
`Natsbeam.start_route_listener/1`, `Natsbeam.start_leaf_listener/1`, `Natsbeam.links/0`.

### Route pooling

Routes pool like nats-server 2.10+ (`cluster { pool_size: 3 }`, the default
there and here): one peer is several TCP connections, each its own
`Natsbeam.Link`.

* **Pool connections** `0..size-1`. Account `name` lives on slot
  `fnv32a(name) rem size` (`Natsbeam.Link.pool_slot/2`, the same function as
  nats-server's `computeRoutePoolIdx`); its `RS+`/`RS-`/`RMSG`/`HMSG` only use
  that connection, with the account as the first argument.
* **Pinned-account connections**, one per dedicated account: the system
  account under its configured name (`$SYS` by default, `SYS` for a Go
  `system_account: SYS`; nats-server always pins its system account) plus
  `:pinned_accounts` (nats-server's `cluster { accounts: [...] }`, which must
  list the same accounts). They carry that one account and leave its name out
  of every frame. If the names differ, Go refuses the pinned connection
  (`No route for account`) and that account rides its pool slot instead.

When we dial (`add_route`), the slot-0 link opens slots `1..size-1` and the
pinned accounts after the peer's first PONG; when Go dials us, each inbound
connection names its slot (`route_pool_idx`) or account (`route_account`) and
is registered under it. If both servers dial each other, each slot keeps the
connection the Go server kept and our other connection waits until it closes.
`Natsbeam.links/0` shows `slot` (`{:pool, idx}`, `{:account, name}` or
`:no_pool`), `pool_size`, `local_port`/`peer_port` per connection.

```elixir
config :natsbeam, route_pool_size: 3, pinned_accounts: ["ACME"]   # NATSBEAM_ROUTE_POOL_SIZE, NATSBEAM_PINNED_ACCOUNTS
config :natsbeam, route_pool_size: 0      # legacy: one connection, every account on it
```

With `route_pool_size` 0 or -1 (or a peer without pooling, `pool_size: -1`)
the route falls back to no-pool mode, where one connection carries every
account and pinned routes are refused, as nats-server does.

How it works (`Natsbeam.Link`): every account tells links when the weight of a
`{subject, queue}` changes (excluding the link's own remote subscriptions, and
for routes everything learned over any route), and the link sends `RS+`/`RS-`
(routes, the accounts of its slot) or `LS+`/`LS-` (leafs, one account).
Remote interest is registered as `{:link, ...}`/`{:route_link, ...}` subscriptions so
local publishes are forwarded as `RMSG`/`LMSG`/`HMSG`; inbound messages are
delivered locally and never relayed to another link, as nats-server expects
of a full mesh. JetStream deliveries use nats-server's `$JS.ACK...@subject`
reply convention. `$JS.API.>` interest is never exchanged (both servers run
their own JetStream); each side's `$JS.<domain>.API.>` is.

Validated against nats-server 2.15.1 over both link types: pub/sub with
wildcards, headers, request/reply, no-responders, cross-server queue groups,
interest add/remove, 1 MB payloads, 50k msg/s benches, reconnect after either
side restarts, gossip (a second Go node dials natsbeam's route port), pooled
routes dialled by either side or both (3 slots + the pinned system account, accounts on
their fnv32a slot as Go's `/routez` reports them), the legacy no-pool mode, leaf
auth by user/password, token, operator creds and TLS, Go spokes connecting to
natsbeam, and JetStream in both directions through domains: streams,
pull/push/ordered consumers, pause, KV (domain-mapped `$KV` writes included),
Object Store, benches, backup/restore and external mirrors, and system-account
tooling (`nats server list|ping|info|report connections|report jetstream`)
from either side over routes, a system-account leaf to a Go hub, and a Go
spoke's system-account leaf to natsbeam.

Limits: route compression (`compression: s2_*`) is not supported (we
advertise none, so Go stays uncompressed), accounts cannot be moved to or from
a pinned route by a config reload on a live connection (`route_acc_add_reqid`
INFOs are ignored), and Go JetStream in *clustered* mode needs
Raft peers that natsbeam does not speak, so JetStream stays per-server and is
bridged by domains. Queue members on a Go hub are preferred over a natsbeam
spoke, as for any leaf.

### System account across links

natsbeam answers nats-server's system requests and sends its events in its
system account, so once that account crosses the link, Go-side tooling lists
and queries natsbeam like any other server, and natsbeam-side tooling sees the
Go servers (their replies come back over the same link):

```sh
nats --user sys --password sys -s nats://go:4222 server list 2      # both servers
nats --user sys --password sys -s nats://go:4222 server info beam-1
nats --user sys --password sys -s nats://go:4222 server report connections
nats --user sys --password sys -s nats://go:4222 server report jetstream
nats --user sys --password sys -s nats://beam:4222 server list 2    # and from our side
```

**Routes.** The system account must have the same *name* on both servers,
because a route carries account names (`$SYS.REQ.*` travels on the system
account's pinned route). nats-server calls it `$SYS` unless configured; with
`system_account: SYS` (or operator mode, where it is the account's public key)
the wire name is that configured name, so configure natsbeam to match:

```hcl
# nats-server
system_account: SYS
accounts { SYS { users: [{user: sys, password: sys}] } }
cluster { name: gocluster, listen: 0.0.0.0:6222, routes: [nats-route://beam:6232] }
```

```elixir
config :natsbeam, cluster_name: "gocluster", cluster_port: 6232,
  routes: ["nats-route://go:6222"],
  auth: [system_account: "SYS",                # NATSBEAM_SYSTEM_ACCOUNT=SYS
         users: [%{user: "sys", password: "sys", account: "SYS"}]]
# live: Natsbeam.set_system_account("SYS")
```

**Leafnodes.** A leaf connection is bound to one account and `$SYS` only crosses
the one bound to the system account, so add a second remote for it (account
names need not match across a leaf):

```elixir
# natsbeam spoke, Go hub with system_account: SYS
Natsbeam.add_leaf_remote("nats-leaf://hub:7422", account: "$G", credentials: "app.creds")
Natsbeam.add_leaf_remote("nats-leaf://hub:7422", account: "$SYS", credentials: "sys.creds")
# NATSBEAM_LEAF_REMOTES='nats-leaf://hub:7422;credentials=app.creds,nats-leaf://hub:7422;account=$SYS;credentials=sys.creds'
```

```hcl
# Go spoke, natsbeam hub (NATSBEAM_LEAF_PORT=7422, a natsbeam user "sys" in $SYS)
system_account: SYS
leafnodes {
  remotes: [
    { url: "nats-leaf://app:app@beam:7422" },
    { url: "nats-leaf://sys:sys@beam:7422", account: SYS }
  ]
}
```

`NATSBEAM_LEAF_REMOTES` entries are `url[;key=value...]` with `account`,
`credentials`, `user`, `pass`, `token` and `nkey`. As on any Go hub, a system
leaf from a different JetStream domain does not extend JetStream; the
`$JS.<domain>.API` bridging stays on the application account's leaf.

Every server sends `$SYS.SERVER.<id>.STATSZ` heartbeats (`statsz_interval`,
default 10 s; natsbeam starts at 250 ms and backs off like nats-server) and
`$SYS.SERVER.<id>.SHUTDOWN` on stop; natsbeam's come from
`Natsbeam.SystemAccount`, which also tracks the other servers'
(`Natsbeam.SystemAccount.remote_servers/0`, `active_servers` in STATSZ).
Requests honour nats-server's filters (`server_name`, `cluster`, `host`,
`domain`, `tags` from `config :natsbeam, server_tags: [...]`, `exact_match`),
so `nats server info <go server>` is answered by that server alone.

### Gateways (super-cluster)

natsbeam can also be one cluster of a super-cluster: its gateway name is its
cluster name, and it speaks nats-server's gateway protocol to other clusters.

```sh
# natsbeam cluster "beam", gateway port 7522, dialling the Go cluster "gocluster"
NATSBEAM_CLUSTER_NAME=beam NATSBEAM_GATEWAY_PORT=7522 \
NATSBEAM_GATEWAYS=gocluster=nats://go-host:7222 iex -S mix
```

```
# nats-server side
gateway {
  name: gocluster
  listen: 0.0.0.0:7222
  gateways: [{name: beam, url: "nats://beam-host:7522"}]   # optional, see below
}
```

Live calls: `Natsbeam.start_gateway_listener(port, host: ...)`,
`Natsbeam.add_gateway("nats://host:7222", name: "gocluster")`,
`Natsbeam.remove_gateway("gocluster")`, `Natsbeam.gateways/0` (also listed in
`Natsbeam.links/0` with `kind: :gateway`). Config keys: `gateway_port`,
`gateway_host`, `gateway_advertise` (`NATSBEAM_GATEWAY_ADVERTISE`, the
`host:port` other clusters dial back), `gateways: [%{name:, url:}]`,
`gateway_auth: %{user:, pass:}`, `gateway_reject_unknown`,
`gateway_interest_only` (default `true`), `gateway_max_runsub` (default 1000).

How it works (`Natsbeam.Gateway`, protocol notes in its moduledoc): each pair
of clusters runs **two** connections, ours (outbound: we send messages,
receive interest) and theirs (inbound: we receive messages, send interest).
If the Go side is not configured with our URL it learns it from the
`gateway_urls` of our INFO and dials back (an implicit gateway); natsbeam does
the same for Go clusters that dial it, and for gateways gossiped to it.
Interest follows nats-server: optimistic sending with `A-`/`RS-` "no interest"
replies, switching an account to interest-only (`INFO` commands 2/3 around
the full `RS+` list) after too many `RS-`, or immediately when accepting a
gateway (nats-server >= 2.9 behaviour, natsbeam's default, advertised as
`gateway_iom`). Queue groups are served in the publishing cluster when it has
a member, otherwise by one remote cluster; every message is one `RMSG`/`HMSG`
per gateway carrying its queue list. Requests use `_GR_.<cluster>.<server>.`
mapped replies in both directions, so replies reach the requester's cluster
even before its inbox interest has propagated.

Validated against nats-server 2.15.1 (`test/natsbeam/go_gateway_test.exs`):
both connections, pub/sub with headers both ways, request/reply both ways
(mapped replies checked in Go's trace), queue groups, the optimistic mode with
`A-`/`RS-`/`RS+` and the switch to interest-only, implicit dial-back, and both
connections coming back after the Go server restarts.

Limits:

* No relaying: messages arriving over a gateway go to local subscribers and
  BEAM peers only, never to routes, leafnodes or other gateways, and only
  local client/in-VM interest is advertised to gateways (a Go route or leaf
  subscriber attached to natsbeam is not reachable from another cluster).
  Gateway interest is never advertised over routes or leafnodes.
* BEAM clustering: interest is advertised per node, so in interest-only mode a
  subscriber on a BEAM node without its own gateway connection is not seen by
  the remote cluster; with several natsbeam nodes connected to the same remote
  cluster, publishes forwarded between BEAM nodes may reach it twice.
* natsbeam accounts start lazily; remote interest for an account that has not
  started yet is registered when it starts, so a publish that creates the
  account can miss a remote subscriber.
* JetStream does not span gateways: `$JS.API.>` is neither advertised nor
  delivered (each side runs its own JetStream; use distinct domains, whose
  `$JS.<domain>.API.>` interest does cross). `$SYS` gets no special treatment.
* Inbound gateway connections are plain TCP (outbound ones upgrade to TLS if
  the remote requires it); no gateway nkey/JWT auth, only user/password.
* In optimistic mode an account with no gateway-specific state has a
  catch-all subscription, so `no_responders` is not reported there and push
  consumers see interest; the default interest-only mode avoids both.

## In-VM participation

Elixir processes are first-class NATS citizens:

```elixir
{:ok, sid} = Natsbeam.subscribe("orders.*", account: "ACME", queue: "workers")
receive do
  {:natsbeam_msg, ^sid, subject, reply, headers, payload} -> ...
end

Natsbeam.publish("orders.new", "hello", account: "ACME", headers: [{"X-Source", "beam"}])
{:ok, %{payload: p}} = Natsbeam.request("svc.echo", "ping", account: "ACME", timeout: 1_000)
```

Subscriptions belong to the calling process and disappear when it exits.
This is also how `Natsbeam.SystemAccount` serves `$SYS` requests.

## Clustering

Start several nodes in the same Erlang cluster and they form one NATS
"cluster" for every account automatically. There is no interest propagation
yet: every publish is forwarded to each peer node, which matches it locally.
Queue groups receive exactly one copy cluster-wide.

```sh
NATSBEAM_PORT=4222 elixir --sname a -S mix run --no-halt
NATSBEAM_PORT=4223 NATSBEAM_PEERS=a@$(hostname -s) elixir --sname b -S mix run --no-halt
nats -s nats://127.0.0.1:4223 sub 'x.>' &
nats -s nats://127.0.0.1:4222 pub x.y hi
```

## Architecture

```
Natsbeam.Supervisor
├── Natsbeam.Registry          account and listener names
├── Natsbeam.ClientRegistry    connected clients + metadata
├── :pg (Natsbeam.PG)          cluster membership per account
├── Natsbeam.Config            live config in ETS, telemetry on change
├── Natsbeam.Server            server NKey id, client id counter
├── Natsbeam.AccountSupervisor one Natsbeam.Account per account
│     └── Natsbeam.Sublist     ETS trie of subscriptions (read lock-free by publishers)
├── Natsbeam.ListenerSupervisor one ThousandIsland listener per port
│     └── Natsbeam.Client      one process per connection (parser, auth, delivery)
├── Natsbeam.SystemAccount     $SYS request responder
├── Natsbeam.LinkSupervisor    Natsbeam.Link per route/leaf connection, Natsbeam.Gateway per gateway connection; Link.Acceptor listeners
├── Natsbeam.JetStream         stream / consumer / $JS.API supervisors + recovery
│     └── Natsbeam.JetStream.Cluster   :global names, :pg node group, replica placement
└── Natsbeam.Boot              starts configured listeners / connects peers
```

A publish runs entirely in the publisher's process: it matches the subject
against the account's ETS trie and sends one Erlang message per matching
subscription. Subscriber processes batch their mailbox into a single socket
write. Subscription writes are serialised through the account process, and
the account monitors every subscriber so crashed processes clean themselves up.

## Tests

```sh
mix test            # unit + raw-protocol + in-VM + JetStream tests
```

Tests tagged `:cli` run the real `nats` binary (found on `PATH` or in
`~/Work/bin`), `:tls` tests need `openssl`, and `:cluster` tests start extra
BEAM nodes with `:peer` (they need `epmd`), and `:go` tests launch a real
`nats-server` and join it over routes and leafnodes; each group is skipped
when its tool is missing.

## Benchmarking

```sh
mix run bench/pub_bench.exs            # parser + publish-path microbenchmarks, then `nats bench pub`
mix run bench/pub_bench.exs --eprof    # also profile the client process during the run
mix run bench/pub_bench.exs --buffer 32768   # try another socket read buffer
```

The script prints each path's cost per message next to the numbers measured
before the publish path was optimised, so regressions stand out.
