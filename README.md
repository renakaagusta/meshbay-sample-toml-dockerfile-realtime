# meshbay-sample-toml-dockerfile-realtime

Dockerfile variant of [meshbay-sample-realtime](https://github.com/renakaagusta/meshbay-sample-realtime): same Bun +
Hono service, but it builds from [`Dockerfile`](Dockerfile) instead of Railpack. `oven/bun:1.3.14-alpine` runs
`bun install --frozen-lockfile`, copies `src/`, drops to the `bun` user, exposes 8080 and starts
`bun run src/index.ts` as `CMD` (the old Railpack start). `bun` is on `PATH` in the image, so the `command`
healthcheck in `meshbay.toml` still works unchanged.

Bun + Hono service built for the Meshbay test suite (MESH-135). It keeps long-lived connections open and fans them out
between replicas:

- `GET /ws` — WebSocket chat, every message is broadcast to every connected client
- `GET /events` — Server-Sent Events ticker, one event per second carrying the `replica` that streamed it
- `GET /replicas` — the replicas that registered themselves in Redis
- `GET /` — a small page with both connections and auto-reconnect
- fan-out between replicas over Redis pub/sub when `REDIS_URL` is set; without it everything stays on one replica

Dockerfile variant: the repo ships a `Dockerfile` (`oven/bun:1.3.14-alpine`, `bun install --frozen-lockfile`,
non-root `bun` user, `CMD ["bun", "run", "src/index.ts"]`), which is what `meshbay.toml` builds.

## Environment

| Variable | Required | Default | Description |
|---|---|---|---|
| `PORT` | no | `8080` | HTTP port; it has to match `ports` in `meshbay.toml` |
| `REDIS_URL` | no | none | `redis://...` used for pub/sub fan-out and the replica registry. Empty or unset = local broadcast only |
| `REPLICA_ID` | no | container hostname | Identity in `/health`, in SSE ticks and in every chat frame; two replicas show two different values |
| `CHANNEL` | no | `meshbay-sample-realtime:chat` | Redis pub/sub channel |
| `TICK_INTERVAL_MS` | no | `1000` | SSE tick interval |
| `HEARTBEAT_INTERVAL_MS` | no | `20000` | WebSocket heartbeat interval, keeps idle sockets warm through Caddy and Cloudflare |
| `SSE_RETRY_MS` | no | `3000` | `retry:` hint sent to `EventSource` clients |
| `IDLE_TIMEOUT_SECONDS` | no | `240` | `Bun.serve` idle timeout (maximum 255) |

Without `REDIS_URL` the service still works: a chat message reaches the sockets of the replica that received it,
`/events` still ticks, and `/replicas` lists only the answering replica.

## Run locally

```sh
bun install
bun run start            # http://localhost:8080/
```

With the fan-out path (any Redis works, for example `docker run --rm -p 6379:6379 redis:7-alpine`):

```sh
REDIS_URL=redis://localhost:6379/0 bun run start
```

Typecheck: `bun run typecheck`. To boot the app, wait for `/health` and run the very same smoke test the LIVE check
uses, in one command:

```sh
bash scripts/verify-local.sh 8099
```

A second instance on another port with the same `REDIS_URL` is enough to watch the cross-replica fan-out on one machine:

```sh
PORT=8081 REPLICA_ID=one REDIS_URL=redis://localhost:6379/0 bun run start
PORT=8082 REPLICA_ID=two REDIS_URL=redis://localhost:6379/0 bun run start
node scripts/fanout-check.mjs http://127.0.0.1:8081 http://127.0.0.1:8082   # exits 0 when a chat crosses instances
```

## Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/` | Chat page: SSE ticks, WebSocket chat, reconnects after a close |
| GET | `/health` | `200 {"status":"ok","version":"v1","replica":"...","redis":true,"clients":3,"uptime_seconds":12}` |
| GET | `/events` | `text/event-stream`: one `hello`, then a `tick` every second with `replica`, `sequence`, `clients`, `at` |
| GET | `/ws` | WebSocket, protocol below |
| GET | `/replicas` | Replicas registered in Redis (key TTL 30 s, refreshed every 10 s). Without Redis: only this replica |

### WebSocket protocol

Client to server:

```json
{"type":"chat","text":"hello"}
{"type":"ping"}
```

Server to client:

```json
{"type":"hello","replica":"<replica that accepted the socket>","client":"<replica>-1","clients":1,"redis":true,"version":"v1","at":"..."}
{"type":"chat","id":"<uuid>","text":"hello","origin":"<replica that accepted the message>","at":"..."}
{"type":"heartbeat","origin":"<replica>","clients":1,"at":"..."}
{"type":"pong","replica":"<replica>","at":"..."}
{"type":"error","error":"expected {\"type\":\"chat\",\"text\":\"...\"}"}
```

A chat frame is written once per replica: the receiving replica publishes it on the Redis channel and every replica,
its own included, delivers it to its local sockets. So `origin` (the replica that took the message) and the `hello`
replica of a socket can differ — when they do, the fan-out went through Redis.

### SSE format

```
event: hello
data: {"replica":"...","version":"v1","redis":true,"at":"..."}

event: tick
id: <replica>-<sequence>
data: {"replica":"...","sequence":1,"clients":0,"at":"..."}
```

## Scripts

| Script | What it does |
|---|---|
| `scripts/smoke.sh <url>` | LIVE check: `/health`, `/`, `/replicas`, two WebSocket clients, 5 SSE ticks; prints `OK` at the end |
| `scripts/soak.mjs <url> [seconds] [chatSeconds]` | Holds one socket and one stream open. Exit 0 = both held for the whole duration, exit 1 = a connection ended (a redeploy does that on purpose) |
| `scripts/verify-local.sh [port]` | Boots the app on a local port, waits for `/health`, runs the smoke test against it |
| `scripts/fanout-check.mjs <urlA> <urlB>` | Two instances sharing `REDIS_URL`: a chat written on the first must arrive on a socket of the second |

## Test scenarios

### 1. Deploy

Deploy the repository as a new Meshbay project (built from `Dockerfile`, `meshbay.toml` is picked up: one service `web`, port `8080`,
2 replicas, command healthcheck on `/health`). The project needs no database to come up.

```sh
curl -fsS https://<domain>/health
```

```json
{"status":"ok","version":"v1","replica":"<container hostname>","redis":false,"clients":0,...}
```

### 2. Smoke test

```sh
bash scripts/smoke.sh https://<domain>
```

It prints one `ok` line per check and ends with `OK`. The checks are: `/health` is `ok` and names its replica, `/`
serves the page, `/replicas` lists at least one replica, two WebSocket clients connect and a chat sent on one arrives
on the other, and `/events` delivers 5 ticks ~1 s apart from one replica. The chat line tells you whether the two
sockets landed on the same replica (`both sockets on one replica`) or on two (`cross-replica fan-out through Redis`) —
with `REDIS_URL` unset the second case needs two replicas and will not happen, which is not a failure.

### 3. Redis fan-out between two replicas

1. Create a Redis database in the project (Meshbay databases, template `redis`).
2. Set `REDIS_URL` on the `web` service to the URL Meshbay shows for that database (`redis://:<password>@<host>:6379/0`).
3. Redeploy, then check:

```sh
curl -fsS https://<domain>/health     # "redis": true
curl -fsS https://<domain>/replicas    # both replicas, each with its own start time and version
```

4. Open the page in two browser tabs. The replica in the header of each tab is the replica that terminated that
request. A message typed in one tab appears in both, in the other tab with the *other* replica in front of it — that is
the pub/sub round trip. If both tabs show the same replica, repeat the request (a redeploy or a new tab usually lands
on the other one); there is no way to pin a request to a replica, see the gaps below.

### 4. Two replicas on different machines

`/replicas` lists the container hostname of every replica and the version it runs, which is what the platform UI shows
per replica. During a rolling redeploy both the old and the new version appear until the old containers are gone. Check
the machine placement in the Meshbay UI: `meshbay.toml` has no key for placement or anti-affinity, so both replicas may
end up on the same machine.

### 5. Redeploy while a connection is open

Terminal 1:

```sh
node scripts/soak.mjs https://<domain> 120
```

Terminal 2: change `applicationVersion` in `src/config.ts` (`v1` → `v2`), commit, deploy.

Expected: the soak run logs `ws closed at ...: code 1006/1001` and `sse stream ended`, then exits 1 — the old
containers were replaced, which is the point. The page in a browser shows `closed (1006), reconnecting in 2s`, then a
new `hello` with the new version and, when there are two replicas, possibly the other replica. Run
`bash scripts/smoke.sh https://<domain>` afterwards: it passes on the new version, so the reconnect chain is intact.

### 6. Long connection through Caddy and Cloudflare (open question in MESH-135)

```sh
node scripts/soak.mjs https://<domain> 300      # exit 0 and "OK held 300 s" means both connections survived
curl -N https://<domain>/events                 # stop with Ctrl-C; ticks must keep arriving, no buffering
```

The WebSocket sends a heartbeat every 20 s (`HEARTBEAT_INTERVAL_MS`) because an idle socket is what proxies drop first.
The soak run reports the largest gap between SSE ticks; a gap far above 1 s means something buffered the stream.

### 7. Local-only check without Redis

```sh
REDIS_URL= bash scripts/smoke.sh http://localhost:8080
```

## Known Meshbay gaps

Collected while building this fixture. Each one is a candidate ticket, not something worked around here.

- **`meshbay.toml` has no environment interpolation.** `REDIS_URL` can only be set by hand after the Redis database
  exists; there is no `${{ redis.REDIS_URL }}` style reference, and the manifest `env` block rejects nothing but
  silently carries an empty string.
- **No key for replica placement or anti-affinity.** `replicas = 2` may put both replicas on one machine.
- **No key for a service's source subdirectory or watch paths.** One repository = one build context, so a service
  cannot point at `apps/x`. The repo keeps everything at the root because of it.
- **Path healthchecks render `curl -fsS -o /dev/null http://127.0.0.1:<port><path>` inside the container** and the
  Railpack Bun runtime image does not guarantee `curl`. This fixture uses the `command` form (`bun -e fetch(...)`)
  so the deploy is green; a project that needs a path healthcheck has to add `curl` through its own build config.
  Untested here: whether Meshbay honours a repository `railpack.json` (`deploy.aptPackages`).
- **Nothing pins a request to a replica.** No sticky cookie or header, so "same client, same replica" cannot be
  forced; the smoke test therefore reports which replicas it hit instead of asserting a distribution.
- **No drain period on redeploy.** Open WebSocket/SSE connections are cut when the old containers stop (verified
  LIVE, expect code 1006 from the client's side); the fixture only proves that clients reconnect on their own.
- **No ingress timeout / buffering knobs in the manifest.** SSE relies on `Cache-Control: no-transform` and
  `X-Accel-Buffering: no` from the app, and on Caddy and Cloudflare passing the stream through untouched.
- **No shared state between replicas by default.** The `items` CRUD contract of the other samples is deliberately
  absent here: with `replicas = 2` and no database, an in-memory `items` table would 404 on the replica that did not
  receive the `POST`. Add a Postgres database (like `meshbay-sample-go-api`) if that contract is wanted in this
  fixture as well.

## Docker

The repo ships a Dockerfile: `oven/bun:1.3.14-alpine` installs with `bun install --frozen-lockfile` (the committed
`bun.lock`), copies `src/`, drops to the `bun` user and starts `bun run src/index.ts`. The path healthcheck note above
does not apply here because the image is built by this repo, not by Railpack; the manifest keeps the `command` form.
