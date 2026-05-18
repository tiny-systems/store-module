# Tiny Systems Store Module

Embedded, persistent storage components for Tiny Systems flows. The
backing store is [bbolt](https://github.com/etcd-io/bbolt) — pure Go,
single-file, transactional, used inside etcd itself. Data lives on a PVC
mounted inside the operator pod so it survives pod restarts.

## When to use this vs. alternatives

| Use case | Reach for |
|---|---|
| Small in-component state (counters, last-seen, port runtime data) | SDK `State` via `module.Base.State()` — protected by `MaxStateBytes = 900KB` since SDK v0.10.9 |
| Per-flow persistence above ~1MB (chat history, agent scratchpads, retrieval caches) | **document_store** (this module) |
| Shared persistence across flows / HA requirements | `postgres_*` / `redis_*` in `database-module-v0` |

`document_store` is the "no external infra" path. One container, one
file, one PVC. Configure once and any flow can use it via standard
edges.

## Components

### `document_store`

Embedded KV store with per-collection buckets. Four operation ports
(`put`, `get`, `delete`, `find`) each with a matching source result port.

**Settings**

| Field | Type | Notes |
|---|---|---|
| `path` | string | Absolute path to the bbolt file. Default `/data/store.db`. Mount a PVC at this directory. |
| `collections` | `[{name}]` | Named buckets. Writes to undeclared collections fail. At least one required. |
| `maxSizeMB` | int | Soft cap on file size. Puts above this route to error port (`diskFull: true`). Default 1024. |
| `enableErrorPort` | bool | Route operational failures (disk full, missing collection, marshal errors) to the error port. |

**Ports**

Input (target):

- `put` — `{context, collection, key, value}` → emits on `put_ok`
- `get` — `{context, collection, key}` → emits on `get_ok` with `{value, found}`
- `delete` — `{context, collection, key}` → emits on `delete_ok` with `{deleted}` (false if key was absent)
- `find` — `{context, collection, prefix?, limit?}` → emits on `find_ok` with `{items: [{key, value}], count}`

Output (source): `put_ok`, `get_ok`, `delete_ok`, `find_ok`, and (when
enabled) `error` with optional `diskFull: true`.

Values are stored as JSON. Any JSON-serialisable Go value works —
strings, numbers, objects, arrays, nested structures.

## Deployment patterns

bbolt is single-writer. The .db file has an exclusive OS-level lock,
so the deployment shape determines availability:

### 1. Single replica (default) — RWO PVC, `replicas: 1`

Simplest. One pod owns the file. Failure modes:

- **Pod crash / restart on same node**: k8s reschedules, PVC remains
  attached, new pod opens bbolt within ~5-10s.
- **Node failure**: PVC detaches from dead node and attaches to a new
  one — typically 30-90s depending on storage class. During this
  window all requests fail; flow authors should route store errors
  through a retry component.
- **Storage class matters**: GKE `standard-rwo` typically detaches in
  ~30s; some CSI drivers are slower. Test your failure mode.

This is what `Settings.LeaderOnly = false` configures. Good enough for
single-tenant clusters, internal tooling, demos.

### 2. Leader-only mode — RWX PVC, `replicas: N`

For deployments that want fast failover and can pay for RWX storage.
Set `Settings.LeaderOnly = true`. Behaviour:

- All N pods come up; SDK leader-election picks one.
- Leader opens bbolt, holds the file lock, serves requests.
- Followers refuse with `Retryable: true` on the error port.
- Leader dies → election fires (~10s) → new leader retries
  `bbolt.Open` for up to 30s until the prior leader's lock releases
  (clean death is instant; dirty death waits for NFS/CSI cleanup).

Tradeoffs:

- **RWX storage (Filestore / EFS) is ~10× the cost of block storage**
  and slower per-op. The whole point of bbolt was cheap embedded
  storage; using RWX moves you partway toward "just use postgres."
- **Followers don't serve reads.** Single writer, single reader. The
  extra replicas are warm standbys, not horizontal scaling.
- **Load balancer doesn't know about leadership.** Requests that land
  on a follower fail with `retryable=true`. Callers (or a Kubernetes
  Service with a smarter health check) need to handle the retry.

### 3. External storage — `postgres_*` / `redis_*` from database-module-v0

When you need real horizontal scaling, multi-region, or sub-second
failover. The components are stateless clients; the database does
HA at its layer. Same flow shape — swap `document_store.put` for
`postgres_exec`, `document_store.get` for `postgres_query` — and the
LLM never knows the difference.

## Other notes

**PVC required.** Without persistent storage at `Settings.path`, all
data is lost on pod restart. The Helm chart for `tinysystems-operator`
supports volume mounts — point a PVC at `/data` (or wherever `path`
lives) when installing. For LeaderOnly mode, the PVC must be RWX.

**Lock contention messages.** When `bbolt.Open` times out (30s
ceiling), the error message specifically names "another pod may be
holding the file lock" so the operator knows where to look. Common
causes: a Pending pod with the same PVC, a stuck NFS client on a dead
node, or a developer running `go run cmd/main.go` against the same
.db while a pod is also running.

**Backup story.** bbolt is a single file. Copy `path` while the pod
is quiesced. Scheduled backup is out of scope for v1.

## Pattern: persistent chat history

```
http_server.request → json_decode → document_store.get (conversation key)
                                  → join messages + new user turn
                                  → llm_chat (stateless)
                                  → document_store.put (conversation key)
                                  → http_server.response
```

`llm_chat` stays stateless and reusable — the flow composes
persistence around it. Swap `document_store` for `kv` (in
`common-module-v0`) for small histories, or `postgres_exec` when you
need HA.

## Run locally

```shell
mkdir -p /tmp/store-module-data
go run cmd/main.go run \
  --name=tinysystems/store-module \
  --namespace=tinysystems-tinysystems \
  --version=0.1.0
```

Bind `/data` (or your configured `Settings.path` directory) to a
writable host path when running outside Kubernetes.

## Deploy

```shell
docker build -t myregistry/store-module:0.1.0 .
docker push myregistry/store-module:0.1.0

helm install store-module tinysystems/tinysystems-operator \
  --set controllerManager.manager.image.repository=myregistry/store-module \
  --set persistentVolume.enabled=true \
  --set persistentVolume.mountPath=/data \
  --set persistentVolume.size=10Gi \
  --set controllerManager.replicas=1
```

See `helm get values tinysystems-llm-module-v0` (or any other module)
for the existing Helm shape — the operator chart is shared across all
modules.

## License

MIT for this module's source. Depends on the [Tiny Systems Module SDK](https://github.com/tiny-systems/module) (BSL 1.1) and [bbolt](https://github.com/etcd-io/bbolt) (MIT).
