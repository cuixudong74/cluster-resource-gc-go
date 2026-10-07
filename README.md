# Cluster Resource Garbage Collector

A dependency-free Go implementation that identifies unreachable cluster
resources from a local snapshot and safely reclaims them. The default local
workflow does not require a real cluster, credentials, or external network
access. A mutable in-process source backed by the snapshot is used for local
execution and automated tests.

## Safety model

1. **Dry run first.** Every execution computes and atomically commits a plan
   before mutating anything. The executor only performs actions whose exact
   item and idempotency key are present in that committed plan.
2. **Root reachability.** Objects marked `root: true` are live roots. The
   analyzer traverses explicit references and owner relationships iteratively,
   so reference cycles terminate and never cause infinite recursion.
3. **Owner semantics.** Snapshot owner references are recorded as child-to-owner
   edges. The analyzer also derives owner-to-dependent reachability, so a live
   owner keeps all existing dependents reachable.
4. **Dangling references are visible.** A reference to an absent target is kept
   in `danglingReferences`; the holder receives `dangling-reference` and is
   routed to manual confirmation instead of automatic deletion.
5. **Protection is absolute.** Explicit IDs, the protection label/annotation,
   creation grace periods, and configured selectors override garbage status.
   Protected objects are emitted as skips with explicit reasons.
6. **Authorization happens before deletion.** Unauthorized delete requests are
   denied before the resource source is contacted, persisted as `denied`, and
   written to the append-only audit log.
7. **Recovery is checkpoint based.** `attempting` is uncommitted and is retried
   using the same idempotency key; `deleted` is committed and is skipped. A
   resumed plan is refused if the current snapshot fingerprint changes.
8. **Deletes are atomic at the source contract.** The system does not write a
   committed `deleted` marker until the source operation succeeds. Cancellation
   between source deletion and marker commit is recovered by an idempotent
   source replay that reports `AlreadyDeleted`.

## Reclaim categories

| Classification | Plan result | Meaning |
| --- | --- | --- |
| `reachable-from-root` | skip | Reachable from a live root, including cycles shared with a live component |
| `unreachable-from-root` | delete or skip | No live-root path exists |
| `only-referenced-by-garbage` | delete if otherwise auto-safe | Every inbound referencer is itself unreachable |
| `dangling-reference` | skip/manual | The object points to a missing target; a human must confirm |
| `long-idle` | skip/manual | Last usage is older than `policy.idleThreshold`; inactivity alone is weaker than reachability |
| `manual-confirmation-required` | skip | The resource is not auto-safe and was not included in `approvedManual` |
| protection reasons | skip | Explicitly protected, marked, in grace, or matched by a rule |

Manual items can be approved for a planned run through `approvedManual` or the
CLI `-approve` flag. Approval only affects the plan fingerprint in which it is
included; it does not grant authorization. The principal still needs the
`gc-admin` delete role.

## Snapshot format

```json
{
  "generatedAt": "2026-01-02T00:00:00Z",
  "objects": [
    {"id": {"kind": "Namespace", "name": "live"}, "root": true},
    {
      "id": {"kind": "ConfigMap", "namespace": "live", "name": "active"},
      "owner": {"kind": "Namespace", "name": "live"},
      "labels": {"app": "demo"},
      "createdAt": "2026-01-01T00:00:00Z",
      "lastUsedAt": "2026-01-01T23:00:00Z"
    }
  ],
  "references": [
    {
      "from": {"kind": "ConfigMap", "namespace": "live", "name": "active"},
      "to": {"kind": "Secret", "namespace": "live", "name": "token"},
      "relation": "secret-ref"
    }
  ]
}
```

Object IDs are JSON objects with `kind`, optional `namespace`, and `name`.
Using the JSON form internally avoids ambiguity when names contain `/` or `:`.

## Configuration

See [`examples/config.json`](examples/config.json).

| Key | Description |
| --- | --- |
| `source` | `memory-file` for local mutable simulation, or `file` for read-only dry runs |
| `snapshotPath` | Required local JSON snapshot path |
| `stateDir` | Directory for `<plan>.json` and `<plan>.events.jsonl` |
| `auditPath` | Append-only JSONL audit trail |
| `dryRun` | Generate and save a plan without source deletion |
| `resumePlanId` | Resume a previously committed plan ID |
| `concurrency` | Maximum delete workers in a batch |
| `batchSize` | Items processed per concurrent batch |
| `maxAttempts` | Retry attempts per item; retries use the same idempotency key |
| `requestsPerSecond` | Process-wide delete rate limit |
| `timeout` | Overall wall-clock timeout |
| `principal.name`, `principal.roles` | Authenticated local caller and RBAC roles |
| `policy.gracePeriod` | Protect newly created objects for this duration |
| `policy.idleThreshold` | Age since non-zero `lastUsedAt` that marks long-idle resources |
| `policy.protectedIDs` | Explicit non-deletable object IDs |
| `policy.protectedLabel` | Truthy label value that protects an object |
| `policy.protectedAnnotation` | Truthy annotation value that protects an object |
| `policy.rules` | Kind/namespace/label selector rules with optional `minAge` |
| `approvedManual` | Manual-category object IDs explicitly approved for deletion |

Rule selectors currently support exact `kinds`, `namespaces`, and label
key/value pairs. All reasons are copied into each plan item.

## CLI usage

Dry run:

```sh
go run ./cmd/gc -config examples/config.json
# or
go run ./cmd/gc -snapshot examples/snapshot.json -state-dir .gc-state -mode dry-run
```

Execute with a named authorized principal:

```sh
go run ./cmd/gc \
  -config examples/config.json \
  -mode execute \
  -principal local-operator -roles gc-admin
```

Approve one manual item:

```sh
go run ./cmd/gc -snapshot examples/snapshot.json -mode execute \
  -principal local-operator -roles gc-admin \
  -approve ConfigMap/old/orphan
```

Resume after interruption:

```sh
go run ./cmd/gc -snapshot examples/snapshot.json -mode execute \
  -principal local-operator -roles gc-admin \
  -resume plan-xxxxxxxxxxxx
```

## Determinism and state files

The plan ID and input fingerprint are SHA-256 hashes over normalized objects,
references, policy, and approved IDs. Wall-clock time is not introduced when
the snapshot time is absent. The same input and policy produce identical items,
order, plan ID, and per-item idempotency keys across repeated runs.

- `<stateDir>/<plan>.json`: atomically committed plan.
- `<stateDir>/<plan>.events.jsonl`: append-only `attempting`, `deleted`,
  `failed`, and `denied` transitions.
- `<auditPath>`: authorization decisions, every source attempt, delete results,
  failures, denials, and recovery/replay outcomes.

`attempting`, `failed`, and `denied` are not committed completions:

- `attempting` is retried on restart with the same idempotency key.
- `failed` can be retried in a later run after the transient condition clears.
- `denied` remains traceable forever but a later run with an authorized
  principal re-enters authorization rather than treating the object as handled.
- `deleted` is the only completion marker that allows an object to be skipped
  without another source operation.

## Replacing the resource source

Implement the small `Source` interface to add a real Kubernetes adapter:

```go
type Source interface {
    Load(context.Context) (*Snapshot, error)
    Delete(context.Context, DeleteRequest) (DeleteResult, error)
}
```

A production adapter should use server-side atomic deletion semantics,
authorization from the runtime environment, and make repeated `Delete` calls
with the same idempotency key return `AlreadyDeleted` instead of failing as a
second deletion. No existing code imports a Kubernetes client or contacts the
network.

## Tests

```sh
go test ./...
go test -race -count=1 ./...
go vet ./...
```

Tests cover live/garbage cycles, owner reachability, dangling references,
long-idle manual classification, all protection types, deterministic plans,
concurrent batching, idempotent retries, unauthorized denial/audit, interrupted
recovery, committed-object replay, and refusal to resume against changed input.
