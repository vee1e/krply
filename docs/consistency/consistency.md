# Consistency

krply makes no claim that Kubernetes provides immutable history. It stores its own journal and always shows the truth about coverage. This document defines the consistency guarantees that consumers can rely on.

## Consistency model

| Area | Guarantee |
|---|---|
| Ordering | Ordered within one watch stream |
| Resource version | Meaningful only within one cluster and API resource |
| Ingestion | At least once with deduplication |
| Historical state | Reconstructable only from a complete baseline plus complete events |
| Cluster snapshot | A vector of per-stream watermarks, not one atomic snapshot |
| Timestamps | observed_at means collector observation time |
| Cross-cluster order | None |
| Audit identity | Only through optional audit-log correlation |
| Replay | Declarative state application, not exact side-effect reproduction |

Two consequences follow directly:

- There is no total order across resources. Two streams are ordered within themselves only. You cannot say that a Deployment change happened before a ConfigMap change unless they share a stream.
- The field observed_at is when the collector observed the event, not when the object changed. The UI shows both the source resource version and the observed time. It never labels observed time as changed time.

## Watch semantics

### Resource versions

- Store and compare resource versions as opaque strings.
- Compare them only within one cluster and one API resource. A Deployment resource version is not comparable with a Pod resource version.
- A resource version is not a global cluster clock.
- Extension API servers may use non-numeric resource versions. Never parse them as integers.

### The normal pattern

1. List a collection. Record the collection resource version.
2. Watch from that exact resource version.
3. Apply ADDED, MODIFIED, and DELETED to the journal.
4. Reconnect from the last durable resource version on stream close.

### Bookmarks

- Bookmarks are progress markers, not object changes.
- The API server may ignore the allowWatchBookmarks flag.
- A bookmark may advance the collector durable checkpoint safely.
- A bookmark must never appear as an object change in the UI or in diffs. Bookmark events persist as TypeCheckpoint records, not object events.

### Compaction and relists

- The API server retains watch history for a limited window. The default etcd history window is short. Its exact length depends on cluster configuration.
- On a 410 Gone response: write a GAP record, relist, store a new BASELINE, and watch again.
- A relist restores current state. It cannot reveal what changed during the gap. The gap remains visible in the journal and in query results.

### Streaming initial events

- The sendInitialEvents flag may send synthetic ADDED events for the existing collection.
- These events are a baseline snapshot, not creation events. Mark them synthetic.
- Keep the conventional list-plus-watch fallback.

## Ingestion guarantee

The journal write uses the write-event-then-checkpoint order:

1. Write the raw event first.
2. Update the checkpoint in the same transaction.
3. On restart, replay from the last checkpoint and deduplicate.

This gives at-least-once delivery with deterministic deduplication. If the process crashes between writing an event and its checkpoint, the event may be written again. The field event_id is a deterministic key derived from the stream, the resource, the watch type, and the observed time. This key makes re-application idempotent.

These behaviors are NOT guaranteed:

- Exactly-once network delivery. The API server can resend after a reconnect.
- A crash-safe guarantee that an event exists in the journal if the checkpoint does not reference it. Events that are written but not checkpointed may be redelivered.

## Watermarks

A cluster snapshot is not one atomic snapshot. It is a vector of per-stream watermarks. Each stream reports the last resource version that the collector processed durably. A snapshot therefore has:

- Per-stream boundaries, called watermarks.
- A completeness flag, called Complete.
- A list of Missing streams when any stream is incomplete or has an unresolved gap.

Query results and snapshots expose gaps and warnings. A partial result carries a warning. Never present a partial stream as complete.

## Timestamps

- The field observed_at is the collector observation time. The collector clock sets it.
- The source resource version is the server ordering token. The UI shows it alongside the observed time.
- Diffs and snapshots use observed_at as the key. Clock differences between the collector and the API server affect when a change is attributed. They never affect which changes are recorded.

## See also

- Watch semantics source: Kubernetes API concepts.
- Risks and their controls: ../threat-model/threat-model.md.
- Storage layout: ../architecture/architecture.md.
