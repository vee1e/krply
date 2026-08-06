# Event schema

Every journal record is an immutable entry. The wire format is api/event/v1. This document describes the fields, the record types, and how keys and hashes are derived.

## Record fields

| Field | Purpose |
|---|---|
| cluster_id | Separates resource version and UID domains; one per physical cluster |
| stream_id | Identifies group, version, resource, namespace, and selector |
| event_id | Deterministic deduplication key |
| ingest_seq | Local storage order (ascending); returned in this order |
| observed_at | Collector observation time |
| watch_type | Original event type (ADDED, MODIFIED, DELETED, BOOKMARK, ERROR) |
| synthetic | True for baseline events |
| resource | Group, version, kind, namespace, name, UID, resource version |
| object_hash | Fast equality and deduplication |
| object | Raw or faithfully preserved payload (JSON) |
| provenance | Optional audit correlation |

The raw watch payload is kept separate from normalized fields. This preserves unknown CRD fields. Never re-serialize the object into a lossy normalized shape.

## Record types

The journal stores event records plus these special records:

| Type | Meaning |
|---|---|
| event | A watch event (ADDED, MODIFIED, DELETED) |
| baseline | A list result (initial or post-relist) |
| gap | Continuity was lost (for example, a 410 Gone response) |
| coverage_change | A resource became unavailable or was newly discovered |
| audit_correlation | Optional request provenance matched from audit logs |
| snapshot | A materialized set of stream boundaries |
| checkpoint | Progress marker advanced by a bookmark |

The corresponding record type constants in internal/event are: TypeEvent, TypeBaseline, TypeGap, TypeCoverageChange, TypeAuditCorrelation, TypeSnapshot, and TypeCheckpoint. The watch types are: WatchAdded, WatchModified, WatchDeleted, WatchBookmark, and WatchError.

## event_id derivation

The field event_id is the deterministic deduplication key. The derivation is:

```
EventID(stream, resource, watchType, observedAt) -> string
```

The inputs are the stream identity, the resource reference, and the watch type. For live watch events the observed time is deliberately excluded: the same underlying API event must always produce the same key, so a duplicate delivery after a reconnect is idempotent. Collector-generated synthetic baselines pass the list observation time, so an unchanged object re-listed after a gap is treated as a new observation rather than a duplicate and survives deduplication. See ../consistency/consistency.md for the ingestion guarantee.

## object_hash

The function ObjectHash(objectJSON) returns a fast equality hash over the raw object payload. It lets consumers detect that a MODIFIED event did not change the object. It is not a cryptographic commitment. Treat it as a performance and equality helper only. Deduplication is by event_id, not by object content.

## Provenance

The field provenance is optional. When audit logs are available, an audit event is correlated to a stored event by cluster, namespace, name, UID, and resource version. It is recorded as a TypeAuditCorrelation record that references the field event_id. Audit identity is available only through this optional correlation. A watch event itself never contains the actor who made the change.

## Versioning

The package api/event/v1 is the public wire format. Internal packages stay private until the schema and replay contract stabilize. The manifest used by the object-storage segment layout records a schema version alongside the redaction policy version. Do not change api/event/v1 without a coordinated schema bump. Consumers of the journal and replay contract depend on it.

## See also

- Ingestion and deduplication: ../consistency/consistency.md.
- Storage layout and segment manifests: ../architecture/architecture.md.
- Sanitization of the object payload before replay: ../replay-safety/replay-safety.md.
