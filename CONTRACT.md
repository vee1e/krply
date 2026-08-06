# Cross-package contracts

This file pins the exported signatures that parallel development builds against. Do not change a contract without updating this file and every consumer. Internal implementations may change freely.

## Module

The module is github.com/krply/krply. It uses Go 1.26. It has no cgo. The pre-loaded dependencies are modernc.org/sqlite, k8s.io/api, k8s.io/apimachinery, k8s.io/client-go, sigs.k8s.io/yaml, github.com/spf13/cobra, github.com/prometheus/client_golang, and github.com/google/uuid. Do not add new dependencies without coordinating.

## internal/event (DONE)

- type Record. An immutable journal entry with the fields Type, EventID, IngestSeq, ObservedAt, WatchType, Synthetic, Resource (a ResourceRef), ObjectHash, Object (a json.RawMessage), Provenance, Gap, Coverage, Checkpoint, and Snapshot.
- type RecordType. The constants are TypeEvent, TypeBaseline, TypeGap, TypeCoverageChange, TypeAuditCorrelation, TypeSnapshot, and TypeCheckpoint.
- type WatchType. The constants are WatchAdded, WatchModified, WatchDeleted, WatchBookmark, and WatchError.
- type ResourceRef. It has the methods ObjectKey() and GVK(), and the fields Group, Version, Kind, Namespace, Name, UID, and ResourceVersion.
- type Stream with the fields ClusterID, Group, Version, Resource, Namespace, and Selector, and the method ID().
- StreamID(string) (Stream, error).
- EventID(stream, resource, watchType, observedAt) string. It is the deterministic dedup key.
- ObjectHash([]byte) string.

## internal/storage (interface DONE, implementation pending)

- type Store interface. The methods are Append, Appends, ListClusters, Streams, StreamMeta, Events(EventFilter), ObjectHistory(ObjectRef), ObjectAt, StreamEvents, Baselines, Gaps, SaveSnapshot, Snapshots, and Close.
- The package also exports StreamMeta, EventFilter, ObjectRef, and SnapshotRef.
- The constructors are NewSQLiteStore(path string) (Store, error) and NewInMemory() Store. Both must exist.
- SQLite must use WAL mode. It must write records and the checkpoint atomically.
- Implementations return events in ingest order. The field IngestSeq ascends.

## internal/discovery (implementation)

- type ResourceSpec with the fields APIGroup, Version, Resource, Kind, and Namespace.
- Discover(ctx, client) resolves well-known aliases such as deploy, sts, ds, and cm to full specs. It fills missing versions and kinds through ServerResources. An empty Namespace means cluster scope.
- DefaultResources() returns the MVP watch list. The list is namespaces, configmaps, services, deployments, statefulsets, daemonsets, runtimeclasses, and pods.

## internal/watch (implementation)

- type Config with the fields KubeConfig, Context, ClusterID, Resources (a slice of discovery.ResourceSpec), Selector, Store (a storage.Store), Log (a *slog.Logger), Bookmarks, SendInitial, AgentName, MinBackoff, and MaxBackoff.
- NewCollector(cfg Config) (*Collector, error).
- Run(ctx) error runs all streams.
- ClusterID(ctx, kubeconfigPath, context) derives a stable cluster identity from the kubeconfig. It hashes the server URL.
- Watch semantics: start from the exact resource version. On a close, reconnect from the last durable resource version. On a 410 Gone response, write a GAP record, relist, and write a BASELINE. Write every raw event through store.Append. Bookmarks advance checkpoints only, as TypeCheckpoint. Send an initial BASELINE from the first list. Mark events in the initial list as Synthetic.

## internal/materialize (implementation)

- type ObjectState with the fields ClusterID, StreamID, Namespace, Name, Kind, Object (a json.RawMessage), and At (a time.Time).
- type Snapshot with the fields ID, ClusterID, Name, At, Objects, Streams, Complete, Missing, and Warning.
- type Materializer. The constructor is NewMaterializer(store storage.Store).
- ObjectAt(ctx, clusterID, streamID, namespace, name, at) (*event.Record, error).
- Snapshot(ctx, clusterID, at, name) (*Snapshot, error). It materializes every watched object, records per-stream watermarks, stores a TypeSnapshot record, and marks incomplete streams.
- Diff(ctx, clusterID, namespace, before, after) (*DiffResult, error). DiffResult has the fields Before, After, Changes, HasGaps, and Warning. ObjectDiff has the fields Namespace, Name, Kind, and Changes. FieldChange has the fields Path, Before, After, Added, and Removed.
- The field diff must ignore managedFields, resourceVersion, uid, generation, creationTimestamp, and other server metadata.

## internal/replay (implementation)

- type Policy with the allowlist fields IncludeSecrets, IncludeRoles, IncludePods, IncludeJobs, IncludePV, IncludeWebhooks, IncludeCRDs, AllowFinalizers, and MapNamespaces.
- type PlanObject with the fields Namespace, Name, Kind, Order, Object (a map[string]any), and Warnings.
- type Excluded with the fields Namespace, Name, Kind, and Reason.
- type Plan with the fields ID, ClusterID, SnapshotID, SourceNamespace, TargetNamespace, TargetContext, FieldManager, Objects, Warnings, Excluded, CoverageComplete, and Status.
- type Planner. The constructor is NewPlanner(store, mat, policy).
- Plan(ctx, clusterID, snapshotID, sourceNS, targetNS) (*Plan, error).
- Sanitization defaults follow docs/replay-safety/replay-safety.md. Remove uid, resourceVersion, creationTimestamp, generation, managedFields, deletionTimestamp, status, ownerReferences (except approved), and finalizers (unless allowlisted). Remove Service clusterIP fields. Exclude Secrets, RBAC, Pods, Jobs, PVs, webhooks, and CRDs by default.
- DryRun(ctx, kubeconfig, context) (*DryRunResult, error).
- Apply(ctx, kubeconfig, context, confirm) (*ApplyResult, error).
- type DryRunResult with the fields Applied, Conflicts, Errors, and OK.
- type ApplyResult with the fields PlanID, Applied, and Errors.
- type DryRunItem with the fields Namespace, Name, Kind, Manager, and Message.
- Use SSA with the field manager krply-plan-<planID>. Never force. Run the dry run first.
- Plans refuse incomplete coverage unless the Policy field AllowGaps is true.

## internal/audit (implementation)

- type AuditEvent with the fields ClusterID, RequestID, Verb, Resource, Namespace, Name, UID, ResourceVersion, User, UserAgent, SourceIPs, ResponseCode, Stage, Object, ResponseObject, Annotations, and Timestamp.
- type Correlator. The constructor is NewCorrelator(store).
- Ingest(ctx, events) error. It matches stored events best-effort by cluster, namespace, name, uid, and resourceVersion. It appends TypeAuditCorrelation records.
- Match(ctx, evt) (string, error). It returns the event_id or ErrNoMatch. Export var ErrNoMatch = errors.New(...).

## internal/metrics (implementation)

- Expose Prometheus metrics for ingest counts, gaps, degraded streams, and storage bytes. NewRegistry(store) returns the registry.

## internal/api (implementation)

- NewServer(store, mat, plan, reg) (*Server, error).
- Handler() http.Handler. The routes are GET /v1/health, /v1/clusters, /v1/streams, /v1/events, /v1/objects/{ref}/history, /v1/diff, /v1/snapshots, POST and GET /v1/replay-plans, POST /v1/replay-runs, and /metrics.
- Use the JSON types in api/query/v1. Every historical query returns gaps and warnings.

## cmd/krply-server

- The main function builds the store, materializer, planner, and api server. It serves on the address from the listen flag, default :8080. Flags: store PATH, listen ADDR, and version.

## cmd/krply (CLI, cobra)

- Commands: record, status, coverage, timeline, diff, snapshot, reconstruct, replay plan, replay apply, and export.
- Flags include context, namespace, resource, store, since, target-context, plan-id, confirm, and others.
- The record command uses the internal/watch Collector. All other commands read the local store directly. No server is required. The server URL flag switches to the HTTP client.

## web/

- A vanilla static UI lives in web/src. krply-server serves it at the root path. Views: coverage matrix, stream health, object timeline, JSON diff, gap markers, and replay review. The npm run build command outputs to web/dist. No framework dependency is required. A lightweight build such as esbuild or vite is acceptable if the config is committed.

## deploy/

- The deploy/rbac directory has the recorder ClusterRole with get, list, and watch on specific resources and no secrets, the replay ClusterRole with create and patch only, and a kustomization.
- The deploy/compose/docker-compose.yml file runs krply-server and an optional agent.
- The deploy/helm/krply directory has a minimal chart with values.yaml, Chart.yaml, and templates.

## tests

- test/unit has small Go tests that exercise the exported packages. You may run go test on the packages directly instead.
- test/integration has the tag integration. It uses a fake apiserver, k8s.io/apimachinery, and httptest. It covers 410 relist, bookmark handling, disconnects, exact-RV watch start, and RBAC denial.
- test/e2e has the tag e2e. It runs the full record, timeline, snapshot, replay-plan flow against a fake apiserver. No real cluster is needed.
