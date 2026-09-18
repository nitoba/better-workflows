# Architecture and reliability

## Boundaries

The public entry point contains Nest decorators, dynamic modules, typed clients and Standard Schema contracts. Workflows and activities support a simple form where contract and handler are the same class, plus optional contract-first forms where `@WorkflowContract`/`@ActivitiesContract` are metadata-only and `@Workflow(contract)`/`@Activities(contract)` are the Nest-executable implementations. Contract-first activity metadata owns method schemas, names, versions and policies; Nest providers and infrastructure dependencies remain on the worker side. The declaration graph does not expose Effect types. The implementation pins Effect and its platform/SQL adapters to **4.0.0-rc.115**, because workflow/cluster APIs are unstable.

Nest discovers explicit feature registrations after resolving their factories and dependencies. `forFeature` registers implementations; clients-only and activity-contract-only entries do not construct handlers. Workflow and activity registrations are normalized by contract class before the durable catalog is built, so JavaScript handler names never participate in identity. The resolved catalog tracks ownership, validates imports/exports and freezes final policy objects before infrastructure or workers start. Queue policies no longer depend on a global root catalog. `useExisting` binds a normally imported Nest instance without a duplicate or a global service locator. Context objects are per invocation; workflow replay code must not depend on mutable singleton state.

Queue references persist their explicit names, not feature names or class/file names. Private queues can be hidden behind exported activity contracts. Calls always use owner-resolved defaults. Queue capacity is shared across handlers/features for one logical queue; application-owned activity deliveries route by logical queue and retain contract name/version in their JSON envelope. Root execution restrictions and feature restrictions intersect. See [modular registration](modules.md).

An internal dispatcher bridges an application's async handler to the workflow's own Effect fiber. Durable commands execute on that fiber, where the engine can suspend correctly. Suspension interrupts the interpreter, invalidates the async round and abandons unresolved awaits without rejecting them into application code. Abandoned promises are not retained in a shared forever-pending promise. Application-created timers, global references or arbitrary background work can still retain its own objects; the library is not a JavaScript sandbox or a compiler enforcing determinism.

## What is persisted where

Effect's cluster/message storage owns replayable workflow results, deferred results, timers and workflow ownership. The application-owned activity transport persists queue deliveries, leases, delivery attempts and dead-letter records. The additional journal owns acceptance/deduplication, observable command signatures, history, control requests, FIFO signal consumption, business retry deadlines, fenced activity-result receipts and the engine-result reconciliation outbox.

`start()` commits a run and pending dispatch state in one journal transaction. Dispatch is an outbox: sending to the engine can repeat after a crash; the engine's deterministic execution identity deduplicates it. A completed engine result is recorded in the reconciliation outbox and then committed to the run journal; this removes the need to poll every live execution. Signal/deadline delivery similarly persists the decision before completing the engine deferred, then marks it delivered. Signal waits are explicitly woken by signal acceptance; timeout-only waits enter the outbox only when their deadline is due. Delivery can safely repeat. A slow active-run sweep remains only as a recovery safety net for the crash window before a result outbox row is written.

`WorkflowHandle.result()` follows the same work/wake-up model. It reads the current `event_sequence` revision, registers a local waiter before its second revision read, and sleeps until a journal event, PostgreSQL `LISTEN` notification or the low-frequency 5-second safety fallback. The notification payload contains only execution ID and revision; the database remains the source of truth. PostgreSQL publishes with `pg_notify` inside the journal transaction, and a shared listener connection wakes all result waiters in that runtime. Reconnects wake every waiter so each one rechecks its execution.

Journal keys distinguish workflow name/version, workflow idempotency, logical step identity, business attempt and infrastructure delivery. In particular, changing a deployment version does not silently create a second execution for the same start key. The old handler and its activity contracts must remain available while old executions need them. This release detects observed command mismatches; it cannot prove all TypeScript code deterministic.

## Retry/ownership protocol

Activity dispatch payloads snapshot timeout and retry policy. A business failure and the next retry's database-clock deadline commit in the same SQL transaction. The orchestrator waits for that persisted deadline's deferred; replay does not add another full backoff interval.

A worker leases an application-owned delivery and acquires a journal claim tied to its monotonically increasing queue delivery attempt and a fresh ownership token. Heartbeats/lease renewal and result commits check the current token, live lease and running state. An old invocation cannot commit after a newer delivery fences it, or after its lease expires. A successful receipt can be redelivered to the engine without repeating the handler.

SQL/infrastructure defects and runtime interruption are not serialized as successful queue processing containing a business failure. They cause infrastructure redelivery until the configured delivery limit, after which the delivery is dead-lettered for administration. Explicit `ActivityError` values represent business outcomes. External effects still need provider-side idempotency: no transaction spans an arbitrary remote service and this journal.

Timeouts and cancellation abort the handler's signal. JavaScript cannot forcibly stop arbitrary non-cooperative I/O or CPU loops in that process. A timed-out handler that ignores its signal may continue externally even though its result cannot be committed. Use abort-aware dependencies and separate worker processes for CPU-intensive or untrusted execution.

## Signals and controls

A database row lock serializes signal acceptance, FIFO consumption and timeout decisions. An event accepted at or before a wait's deadline wins over its timeout even when polling is delayed. The consumed event and wait outcome are durable. Duplicate event keys compare payloads; a conflicting payload is rejected. Input validation uses the execution's registered signal contract, not merely a caller-supplied schema with the same name.

Pause is a cooperative command-boundary control, not an external-process freeze. A currently running activity may complete while paused. Cancel is not compensation. Requests are stored durably, may briefly appear as `cancelling`, and are reconciled with the engine. Graceful Nest shutdown stops this runtime without cancelling the persisted executions.

## Storage/operations

Production journal deadlines, activity-delivery leases and claim leases use database time. Only the testing module substitutes business time for timers, retries and signal deadlines; leases always retain database time. Default leases are 30 seconds, renewed every 10 seconds; custom refresh must be positive and at most one third of the lease duration. Extremely short test leases are not production recommendations.

SQLite is an embedded single-process deployment. Distributed topology uses PostgreSQL plus the Effect socket cluster; changing a connection string alone is not cluster configuration. Runner sockets need private-network access and valid advertised addresses. Local administrative connections and runtimes referencing the same canonical SQLite file share the driver and its asynchronous transaction semaphore; reference-counted ownership closes it only after the last owner disposes. In-memory test stores remain isolated. SQLite result notifications are process-local and best-effort; the revision read plus fallback preserves correctness. Local queue slots are per process. Optional global/per-key admission uses SQL row locks and leased permits shared by namespace and queue. Expired owners cannot renew or commit through an old permit. This is a limit on current leased ownership, not a mechanism to kill non-cooperative external work after a process pause. Database credentials, application authorization, payload sensitivity, logging, backups and capacity planning remain deployment responsibilities.

Migrations are forward-only and transactional. The standalone admin API and CLI apply the journal and pinned native engine schemas without starting workers. Bootstrap can run them or validate first. The journal rejects newer/gapped ledgers and missing previously applied journal structures. Version 2 rebuilds the command ordinal index to include scope, preserves v1 command identities and records their timer protocol explicitly. Version 3 adds the signal wake/deadline outbox markers without scanning dormant waits. Version 4 adds the engine-result reconciliation outbox. Version 5 adds continuation-chain metadata. Version 6 adds the application-owned activity-delivery and dead-letter tables. Version 7 adds schedule definitions and occurrence history; version 8 adds persisted static inputs and manual-trigger idempotency keys; version 9 separates manual and recurring occurrence identity so both may share the current business-clock timestamp; version 10 adds durable schedule ownership leases; version 11 adds optional persisted trace-parent context without changing workflow identity. Existing v1 timers continue through the native DurableClock protocol; newly recorded timers use a journal deadline and deferred outbox so test business time can advance them.

Retention previews terminal executions before a cutoff. Application rechecks locks, active parent/child links, live claims/permits, unacknowledged activity deliveries, open/requeued dead letters and pending engine messages. Removal covers that execution's journal, activity transport and native message/reply records in one transaction, with namespace isolation. A tombstone containing the key and input hash prevents the execution from being silently recreated. Tombstones are retained indefinitely; there is no unbounded-duplicate resurrection, automatic archival scheme or file compaction. Use backups and the API, not manual table deletion.

## Durable scheduling and operational state

`@Cron` and `@Interval` metadata is normalized onto the workflow contract, never the
concrete handler. The registry is immutable after discovery. A schedule name is the
stable namespace identity; its definition hash covers the timeline, timezone, policies,
input mode and workflow contract version. A changed hash fails bootstrap with
`SCHEDULE_DEFINITION_CHANGED` instead of silently moving a cursor. Missing definitions
remain as `orphaned` rows and are not deleted by deployment; client-only and
activity-only processes do not claim schedule ownership. An operator may remove a
definition only after pausing it (or for an orphan) with explicit confirmation; occurrence
history is retained.

Cron definitions without an explicit timezone use UTC. Implementation ownership is
durable: a runtime renews a lease for each schedule it registers. Expired owners can
be reconciled as orphaned, while a live owner in another partial deployment protects
its schedule from removal. Ownership reconciliation continues during runtime so a
rolling deployment eventually orphans a removed schedule after its old owner stops.

Schedule definitions own a persisted cursor and each materialized occurrence has a
unique `(namespace, schedule_name, sequence)` identity; recurring rows additionally
have a unique scheduled timestamp. A scheduler pass selects a
bounded due batch, claims a row with database-time fencing, then records the occurrence,
accepts the workflow and advances the cursor in one SQL transaction. A crash before
commit retries the transaction; a crash after commit reuses the deterministic schedule
occurrence idempotency key. SQLite is single-process; PostgreSQL permits concurrent
schedulers without duplicate starts.

`skip`, `latest` and bounded `catch-up` operate on the persisted timeline, while
`allow` and `skip` control active workflow overlap. Continuation chains are followed
when evaluating overlap. Pausing only stops new occurrences; resume leaves the cursor
in place and reapplies the configured misfire policy. Manual triggers are separate
occurrences and never move the recurring cursor; their optional idempotency keys are
durable. Input resolvers are synchronous, schema-validated and excluded from all
telemetry. Execution retention keeps occurrence metadata but clears links to deleted
executions, while retaining the idempotency tombstone/key reservation.

The scheduler is part of the dispatcher loop but has its own readiness state and
staleness timestamp. It polls indexed due rows rather than creating one in-memory timer
per definition. `schedule.tick` and `schedule.trigger` spans, bounded schedule metrics,
and structured logs expose materialization, skip, misfire, catch-up and failure events;
schedule names, workflow identities, type, trigger and policies are the only schedule
dimensions permitted on metric series. Occurrence timestamps, sequence numbers,
execution IDs, idempotency keys and inputs remain span/log-only or durable storage.

## Observability and operational state

The public `better-workflows/observability` subpath configures optional OTLP/HTTP
export. Traces are enabled by default when configured; metrics and logs are opt-in.
The internal Effect layers share the runtime's metric registry, while exporter setup,
network failures and bounded shutdown flushes remain outside durable workflow
transactions. Resource attributes identify the service, package version, namespace,
topology, storage driver and process role without including database URLs or headers.

The telemetry vocabulary separates safe metric dimensions from diagnostic attributes.
Metrics use contract names, versions, logical queues and bounded reason/status values;
execution IDs, step IDs, delivery attempts, dead-letter IDs and idempotency keys never
become metric labels. Structured logs and short-lived spans may retain diagnostic
correlation, but they do not automatically record workflow/activity/signal results,
payloads, heartbeat details or authorization data. User resource attributes are static
strings, numbers or booleans; callbacks are not accepted.

`better_workflows.execution.id` is the canonical correlation attribute in logs and
traces. `WorkflowHandle.describe()`, history and dead-letter administration expose the
same execution identifier in their public models, and the CLI accepts it as a filter.
Activity dispatch persists the trace envelope and the worker creates a consumer span
with that external parent, so correlation survives a process boundary. Continuation
generations add chain and generation attributes; span IDs are deliberately not stored
in the journal, whose durable correlation remains execution/chain identity.

`WorkflowsHealth` exposes liveness without a database query and readiness with storage,
schema, dispatcher and configured-loop checks. A disconnected PostgreSQL notifier is
`degraded`, not automatically unready, because result waits retain a database fallback.
`WorkflowsAdmin.stats()` is different from local metrics: it uses fixed aggregation
queries to return the durable namespace-wide view of active executions, activity queue
backlog, dead letters and overdue deadlines. It excludes payload columns and does not
make open dead letters a readiness failure. The standalone CLI renders the same admin
backend through `better-workflows stats` or `better-workflows status`.

## Validation boundaries

The suite includes actual Effect-engine suspension, Nest injection, real SQLite storage, business retries, local waits, control requests, FIFO/early signals, timeout resolution, queue concurrency, ownership fencing and dead-letter recovery. Crash tests spawn subprocesses, wait for a persisted checkpoint, send **SIGKILL**, and restart with the same SQLite file. Timer/retry tests let the original deadline expire while no process exists and verify the recovery does not restart the delay.

Package smoke tests import the built ESM package and subpath exports under Node 22.16 and Node 24. The PostgreSQL smoke uses a real database with separate Nest application roots for two socket runners, an activity worker and an API producer, then shuts down one runner and verifies new work still completes. The basic smoke uses multiple roots in one process. The advanced smoke adds two independent OS activity-worker processes, real PostgreSQL, map/child orchestration, measured global/per-key admission and complete terminal-tree retention. Neither is a multi-host network-partition test or a PostgreSQL SIGKILL failover certification.

Not yet covered comprehensively: process pauses and network partitions across hosts, overload/fairness under large backlogs, migration rollback, storage corruption, all commit/ACK crash windows, or arbitrary user-code memory retention. A passing suite does not make this an exactly-once system. Treat this as an alpha, validate application-specific failure cases and keep external operations idempotent.

## Structured execution and compensation

Each map/parallel branch owns a scoped command stream, persisted admission state and final result. Suspending one branch must not cancel its siblings: branch interpreters run in child fibers whose exits are observed independently. A waiting branch retains its map admission. Completed branches are read from the journal, and the next pending branches are admitted only when durable slots free up. Groups settle all branches and report the first failure in stable input/key order. Applications must version changes to branch membership, concurrency or commands.

Child creation and parent linking commit in a journal transaction before outbox dispatch. Results are delivered to the parent's durable deferred. Parent termination applies the persisted close policy; explicit abandon leaves the child independent. A waiting active parent prevents retention of the child's result.

A saga step registers compensation after its forward result is stored. Replay reconstructs callbacks from the same versioned handler, reuses forward results and walks registered steps in reverse order. Undo progress is persisted separately; undo activities use the normal retry/deadline/lease protocol. A failed compensation does not skip the remaining compensations. Suspension and infrastructure defects never enter application business catch/rollback paths. Cancellation is intentionally not automatic compensation: model a business abort inside the saga when rollback must complete.

The manual testing clock controls journal business deadlines and activity timeout scheduling. SQL ownership/transport leases, cluster polling and arbitrary application clocks remain real. The harness advances each due instant, flushes outboxes and waits for observable progress to quiesce under a bounded real-time safety timeout. It does not mock external networks or prove that arbitrary application promises are idle.
