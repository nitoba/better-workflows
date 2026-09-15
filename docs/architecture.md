# Architecture and reliability

## Boundaries

The public entry point contains Nest decorators, dynamic modules, typed clients and Standard Schema contracts. The declaration graph does not expose Effect types. The implementation pins Effect and its platform/SQL adapters to **4.0.0-rc.115**, because workflow/cluster APIs are unstable and the queue worker consumes that version's wire protocol.

Nest discovery registers existing provider instances. It does not construct copies or bypass module imports/exports with a global service locator. Context objects are per invocation. Workflow replay code must not depend on mutable singleton state.

An internal dispatcher bridges an application's async handler to the workflow's own Effect fiber. Durable commands execute on that fiber, where the engine can suspend correctly. Suspension interrupts the interpreter, invalidates the async round and abandons unresolved awaits without rejecting them into application code. Abandoned promises are not retained in a shared forever-pending promise. Application-created timers, global references or arbitrary background work can still retain its own objects; the library is not a JavaScript sandbox or a compiler enforcing determinism.

## What is persisted where

Effect's cluster/message storage owns replayable workflow results, deferred results, timers, workflow ownership and delivery. Effect's persisted queue owns activity dispatch. The additional journal owns acceptance/deduplication, observable command signatures, history, control requests, FIFO signal consumption, business retry deadlines and fenced activity-result receipts.

`start()` commits a run and pending dispatch state in one journal transaction. Dispatch is an outbox: sending to the engine can repeat after a crash; the engine's deterministic execution identity deduplicates it. Signal/deadline delivery similarly persists the decision before completing the engine deferred, then marks it delivered. Delivery can safely repeat.

Journal keys distinguish workflow name/version, workflow idempotency, logical step identity, business attempt and infrastructure delivery. In particular, changing a deployment version does not silently create a second execution for the same start key. The old handler and its activity contracts must remain available while old executions need them. This release detects observed command mismatches; it cannot prove all TypeScript code deterministic.

## Retry/ownership protocol

Activity dispatch payloads snapshot timeout and retry policy. A business failure and the next retry's database-clock deadline commit in the same SQL transaction. The orchestrator waits for that persisted deadline's deferred; replay does not add another full backoff interval.

A worker acquires a journal claim tied to its monotonically increasing queue delivery attempt and a fresh ownership token. Heartbeats/lease renewal and result commits check the current token, live lease and running state. An old invocation cannot commit after a newer delivery fences it, or after its lease expires. A successful receipt can be redelivered to the engine without repeating the handler.

SQL/infrastructure defects and runtime interruption are not serialized as successful queue processing containing a business failure. They cause infrastructure redelivery. Explicit `ActivityError` values represent business outcomes. External effects still need provider-side idempotency: no transaction spans an arbitrary remote service and this journal.

Timeouts and cancellation abort the handler's signal. JavaScript cannot forcibly stop arbitrary non-cooperative I/O or CPU loops in that process. A timed-out handler that ignores its signal may continue externally even though its result cannot be committed. Use abort-aware dependencies and separate worker processes for CPU-intensive or untrusted execution.

## Signals and controls

A database row lock serializes signal acceptance, FIFO consumption and timeout decisions. An event accepted at or before a wait's deadline wins over its timeout even when polling is delayed. The consumed event and wait outcome are durable. Duplicate event keys compare payloads; a conflicting payload is rejected. Input validation uses the execution's registered signal contract, not merely a caller-supplied schema with the same name.

Pause is a cooperative command-boundary control, not an external-process freeze. A currently running activity may complete while paused. Cancel is not compensation. Requests are stored durably, may briefly appear as `cancelling`, and are reconciled with the engine. Graceful Nest shutdown stops this runtime without cancelling the persisted executions.

## Storage/operations

The journal uses database time for retry deadlines, signal acceptance/deadlines and claim leases. Default leases are 30 seconds, renewed every 10 seconds; custom refresh must be positive and at most one third of the lease duration. Extremely short test leases are not production recommendations.

SQLite is an embedded single-process deployment. Distributed topology uses PostgreSQL plus the Effect socket cluster; changing a connection string alone is not cluster configuration. Runner sockets need private-network access and valid advertised addresses. Queue limits are local to each process. Database credentials, application authorization, payload sensitivity, logging, backups and capacity planning remain deployment responsibilities.

Migrations are automatic and additive at bootstrap. The journal rejects schema versions newer than the code knows. This alpha has no explicit migrations CLI, retention API, archival scheme or UI. Historical idempotency/result records are intentionally retained. Removing records changes deduplication guarantees and can break replay. Do not downgrade the engine or prune arbitrary engine/journal tables independently.

## Validation boundaries

The suite includes actual Effect-engine suspension, Nest injection, real SQLite storage, business retries, local waits, control requests, FIFO/early signals, timeout resolution, queue concurrency and ownership fencing. Crash tests spawn subprocesses, wait for a persisted checkpoint, send **SIGKILL**, and restart with the same SQLite file. Timer/retry tests let the original deadline expire while no process exists and verify the recovery does not restart the delay.

Package smoke tests import the built ESM package and subpath exports under Node 22.16 and Node 24. The PostgreSQL smoke uses a real database with separate Nest application roots for two socket runners, an activity worker and an API producer, then shuts down one runner and verifies new work still completes. Those roots run in one Node process; this is **not** a multi-host network-partition test or a PostgreSQL SIGKILL failover certification.

Not yet covered comprehensively: process pauses and network partitions across hosts, overload/fairness under large backlogs, migration rollback, storage corruption, all commit/ACK crash windows, or arbitrary user-code memory retention. A passing suite does not make this an exactly-once system. Treat this as an alpha, validate application-specific failure cases and keep external operations idempotent.
