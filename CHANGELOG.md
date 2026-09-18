# Changelog

## 0.1.0-alpha.9

- Added optional contract-first activities with metadata-only contracts and Nest-injected handlers.
- Added typed abstract-capable activity contracts, handler association, explicit method metadata and
  contract-based activity routing while preserving simple-mode registrations and the wire protocol.

## 0.1.0-alpha.8

- Added durable cron and interval scheduling with persisted cursors, misfire/overlap policies and distributed claims.
- Added durable manual schedule triggers, collision-safe occurrence identity, bounded occurrence retention and forward-only schedule migrations.
- Cron schedules without a timezone now use deterministic UTC semantics; persistent ownership leases reconcile completely removed implementations without breaking partial deployments.
- Added schedule occurrence metrics, lag histograms, trigger/tick tracing, structured scheduler logs and scheduler-specific readiness.
- Added administrative/CLI schedule operations, virtual-time testing support, crash/concurrency hardening, DST/timezone coverage and package smoke compatibility.

## 0.1.0-alpha.7

- Added production observability with stable workflow/activity metrics, short-lived distributed tracing, structured logs and bounded OTLP export.
- Added privacy-safe correlation, exporter failure isolation, shutdown flushing, liveness/readiness checks and namespace-wide operational diagnostics.
- Added administrative stats/status commands and behavioral coverage for trace propagation, continuation chains and dead-letter recovery.

## 0.1.0-alpha.6

- Added `WorkflowContext.continueAsNew()` with atomic continuation chains, `continued` execution snapshots, chain-aware results and retention.
- Added forward-only journal migration v5 and crash-recovery coverage for continuation dispatch.
- Added application-owned SQL activity deliveries and dead-letter administration for operational failures, with `blocked` executions, metadata-first `list/get/requeue/discard` APIs, CLI commands, safe retention, cancellation fencing and forward-only journal migration v6.
- Added notification-first `WorkflowHandle.result()` waits with SQLite wake-ups, shared PostgreSQL `LISTEN`/`NOTIFY`, revision-based race protection, reconnect rechecks and a low-frequency fallback.

## 0.1.0-alpha.5

- Optional contract-first workflow declarations with `@WorkflowContract` and `@Workflow(contract)` handlers.
- Abstract contracts can be shared by client-only producers, orchestrators and child workflows without importing handler implementations.
- Workflow client typing now infers input/output from abstract contract signatures while preserving the simple decorator API.

## 0.1.0-alpha.4

- Added queue admission fast paths for queues without distributed limits and per-key-only admission without a global queue-row lock.

## 0.1.0-alpha.3

Breaking registration refactor: replace array-only `forFeature` and string/dictionary queues with domain-owned object registrations and `defineQueue` references. No compatibility overloads.

- Optional root queue catalog; root/feature defaults and explicit final deployment overrides.
- `forFeature` / `forFeatureAsync` distinguish implementations, clients and activity contracts without constructing remote worker services.
- Default routing/retry/timeout on `@Activities`, method overrides and frozen owner-resolved contracts.
- Actual Nest import/export visibility, private queues, shared activity capabilities and `useExisting` bindings.
- Single runtime and local semaphore per logical queue; global/per-key policies remain shared across processes.
- Root/feature execution intersections, per-feature workflow budgets and optional non-global root infrastructure.
- Public admin/testing APIs use the same queue references and catalog. Existing durability and schema administration remain intact.
- Updated examples, Node consumers and PostgreSQL two-process tests; new modularity tests and runnable multi-domain example.

## 0.1.0-alpha.2

- Durable keyed map and named parallel branches, with persisted admission and ordered settle-all results.
- Child workflows with deterministic parent/step identities and explicit parent-close policies.
- Scoped sagas, reverse-order compensation, persisted undo progress and normal activity retries during rollback.
- Nest testing module with isolated manual business time for timers, signal deadlines, retries and activity timeouts.
- Standalone and Nest administrative APIs, plus CLI migration status/run/validate and preview-confirm retention.
- Forward-only transactional journal v2 migration preserving v1 commands and timer protocol.
- SQL-backed global and per-key queue permits, configuration-drift checks, lease renewal and stale-owner fencing.
- Idempotency tombstones after safe history removal, active parent/child protection and stale-plan revalidation.
- Shared reference-counted SQLite connections serialize runtime/administration transactions in one process.
- Additional process-kill recovery and built-package multi-process PostgreSQL tests.

No publication to npm is performed by this change.

## 0.1.0-alpha.1

Initial implementation for NestJS 12, with Effect 4.0.0-rc.115 isolated internally.

- Native workflow/activity decorators, typed injection and configurable dynamic modules.
- Durable SQL-backed acceptance, idempotency, replay and paginated execution history.
- Queued activities, local queue concurrency, cancellation/timeout signals and fenced result commits.
- Persisted business attempts and atomic retry deadlines.
- FIFO signal inbox, early delivery, event deduplication and durable timeout decisions.
- Pause, resume, cancellation and local result waits with independent timeout/abort.
- SQLite Bun/Node adapters and PostgreSQL distributed infrastructure configuration.
- Real JSON-report example, crash-recovery tests and built-package runtime checks.

The initial alpha shipped sequential workflows only. The advanced features listed above were added in alpha.2.
