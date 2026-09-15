# Changelog

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
