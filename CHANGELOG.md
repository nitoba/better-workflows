# Changelog

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

This alpha does not yet implement child workflows, parallel map, sagas, a virtual clock, retention/admin tooling, global concurrency or a visual dashboard. It has not been published to npm by this change.
