# Agent instructions

- Use Bun `1.4.2` for repository commands (`bun install`, `bun run`, `bun test`); compiled package smoke tests also require Node `>=22.16.0`.
- Install with `bun install --frozen-lockfile`; do not edit `bun.lock` by hand.
- This is a single ESM library package. Public entrypoints are `src/index.ts`, `src/sqlite.ts`, `src/postgres.ts`, `src/admin.ts`, `src/testing.ts`, `src/observability.ts`, and `src/cli.ts`; `tsdown.config.ts` builds them into `dist/`.
- Keep Effect and storage/worker machinery behind the public Nest-facing API; application-facing declarations must not expose internal Effect types.
- Treat `dist/` as generated output: use `bun run build` rather than editing it, and run the build before package smoke tests or JSDoc checks.
- For the normal focused loop, run `bun run typecheck`, `bun test <test-file-or-pattern>`, `bun run format:check`, and `bun run lint`; `bun test` uses `bunfig.toml` with `tests/` as its root.
- The release-equivalent check is `bun run check` (typecheck, all tests, formatting, build, JSDoc, lint, and publint). CI additionally runs both examples and Node package smoke tests.
- PostgreSQL integration coverage needs Docker or a Podman socket: `bun run test:postgres` starts PostgreSQL 16, then runs build, the Bun suite, and Node package smoke tests; do not assume SQLite tests cover distributed behavior.
- Node package smoke tests require a prior build; run `bun run build && bun run test:node`. The PostgreSQL script supplies `WORKFLOWS_TEST_POSTGRES_URL` only to the integration run.
- Preserve the repository's formatting (`oxfmt`: no semicolons, single quotes, 100-column width) and lint constraints, including the vendored `tools/oxlint/anti-slop` plugin. Formatting excludes that vendored tree.
- Public API changes require JSDoc compatible with declaration generation; verify with `bun run build && bun run docs:check`.
- Workflow code must remain deterministic: durable commands are awaited, stable explicit `stepId`s are used, and I/O, mutable reads, randomness, and external effects belong in activities. External effects are at-least-once and need idempotency.
- SQLite is single-process/single-host; distributed execution uses PostgreSQL plus the configured Effect socket cluster. Do not share a SQLite file across hosts.
- Do not register the same workflow/activity handler in both `forFeature()` and an outer Nest module's `providers`; use feature registration and Nest imports/exports or `useExisting`.
- Existing uncommitted files may be user work; inspect and preserve them. In particular, do not modify or remove `alpha.9.md` or `alpha.8.md` unless explicitly asked.
