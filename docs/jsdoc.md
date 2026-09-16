# Public API JSDoc

The primary API documentation is beside the declarations consumed by the editor, not
only in this README tree. It covers `better-workflows`, `better-workflows/sqlite`,
`better-workflows/postgres`, `better-workflows/admin` and `better-workflows/testing`.

## What to document

Document every public export and its public members at the declaration site. Include
an actionable summary, `@param` for each argument and `@returns` for callables.
Describe generic arguments with `@typeParam`, meaningful failure cases with `@throws`,
and omitted-option behavior with `@defaultValue`. Explain units and the distinction
between local slots, durable branch admission and distributed permits.

Examples use fenced TypeScript inside `@example`. They must import real package entry
points and type-check without suppressions. A `declare` binding represents a registered
contract, injected service or runtime context supplied by the surrounding application;
it is not an executable implementation. Supply actual decorated contracts when an
example illustrates registration. Never import private `src/internal` code in examples.

Do not promise more than the implementation guarantees. In particular, distinguish
local result waits from workflow cancellation, cooperative pause from freezing I/O,
request-cancel child policies from propagation of pause, and business compensation
from cancellation. Document at-least-once external effects, replay restrictions, queue
policy ownership, whole-object retry replacement, and retention tombstones explicitly.

Constructors/backend tokens used only by the library are marked `@internal` where
relevant. This tag documents intent; it is not an authorization mechanism or a promise
that the bundler removes a symbol. Normal application code obtains clients and admin
services from Nest, not by constructing private backends.

## Verification

```sh
bun run build
bun run docs:check
```

The checker discovers the five typed package entry points from `package.json`. It:

1. Reads their exported symbols and public member types with the locked TypeScript
   compiler, following aliases and local declaration references.
2. Checks that public summaries, callable parameters and returns are documented.
3. Compares the documentation and tags seen through generated `.d.mts` files against
   the source. This checks what a TypeScript consumer resolves, including declaration
   chunks produced by the bundler, instead of grepping only `dist/index.d.mts`.
4. Extracts every TypeScript `@example` and compiles it as a strict NodeNext consumer
   of the package exports. Temporary files are removed even on failure.

`bun run check` and a dedicated documentation CI job run this verification after the build. No extra
production dependency or generated API website is introduced. The checker uses the
locked TypeScript 7 development API; its synchronous pipe transport runs under Node
(the `docs:check` script handles this). Updating TypeScript requires validating this
development tool as well as the library's normal build.

Example compilation checks syntax/types and public imports, not live service behavior.
The existing runtime/SQL/crash tests remain responsible for behavioral verification.
Documentation comparisons cannot prove that prose correctly describes behavior; review
the implementation and its tests whenever changing guarantees or defaults.
