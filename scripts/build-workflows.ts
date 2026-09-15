import { existsSync, readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { spawnSync } from 'node:child_process';

// The pinned GitHub source exposes dist/ but has no prepare lifecycle script.
// Build the installed dependency, not a copied implementation or a workspace link.
const root = resolve(import.meta.dir, '..');
const dependency = resolve(root, 'node_modules/better-workflows');
if (!existsSync(resolve(dependency, 'src/index.ts'))) {
  throw new Error('GitHub source missing. Run bun install with development dependencies.');
}
const manifest = JSON.parse(readFileSync(resolve(dependency, 'package.json'), 'utf8'));
if (manifest.name !== 'better-workflows') throw new Error('Unexpected dependency package');
const result = spawnSync(process.execPath, ['run', '--bun', '--cwd', dependency, 'build'], {
  cwd: root,
  env: { ...process.env, PATH: `${root}/node_modules/.bin:${process.env.PATH ?? ''}` },
  stdio: 'inherit',
});
if (result.error) throw result.error;
if (result.status !== 0) process.exit(result.status ?? 1);
if (!existsSync(resolve(dependency, 'dist/index.mjs'))) throw new Error('Dependency build produced no entry point');
