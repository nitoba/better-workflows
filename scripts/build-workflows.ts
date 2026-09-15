import { cp, mkdtemp, readFile, rm, symlink } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { join, resolve } from 'node:path';
import { spawnSync } from 'node:child_process';

// GitHub installs contain source, but this pinned alpha has no prepare script.
// tsdown's declaration plugin excludes paths under node_modules. Build the exact
// installed source in a disposable staging directory and install its dist output.
// This is NOT a vendored library: package.json + bun.lock resolve the GitHub SHA.
const root = resolve(import.meta.dir, '..');
const dependency = join(root, 'node_modules/better-workflows');
const manifest = JSON.parse(await readFile(join(dependency, 'package.json'), 'utf8'));
if (manifest.name !== 'better-workflows') throw new Error('Unexpected GitHub dependency');
const staging = await mkdtemp(join(root, '.workflows-build-'));
try {
  for (const path of ['src', 'package.json', 'tsconfig.json']) {
    await cp(join(dependency, path), join(staging, path), { recursive: true });
  }
  await symlink(join(root, 'node_modules'), join(staging, 'node_modules'), 'junction');
  const child = spawnSync(process.env.NODE_BINARY ?? 'node', [
    join(root, 'node_modules/tsdown/dist/run.mjs'), '--config-loader', 'native',
    '--config', join(root, 'scripts/workflows-build.config.mjs'),
  ], {
    cwd: root, stdio: 'inherit',
    env: { ...process.env, BW_BUILD_DIR: staging },
  });
  if (child.error) throw child.error;
  if (child.status !== 0) throw new Error(`GitHub dependency build exited with ${child.status}`);
  for (const name of ['index.mjs', 'index.d.mts', 'sqlite.mjs', 'testing.d.mts']) {
    if (!existsSync(join(staging, 'dist', name))) throw new Error(`Missing dependency output: ${name}`);
  }
  await rm(join(dependency, 'dist'), { recursive: true, force: true });
  await cp(join(staging, 'dist'), join(dependency, 'dist'), { recursive: true });
} finally {
  await rm(staging, { recursive: true, force: true });
}
