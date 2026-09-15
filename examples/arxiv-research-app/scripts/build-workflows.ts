import { createRequire } from 'node:module';
import { dirname, resolve, join } from 'node:path';
import { existsSync } from 'node:fs';
import { cp, mkdtemp, rm, symlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { spawnSync } from 'node:child_process';

// GitHub dependency has no committed dist/ or prepare script. Build its installed
// source in a temporary workspace (TS declaration bundlers exclude node_modules),
// then place ONLY generated output back in the installed dependency. No vendoring.
const require = createRequire(import.meta.url);
const app = resolve(import.meta.dir, '..');
const dependency = dirname(require.resolve('better-workflows/package.json'));
if (!existsSync(join(dependency, 'src/index.ts'))) throw new Error('GitHub dependency source missing. Run bun install.');
const stage = await mkdtemp(join(tmpdir(), 'better-workflows-build-'));
try {
  for (const path of ['src', 'package.json', 'tsconfig.json', 'tsdown.config.ts']) {
    await cp(join(dependency, path), join(stage, path), { recursive: true });
  }
  await symlink(join(app, 'node_modules'), join(stage, 'node_modules'), 'junction');
  const executable = join(app, 'node_modules/.bin/tsdown');
  const result = spawnSync(executable, ['--config', join(stage, 'tsdown.config.ts')], {
    cwd: stage, stdio: 'inherit', shell: process.platform === 'win32',
  });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`GitHub dependency build exited ${result.status}`);
  if (!existsSync(join(stage, 'dist/index.d.mts'))) throw new Error('Missing dependency declarations after build');
  await rm(join(dependency, 'dist'), { recursive: true, force: true });
  await cp(join(stage, 'dist'), join(dependency, 'dist'), { recursive: true });
} finally {
  await rm(stage, { recursive: true, force: true });
}
