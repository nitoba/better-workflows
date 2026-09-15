import { createRequire } from 'node:module';
import { dirname, join, resolve } from 'node:path';
import { existsSync } from 'node:fs';
import { mkdir, writeFile, readFile, readdir, rm } from 'node:fs/promises';
import { spawnSync } from 'node:child_process';
import { build } from 'tsdown';

// Build the installed GitHub dependency, never a vendored copy or patched source.
// The pinned commit has no prepare hook. A direct tsdown run inside node_modules
// skips declaration/decorator transforms, so first compile with TypeScript itself.
const require = createRequire(import.meta.url);
const root = resolve(import.meta.dir, '..');
const directory = dirname(require.resolve('better-workflows/package.json'));
const manifest = JSON.parse(await readFile(join(directory, 'package.json'), 'utf8'));
if (manifest.name !== 'better-workflows' || !existsSync(join(directory, 'src/index.ts'))) {
  throw new Error('The installed GitHub source is missing or is not better-workflows.');
}
const dist = join(directory, 'dist');
const compiled = join(dist, 'compiled');
const scratch = join(root, '.build');
await mkdir(scratch, { recursive: true });
await rm(dist, { recursive: true, force: true });
const config = join(scratch, 'workflows-build.json');
await writeFile(config, JSON.stringify({
  extends: join(directory, 'tsconfig.json'),
  compilerOptions: {
    rootDir: join(directory, 'src'), outDir: compiled, target: 'ES2023',
    noEmit: false, emitDeclarationOnly: false, declaration: true, declarationMap: false,
    allowImportingTsExtensions: false, types: ['node', 'bun'],
    typeRoots: [join(root, 'node_modules/@types')],
  },
  include: [join(directory, 'src/**/*.ts')], exclude: [],
}));
try {
  const tsc = join(dirname(require.resolve('typescript/package.json')), 'bin/tsc');
  const result = spawnSync(process.execPath, [tsc, '-p', config], { cwd: root, stdio: 'inherit' });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`GitHub dependency compilation failed (${result.status}).`);
  for (const file of await readdir(compiled, { recursive: true })) {
    if (!file.endsWith('.d.ts')) continue;
    const path = join(compiled, file);
    // Normalize generated declaration specifiers for NodeNext consumers.
    await writeFile(path, (await readFile(path, 'utf8')).replace(/(from\s+|import\s*\()(['"])(\.{1,2}\/[^'"]+)(['"])/g,
      (match, prefix, quote, specifier, end) => /\.[cm]?js$/.test(specifier) ? match : `${prefix}${quote}${specifier}.js${end}`));
  }
  const entries = ['index', 'sqlite', 'postgres', 'admin', 'testing', 'cli'];
  await build({
    config: false,
    entry: Object.fromEntries(entries.map(name => [name, join(compiled, `${name}.js`)])),
    outDir: dist, format: ['esm'], platform: 'node', target: 'es2023',
    dts: false, sourcemap: true, clean: false,
    outExtensions: () => ({ js: '.mjs' }),
    deps: { neverBundle: [/^@nestjs\//, /^effect(?:\/|$)/, /^@effect\//, 'reflect-metadata', 'rxjs', 'bun:sqlite'], onlyBundle: false },
  });
  for (const entry of entries) await writeFile(join(dist, `${entry}.d.mts`), `export * from './compiled/${entry}.js';\n`);
  const runtime = (await readdir(dist)).find(file => file.startsWith('module-') && file.endsWith('.mjs'));
  if (runtime && (await readFile(join(dist, runtime), 'utf8')).includes('constructor(@Inject')) {
    throw new Error('Untransformed parameter decorators in GitHub dependency build.');
  }
} finally { await rm(config, { force: true }); }
console.log('Installed GitHub dependency compiled with Nest decorator metadata and declarations.');
