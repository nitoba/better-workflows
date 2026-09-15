// Build the exact source installed from GitHub; no package implementation is vendored.
export default {
  cwd: process.env.BW_BUILD_DIR,
  entry: {
    index: 'src/index.ts', sqlite: 'src/sqlite.ts', postgres: 'src/postgres.ts',
    admin: 'src/admin.ts', testing: 'src/testing.ts', cli: 'src/cli.ts',
  },
  format: ['esm'], platform: 'node', target: 'es2023', dts: true,
  sourcemap: true, clean: true, treeshake: true,
  deps: { neverBundle: [/^@nestjs\//, 'reflect-metadata', 'rxjs', 'zod', 'bun:sqlite'] },
};
