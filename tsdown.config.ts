import { defineConfig } from "tsdown";

export default defineConfig({
  entry: {
    index: "src/index.ts",
  },
  format: ["esm"],
  platform: "node",
  target: "es2023",
  dts: true,
  sourcemap: true,
  clean: true,
  treeshake: true,
  deps: {
    neverBundle: [
      /^@nestjs\//,
      "reflect-metadata",
      "rxjs",
      "zod",
      "bun:sqlite",
    ],
  },
});
