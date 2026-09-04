import { defineConfig } from "tsup";

export default defineConfig({
  entry: { index: "src/index.ts" },
  format: ["esm"],
  external: ["@xi-era/acp-sdk"],
  clean: true,
  sourcemap: true,
  target: "es2022",
});
