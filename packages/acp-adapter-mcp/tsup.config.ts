import { defineConfig } from "tsup";

export default defineConfig([
  {
    entry: { index: "src/index.ts" },
    format: ["esm"],
    external: ["@xi-era/acp-sdk", "@modelcontextprotocol/sdk"],
    dts: true,
    clean: true,
    sourcemap: true,
    target: "es2022",
  },
  {
    entry: { bin: "src/bin.ts" },
    format: ["esm"],
    external: ["@xi-era/acp-sdk", "@modelcontextprotocol/sdk"],
    banner: { js: "#!/usr/bin/env node" },
    clean: false,
    sourcemap: true,
    target: "es2022",
  },
]);
