import { defineConfig } from "tsup";

export default defineConfig({
  entry: { cli: "src/cli.ts" },
  format: ["esm"],
  // Keep workspace deps external; only commander is bundled-in at publish time via node_modules
  external: ["@xi-era/acp-sdk"],
  banner: { js: "#!/usr/bin/env node" },
  clean: true,
  sourcemap: true,
  target: "es2022",
});
