import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["examples/*/test/**/*.e2e.ts", "templates/test/**/*.e2e.ts", "test/e2e/**/*.e2e.ts"],
    name: "e2e",
    testTimeout: 30_000,
    hookTimeout: 30_000,
    pool: "forks",
    maxConcurrency: 1,
  },
});
