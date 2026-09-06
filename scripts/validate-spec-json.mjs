#!/usr/bin/env node
// Validates that every JSON block in spec/ACP-*.md parses.
import { readFileSync, readdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const specDir = join(dirname(fileURLToPath(import.meta.url)), "..", "spec");
const specs = readdirSync(specDir).filter((f) => f.endsWith(".md"));

let totalOk = 0;
let totalSkipped = 0;
let failed = false;

for (const spec of specs) {
  const text = readFileSync(join(specDir, spec), "utf8");
  const blocks = [...text.matchAll(/```json\n([\s\S]*?)```/g)].map((m) => m[1]);
  let ok = 0;
  let skipped = 0;
  const failures = [];

  for (const [i, block] of blocks.entries()) {
    if (block.includes('"..."') || block.includes('"...":')) {
      skipped++;
      continue;
    }
    const isNdjson = block.trim().split("\n").length > 1 && !block.trim().startsWith("{\n");
    try {
      if (isNdjson) {
        for (const line of block.trim().split("\n")) {
          if (line.trim()) JSON.parse(line.trim());
        }
      } else {
        JSON.parse(block);
      }
      ok++;
    } catch (e) {
      failures.push({ index: i, error: e.message, preview: block.slice(0, 120) });
    }
  }

  console.log(`${spec}: blocks ${blocks.length}, parsed ok ${ok}, skipped (prose) ${skipped}`);
  for (const f of failures) {
    failed = true;
    console.error(`  FAIL #${f.index}: ${f.error}\n    ${f.preview}`);
  }
  totalOk += ok;
  totalSkipped += skipped;
}

console.log(`Total: ok ${totalOk}, skipped ${totalSkipped}`);
if (failed) process.exit(1);
console.log("All spec JSON examples parse successfully.");
