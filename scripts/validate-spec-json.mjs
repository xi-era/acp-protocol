#!/usr/bin/env node
// Validates that every JSON block in spec/ACP-0.1-SPEC.md parses.
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const specPath = join(dirname(fileURLToPath(import.meta.url)), "..", "spec", "ACP-0.1-SPEC.md");
const text = readFileSync(specPath, "utf8");

// Skip ```json blocks that contain prose placeholders like "..." keys
const blocks = [...text.matchAll(/```json\n([\s\S]*?)```/g)].map((m) => m[1]);
let ok = 0;
let skipped = 0;
const failures = [];

for (const [i, block] of blocks.entries()) {
  if (block.includes('"..."') || block.includes('"...":')) {
    skipped++;
    continue;
  }
  // NDJSON example blocks: multiple lines each a JSON envelope
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

console.log(`JSON blocks: ${blocks.length}, parsed ok: ${ok}, skipped (prose): ${skipped}`);
if (failures.length) {
  for (const f of failures) console.error(`FAIL #${f.index}: ${f.error}\n  ${f.preview}`);
  process.exit(1);
}
console.log("All spec JSON examples parse successfully.");
