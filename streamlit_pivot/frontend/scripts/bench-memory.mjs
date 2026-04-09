#!/usr/bin/env node
/**
 * Copyright 2025 Snowflake Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// bench-memory.mjs — Memory profiling for pivot engine data structures
// Run: node --expose-gc scripts/bench-memory.mjs

import { writeFileSync, mkdirSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUTPUT_DIR = join(__dirname, "..", "perf-results");

function generateRecords(numRows, numRegions, numYears) {
  const records = [];
  for (let i = 0; i < numRows; i++) {
    records.push({
      region: `Region_${i % numRegions}`,
      year: `${2000 + (i % numYears)}`,
      revenue: Math.random() * 10000,
    });
  }
  return records;
}

function simulatePivotAggregation(records) {
  const cells = new Map();
  const rowKeys = new Set();
  const colKeys = new Set();
  for (const rec of records) {
    const rk = rec.region;
    const ck = rec.year;
    rowKeys.add(rk);
    colKeys.add(ck);
    const key = `${rk}\x00${ck}`;
    cells.set(key, (cells.get(key) || 0) + rec.revenue);
  }
  return { cellCount: cells.size, rowKeyCount: rowKeys.size, colKeyCount: colKeys.size };
}

function measureProfile(name, numRows, numRegions, numYears) {
  if (globalThis.gc) globalThis.gc();
  const heapBeforeBytes = process.memoryUsage().heapUsed;

  const records = generateRecords(numRows, numRegions, numYears);
  const _pivotResult = simulatePivotAggregation(records);

  if (globalThis.gc) globalThis.gc();
  const heapAfterBytes = process.memoryUsage().heapUsed;

  const heapDeltaBytes = heapAfterBytes - heapBeforeBytes;
  return {
    name,
    rows: numRows,
    regions: numRegions,
    years: numYears,
    heapBeforeBytes,
    heapAfterBytes,
    heapDeltaBytes,
    heapDeltaMB: Number((heapDeltaBytes / 1024 / 1024).toFixed(2)),
  };
}

const profiles = [
  measureProfile("PivotData 50K rows (100x20 grid)", 50000, 100, 20),
  measureProfile("PivotData 200K rows (500x20 grid)", 200000, 500, 20),
];

const report = {
  timestamp: new Date().toISOString(),
  profiles,
};

mkdirSync(OUTPUT_DIR, { recursive: true });
const outputPath = join(OUTPUT_DIR, "memory-profile.json");
writeFileSync(outputPath, JSON.stringify(report, null, 2) + "\n");
console.log(`Memory profile written to ${outputPath}`);
for (const p of profiles) {
  console.log(`  ${p.name}: heapDelta=${p.heapDeltaMB}MB`);
}
