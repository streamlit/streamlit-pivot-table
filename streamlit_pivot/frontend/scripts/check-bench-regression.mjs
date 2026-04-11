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
/**
 * Benchmark regression gate.
 *
 * Compares bench-results.json (current run) against bench-baseline.json
 * using median-of-N timing. Exits non-zero if any benchmark is >20% slower.
 *
 * Usage:
 *   npm run bench:ci                    # produces bench-results.json
 *   node scripts/check-bench-regression.mjs  # compare + gate
 *   npm run bench:save-baseline         # save current as baseline
 */

import { readFileSync, appendFileSync, existsSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = resolve(__dirname, "..");

const REGRESSION_THRESHOLD = 0.20; // 20%

const baselinePath = process.env.BENCH_BASELINE_PATH
  ? resolve(process.env.BENCH_BASELINE_PATH)
  : resolve(root, "bench-baseline.json");
const resultsPath = resolve(root, "bench-results.json");

if (!existsSync(baselinePath)) {
  console.log(
    "No bench-baseline.json found. Run `npm run bench:save-baseline` to create one.",
  );
  console.log("Skipping regression check (first run).");
  process.exit(0);
}

if (!existsSync(resultsPath)) {
  console.error(
    "bench-results.json not found. Run `npm run bench:ci` first.",
  );
  process.exit(1);
}

const baseline = JSON.parse(readFileSync(baselinePath, "utf-8"));
const results = JSON.parse(readFileSync(resultsPath, "utf-8"));

function extractBenchmarks(data) {
  const map = new Map();
  for (const file of data?.files ?? []) {
    for (const group of file?.groups ?? []) {
      for (const bench of group?.benchmarks ?? []) {
        const name = bench?.name;
        const median = bench?.median;
        if (name && median != null) {
          map.set(name, median);
        }
      }
    }
  }
  return map;
}

const baseMap = extractBenchmarks(baseline);
const resultMap = extractBenchmarks(results);

if (baseMap.size === 0) {
  console.warn("Warning: baseline has no benchmark entries. Skipping check.");
  process.exit(0);
}

let failures = 0;
let checked = 0;
const rows = [];

for (const [name, baselineMedian] of baseMap) {
  const currentMedian = resultMap.get(name);
  if (currentMedian == null) {
    console.warn(`  SKIP: "${name}" not found in current results`);
    continue;
  }

  checked++;
  const ratio = currentMedian / baselineMedian;
  const pctChange = ((ratio - 1) * 100).toFixed(1);
  const status =
    ratio > 1 + REGRESSION_THRESHOLD
      ? "FAIL"
      : ratio < 1 - REGRESSION_THRESHOLD
        ? "FASTER"
        : "OK";

  const symbol = status === "FAIL" ? "✗" : status === "FASTER" ? "↑" : "✓";
  console.log(
    `  ${symbol} ${name}: ${baselineMedian.toFixed(2)}ms → ${currentMedian.toFixed(2)}ms (${pctChange > 0 ? "+" : ""}${pctChange}%) [${status}]`,
  );

  rows.push({ name, baselineMedian, currentMedian, pctChange, status, symbol });

  if (status === "FAIL") {
    failures++;
  }
}

console.log(`\nChecked ${checked} benchmarks, ${failures} regression(s).`);

const summaryPath = process.env.GITHUB_STEP_SUMMARY;
if (summaryPath && rows.length > 0) {
  const header = `### Frontend Benchmark Results\n\n` +
    `| Benchmark | Baseline | Current | Change | Status |\n` +
    `|-----------|----------|---------|--------|--------|\n`;
  const body = rows.map((r) => {
    const pct = `${r.pctChange > 0 ? "+" : ""}${r.pctChange}%`;
    return `| ${r.name} | ${r.baselineMedian.toFixed(2)} ms | ${r.currentMedian.toFixed(2)} ms | ${pct} | ${r.symbol} ${r.status} |`;
  }).join("\n");
  const footer = `\n\n> Threshold: ${REGRESSION_THRESHOLD * 100}% · ${checked} benchmarks checked · ${failures} regression(s)\n`;
  appendFileSync(summaryPath, header + body + footer);
}

if (failures > 0) {
  console.error(
    `\nFAILED: ${failures} benchmark(s) regressed by >${REGRESSION_THRESHOLD * 100}%.`,
  );
  process.exit(1);
} else {
  console.log("All benchmarks within threshold.");
  process.exit(0);
}
