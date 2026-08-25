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
 * Two modes:
 *
 *   --mode inrun   (default)
 *     Compares bench-results.json (current commit) against bench-main.json
 *     (base branch, captured in the same CI job on the same runner). No
 *     calibration needed — runner variance is zero. Threshold: 10%.
 *     Always exits 0; posts a step-summary table but never blocks the PR.
 *
 *   --mode drift
 *     Compares bench-results.json against bench-baseline-release.json (a
 *     committed file updated at each release). Because the two files may have
 *     been captured on different runners, results are normalised using the
 *     "__calibration__" benchmark median before comparing. Threshold: 20%.
 *     Always exits 0; posts a step-summary table but never blocks the job.
 *
 * Usage (CI):
 *   npm run bench:main      # on base branch  → bench-main.json
 *   npm run bench:ci        # on PR commit     → bench-results.json
 *   node scripts/check-bench-regression.mjs --mode inrun
 *
 *   npm run bench:ci        # on main          → bench-results.json
 *   node scripts/check-bench-regression.mjs --mode drift
 */

import { readFileSync, appendFileSync, existsSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = resolve(__dirname, "..");

// ── CLI args ────────────────────────────────────────────────────────────────

const args = process.argv.slice(2);
const modeArg = args.find((a) => a.startsWith("--mode="))?.split("=")[1]
  ?? (args[args.indexOf("--mode") + 1] ?? "inrun");

const MODE = modeArg === "drift" ? "drift" : "inrun";

const INRUN_THRESHOLD = 0.10;  // 10% — same-runner comparison, tight
const DRIFT_THRESHOLD = 0.20;  // 20% — cross-runner, calibration-normalised

const CALIBRATION_PREFIX = "__calibration__";

// ── File paths ───────────────────────────────────────────────────────────────

const resultsPath  = resolve(root, "bench-results.json");
const baselinePath = MODE === "drift"
  ? resolve(root, "bench-baseline-release.json")
  : resolve(root, "bench-main.json");

const THRESHOLD = MODE === "drift" ? DRIFT_THRESHOLD : INRUN_THRESHOLD;

// ── Helpers ──────────────────────────────────────────────────────────────────

/**
 * Extract { name → median } from a vitest --outputJson file.
 * Calibration entries are returned in a separate map so callers can strip them
 * from the main comparison without losing the value.
 */
function extractBenchmarks(data) {
  const benchmarks = new Map();
  const calibration = new Map();
  for (const file of data?.files ?? []) {
    for (const group of file?.groups ?? []) {
      for (const bench of group?.benchmarks ?? []) {
        const { name, median } = bench ?? {};
        if (!name || median == null) continue;
        if (name.startsWith(CALIBRATION_PREFIX)) {
          calibration.set(name, median);
        } else {
          benchmarks.set(name, median);
        }
      }
    }
  }
  return { benchmarks, calibration };
}

function loadJson(path, label) {
  if (!existsSync(path)) {
    // Non-blocking: warn and skip rather than hard-failing the CI job.
    console.warn(`⚠  ${label} not found at ${path}. Skipping bench check.`);
    process.exit(0);
  }
  return JSON.parse(readFileSync(path, "utf-8"));
}

// ── Load data ────────────────────────────────────────────────────────────────

const baselineData = loadJson(baselinePath,
  MODE === "drift" ? "Release baseline (bench-baseline-release.json)" : "Main branch results (bench-main.json)");
const resultsData  = loadJson(resultsPath, "Current results (bench-results.json)");

const { benchmarks: baseMap, calibration: baseCalib } = extractBenchmarks(baselineData);
const { benchmarks: currMap, calibration: currCalib } = extractBenchmarks(resultsData);

if (baseMap.size === 0) {
  console.warn("Warning: baseline has no benchmark entries. Skipping check.");
  process.exit(0);
}

// ── Calibration (drift mode only) ────────────────────────────────────────────

let speedRatio = 1.0;

if (MODE === "drift") {
  const baseCalibVal = [...baseCalib.values()][0];
  const currCalibVal = [...currCalib.values()][0];

  if (baseCalibVal == null || currCalibVal == null) {
    console.warn(
      "Warning: calibration benchmark (__calibration__*) missing from one or " +
      "both result files. Drift comparison will use raw timings — results may " +
      "be affected by runner speed differences."
    );
  } else {
    speedRatio = currCalibVal / baseCalibVal;
    const pct = ((speedRatio - 1) * 100).toFixed(1);
    const direction = speedRatio > 1 ? "slower" : "faster";
    console.log(
      `Runner calibration: baseline=${baseCalibVal.toFixed(2)}ms  ` +
      `current=${currCalibVal.toFixed(2)}ms  ` +
      `ratio=${speedRatio.toFixed(3)}  (this runner is ${Math.abs(pct)}% ${direction})`
    );
  }
}

// ── Compare ──────────────────────────────────────────────────────────────────

let failures = 0;
let checked  = 0;
const rows   = [];

const modeLabel = MODE === "drift"
  ? `drift vs release baseline (threshold ${DRIFT_THRESHOLD * 100}%, calibration-normalised)`
  : `in-run vs main branch (threshold ${INRUN_THRESHOLD * 100}%, same runner)`;

console.log(`\nMode: ${modeLabel}\n`);

for (const [name, baselineMedian] of baseMap) {
  const rawCurrentMedian = currMap.get(name);
  if (rawCurrentMedian == null) {
    console.warn(`  SKIP: "${name}" not found in current results`);
    continue;
  }

  checked++;

  // In drift mode, normalise the current result by the runner speed ratio so
  // a uniformly slower runner doesn't register as a regression.
  const currentMedian = MODE === "drift"
    ? rawCurrentMedian / speedRatio
    : rawCurrentMedian;

  const ratio     = currentMedian / baselineMedian;
  const pctChange = ((ratio - 1) * 100).toFixed(1);
  const status    = ratio > 1 + THRESHOLD ? "FAIL"
    : ratio < 1 - THRESHOLD              ? "FASTER"
    :                                       "OK";

  const symbol = status === "FAIL" ? "✗" : status === "FASTER" ? "↑" : "✓";

  const normalised = MODE === "drift" && speedRatio !== 1.0
    ? ` (raw ${rawCurrentMedian.toFixed(2)}ms → normalised ${currentMedian.toFixed(2)}ms)`
    : "";

  console.log(
    `  ${symbol} ${name}: ${baselineMedian.toFixed(2)}ms → ${currentMedian.toFixed(2)}ms ` +
    `(${pctChange > 0 ? "+" : ""}${pctChange}%) [${status}]${normalised}`
  );

  rows.push({ name, baselineMedian, currentMedian, rawCurrentMedian, pctChange, status, symbol });

  if (status === "FAIL") failures++;
}

console.log(`\nChecked ${checked} benchmarks, ${failures} regression(s).`);

if (failures === 0) {
  console.log("All benchmarks within threshold.");
} else {
  console.warn(`\n⚠  ${failures} benchmark(s) exceeded the ${THRESHOLD * 100}% threshold.`);
  if (MODE === "drift") {
    console.warn("   This may indicate accumulated performance drift since the last release.");
    console.warn("   Update bench-baseline-release.json when cutting the next release.");
  } else {
    console.warn("   Review the changes in this PR for unintended performance impact.");
  }
}

// ── GitHub Step Summary ───────────────────────────────────────────────────────

const summaryPath = process.env.GITHUB_STEP_SUMMARY;
if (summaryPath && rows.length > 0) {
  const title = MODE === "drift"
    ? "### Frontend Benchmark — Drift Check (vs release baseline)\n\n"
    : "### Frontend Benchmark — PR Regression Check (vs main)\n\n";

  const calibNote = MODE === "drift" && speedRatio !== 1.0
    ? `> Runner speed ratio: **${speedRatio.toFixed(3)}×** ` +
      `(baseline calibration ${baseCalib.values().next().value?.toFixed(2)}ms, ` +
      `current ${currCalib.values().next().value?.toFixed(2)}ms). ` +
      `Current timings have been normalised before comparison.\n\n`
    : "";

  const header =
    `| Benchmark | Baseline | Current${MODE === "drift" ? " (normalised)" : ""} | Change | Status |\n` +
    `|-----------|----------|---------|--------|--------|\n`;

  const body = rows.map((r) => {
    const pct = `${r.pctChange > 0 ? "+" : ""}${r.pctChange}%`;
    return `| ${r.name} | ${r.baselineMedian.toFixed(2)} ms | ${r.currentMedian.toFixed(2)} ms | ${pct} | ${r.symbol} ${r.status} |`;
  }).join("\n");

  const footer = `\n\n> Threshold: ${THRESHOLD * 100}% · Mode: ${MODE} · ` +
    `${checked} benchmarks checked · ${failures} regression(s)\n`;

  appendFileSync(summaryPath, title + calibNote + header + body + footer);
}

// Always exit 0 — bench checks are informational, not blocking.
process.exit(0);
