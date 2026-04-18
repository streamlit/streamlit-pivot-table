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

import type { PivotData } from "../engine/PivotData";
import type {
  AnyConditionalFormatRule,
  ColorScaleRule,
  DataBarsRule,
  ThresholdRule,
} from "../engine/types";
import { DEFAULT_BUDGETS } from "../engine/perf";

// ---------------------------------------------------------------------------
// Column statistics (precomputed min/max per value field)
// ---------------------------------------------------------------------------

export interface ColumnStats {
  min: number;
  max: number;
  range: number;
}

const statsCache = new WeakMap<PivotData, Map<string, ColumnStats>>();

export function computeColumnStats(
  pivotData: PivotData,
  valField: string,
): ColumnStats {
  let perField = statsCache.get(pivotData);
  if (!perField) {
    perField = new Map();
    statsCache.set(pivotData, perField);
  }
  const cached = perField.get(valField);
  if (cached) return cached;

  let min = Infinity;
  let max = -Infinity;
  const rowKeys = pivotData.getRowKeys();
  const colKeys = pivotData.getColKeys();

  for (const rk of rowKeys) {
    for (const ck of colKeys) {
      const v = pivotData.getAggregator(rk, ck, valField).value();
      if (v !== null) {
        if (v < min) min = v;
        if (v > max) max = v;
      }
    }
  }

  if (!isFinite(min)) min = 0;
  if (!isFinite(max)) max = 0;

  const stats: ColumnStats = { min, max, range: max - min };
  perField.set(valField, stats);
  return stats;
}

// ---------------------------------------------------------------------------
// Color interpolation
// ---------------------------------------------------------------------------

type RGB = [number, number, number];

function parseColor(color: string): RGB | null {
  if (color.startsWith("#")) {
    const hex = color.slice(1);
    if (hex.length === 3) {
      return [
        parseInt(hex[0] + hex[0], 16),
        parseInt(hex[1] + hex[1], 16),
        parseInt(hex[2] + hex[2], 16),
      ];
    }
    if (hex.length === 6) {
      return [
        parseInt(hex.slice(0, 2), 16),
        parseInt(hex.slice(2, 4), 16),
        parseInt(hex.slice(4, 6), 16),
      ];
    }
  }
  const rgbMatch = color.match(/^rgb\((\d+),\s*(\d+),\s*(\d+)\)$/);
  if (rgbMatch) {
    return [
      parseInt(rgbMatch[1]),
      parseInt(rgbMatch[2]),
      parseInt(rgbMatch[3]),
    ];
  }
  return null;
}

function lerpRgb(c1: RGB, c2: RGB, t: number): RGB {
  return [
    Math.round(c1[0] + (c2[0] - c1[0]) * t),
    Math.round(c1[1] + (c2[1] - c1[1]) * t),
    Math.round(c1[2] + (c2[2] - c1[2]) * t),
  ];
}

function clamp01(t: number): number {
  if (t < 0) return 0;
  if (t > 1) return 1;
  return t;
}

function rgbString(rgb: RGB): string {
  return `rgb(${rgb[0]}, ${rgb[1]}, ${rgb[2]})`;
}

/**
 * WCAG 2.1 relative luminance from an sRGB triplet.
 * Returns a value in [0, 1] where 0 = black, 1 = white.
 */
function relativeLuminance(rgb: RGB): number {
  const [rs, gs, bs] = rgb.map((c) => {
    const s = c / 255;
    return s <= 0.03928 ? s / 12.92 : Math.pow((s + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * rs + 0.7152 * gs + 0.0722 * bs;
}

/**
 * Pick a readable text color (dark or light) for the given background.
 * Uses a luminance threshold of 0.179 (WCAG contrast ratio >= 4.5:1).
 */
function contrastTextColor(bgRgb: RGB): string {
  return relativeLuminance(bgRgb) > 0.179 ? "#11181c" : "#f0f2f6";
}

// ---------------------------------------------------------------------------
// Style computation
// ---------------------------------------------------------------------------

function isColorScale(rule: AnyConditionalFormatRule): rule is ColorScaleRule {
  return rule.type === "color_scale";
}

function isDataBars(rule: AnyConditionalFormatRule): rule is DataBarsRule {
  return rule.type === "data_bars";
}

function isThreshold(rule: AnyConditionalFormatRule): rule is ThresholdRule {
  return rule.type === "threshold";
}

/**
 * Compute inline styles for a data cell based on conditional formatting rules.
 * Returns undefined when no rules apply.
 */
export function computeCellStyle(
  value: number | null,
  valField: string,
  rules: AnyConditionalFormatRule[],
  pivotData: PivotData,
  isTotal: boolean,
): React.CSSProperties | undefined {
  if (value === null || rules.length === 0) return undefined;

  // Fail-safe: skip expensive formatting for very large tables
  if (pivotData.totalCellCount > DEFAULT_BUDGETS.maxVisibleCells)
    return undefined;

  for (const rule of rules) {
    if (rule.apply_to.length > 0 && !rule.apply_to.includes(valField)) continue;
    if (isTotal && !rule.include_totals) continue;

    // Threshold rules use absolute values — no stats needed
    if (isThreshold(rule)) {
      for (const cond of rule.conditions) {
        let matches = false;
        switch (cond.operator) {
          case "gt":
            matches = value > cond.value;
            break;
          case "gte":
            matches = value >= cond.value;
            break;
          case "lt":
            matches = value < cond.value;
            break;
          case "lte":
            matches = value <= cond.value;
            break;
          case "eq":
            matches = value === cond.value;
            break;
          case "between":
            matches =
              value >= cond.value && value <= (cond.value2 ?? cond.value);
            break;
        }
        if (matches) {
          const style: React.CSSProperties = {};
          if (cond.background) {
            style.backgroundColor = cond.background;
            if (!cond.color) {
              const bgRgb = parseColor(cond.background);
              if (bgRgb) style.color = contrastTextColor(bgRgb);
            }
          }
          if (cond.color) style.color = cond.color;
          if (cond.bold) style.fontWeight = 600;
          return style;
        }
      }
      continue;
    }

    // Color scale and data bars need column stats (cached per field)
    const stats = computeColumnStats(pivotData, valField);

    if (isColorScale(rule)) {
      const minC = parseColor(rule.min_color);
      const maxC = parseColor(rule.max_color);
      if (!minC || !maxC) continue;

      const midC = rule.mid_color ? parseColor(rule.mid_color) : null;
      const hasMidValue =
        midC !== null &&
        typeof rule.mid_value === "number" &&
        Number.isFinite(rule.mid_value);

      let bgRgb: RGB;
      if (stats.range === 0) {
        // Constant column (no variance): 3-color scales snap to mid_color;
        // 2-color scales fall back to a neutral min<->max blend to match
        // the pre-mid_value shipped behavior rather than implying every
        // value sits at the floor.
        bgRgb = midC ?? lerpRgb(minC, maxC, 0.5);
      } else if (hasMidValue) {
        // Anchor the gradient at an explicit numeric midpoint so the bend
        // sits at `mid_value` in value space rather than at the midpoint of
        // the observed range. Values outside [min, max] clamp to endpoint
        // colors (no extrapolation).
        const mid = rule.mid_value as number;
        if (mid <= stats.min) {
          // Low segment collapses: interpolate the entire range on mid->max.
          const denom = stats.max - mid;
          const t = denom <= 0 ? 0 : clamp01((value - mid) / denom);
          bgRgb = lerpRgb(midC!, maxC, t);
        } else if (mid >= stats.max) {
          // High segment collapses: interpolate the entire range on min->mid.
          const denom = mid - stats.min;
          const t = denom <= 0 ? 1 : clamp01((value - stats.min) / denom);
          bgRgb = lerpRgb(minC, midC!, t);
        } else if (value <= mid) {
          const denom = mid - stats.min;
          const t = denom <= 0 ? 1 : clamp01((value - stats.min) / denom);
          bgRgb = lerpRgb(minC, midC!, t);
        } else {
          const denom = stats.max - mid;
          const t = denom <= 0 ? 0 : clamp01((value - mid) / denom);
          bgRgb = lerpRgb(midC!, maxC, t);
        }
      } else {
        // Legacy path: 2-color or mid_color-only 3-color scale. Clamp the
        // normalized position so totals or outliers outside the body-cell
        // range render as the endpoint color rather than extrapolating.
        const t = clamp01((value - stats.min) / stats.range);
        if (midC) {
          bgRgb =
            t <= 0.5
              ? lerpRgb(minC, midC, t * 2)
              : lerpRgb(midC, maxC, (t - 0.5) * 2);
        } else {
          bgRgb = lerpRgb(minC, maxC, t);
        }
      }
      return {
        backgroundColor: rgbString(bgRgb),
        color: contrastTextColor(bgRgb),
      };
    }

    if (isDataBars(rule)) {
      const t = stats.range === 0 ? 0 : (value - stats.min) / stats.range;
      const widthPct = Math.round(t * 100);
      const barColor = rule.color ?? "var(--st-primary-color)";
      if (rule.fill === "gradient") {
        return {
          background: `linear-gradient(to right, color-mix(in srgb, ${barColor} 5%, transparent) 0%, color-mix(in srgb, ${barColor} 30%, transparent) ${widthPct}%, transparent ${widthPct}%)`,
          position: "relative" as const,
        };
      }
      return {
        background: `linear-gradient(to right, color-mix(in srgb, ${barColor} 30%, transparent) ${widthPct}%, transparent ${widthPct}%)`,
        position: "relative" as const,
      };
    }
  }

  return undefined;
}
