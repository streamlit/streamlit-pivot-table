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

import type { PivotConfigV1 } from "../engine/types";

export const WIDTH_SMALL = 100;
export const WIDTH_MEDIUM = 120;
export const WIDTH_LARGE = 200;

export const WIDTH_MIN = 20;
export const WIDTH_MAX = 2000;

const PRESETS: Record<string, number> = {
  small: WIDTH_SMALL,
  medium: WIDTH_MEDIUM,
  large: WIDTH_LARGE,
};

/**
 * Resolve a `column_config.width` entry for a given field to a pixel value.
 *
 * Accepts:
 *   - "small" | "medium" | "large" (preset strings)
 *   - a positive integer within [WIDTH_MIN, WIDTH_MAX]
 *
 * Returns `undefined` if no configured width is set, or if the value is
 * out-of-range / unparseable (frontend-side safety clamp; Python-side
 * validation already rejects most malformed inputs one-shot with a warning).
 *
 * Runtime (user-resized) widths from `columnWidthMap` should take precedence
 * over this resolver; this helper only provides the config-backed default.
 */
export function resolveFieldWidth(
  config: PivotConfigV1,
  field: string | undefined,
): number | undefined {
  if (!field) return undefined;
  const raw = config.field_widths?.[field];
  if (raw == null) return undefined;
  if (typeof raw === "string") {
    const preset = PRESETS[raw];
    return preset != null ? preset : undefined;
  }
  if (typeof raw === "number" && Number.isFinite(raw)) {
    if (raw < WIDTH_MIN || raw > WIDTH_MAX) return undefined;
    return Math.round(raw);
  }
  return undefined;
}

/** Approximate advance of one character in the table's UI font. */
const CHAR_WIDTH = 7;
/** Horizontal padding plus the right border of a row-header cell. */
const CELL_CHROME = 26;
/** Indent added per nested level in hierarchy row layout. */
const LEVEL_INDENT = 16;
/** A toggle chevron, one per collapsible level of the breadcrumb. */
const TOGGLE_WIDTH = 18;
/** Row keys sampled when estimating; enough to be representative, still cheap. */
const SCAN_LIMIT = 2000;

/** Upper bound on an estimated row-header width, so one outlier label can't
 * push the data columns off screen. */
export const ROW_HEADER_WIDTH_CAP = 360;

/**
 * Estimate a width for each row-header column from the text it must hold.
 *
 * Virtualized tables render with `table-layout: fixed`, where the declared
 * width is final: a column cannot grow to fit its contents the way an
 * auto-layout table does, so anything longer is truncated or spills over the
 * neighbouring column. Row headers feel this most, because hierarchy layout
 * packs every level's labels, their indents, and the breadcrumb of dimension
 * names into one column.
 *
 * This approximates from character counts rather than measuring, since it runs
 * during render where measuring would force a layout pass. It only supplies a
 * starting width — a configured `field_widths` entry, a drag, or a double-click
 * to auto-fit all take precedence.
 */
export function estimateRowHeaderWidths(
  dimLabels: string[],
  rowKeys: string[][],
  hierarchy: boolean,
  minWidth: number,
): number[] {
  const clamp = (width: number): number =>
    Math.min(ROW_HEADER_WIDTH_CAP, Math.max(minWidth, Math.ceil(width)));
  const scanned = Math.min(rowKeys.length, SCAN_LIMIT);
  if (dimLabels.length === 0) return [];

  if (hierarchy) {
    // A single column holds the whole breadcrumb and every level's labels.
    let widest =
      dimLabels.join(" / ").length * CHAR_WIDTH +
      TOGGLE_WIDTH * Math.max(dimLabels.length - 1, 0);
    for (let i = 0; i < scanned; i++) {
      const key = rowKeys[i] ?? [];
      for (let level = 0; level < key.length; level++) {
        const width =
          String(key[level] ?? "").length * CHAR_WIDTH + level * LEVEL_INDENT;
        if (width > widest) widest = width;
      }
    }
    return [clamp(widest + CELL_CHROME)];
  }

  return dimLabels.map((label, dim) => {
    let widest = label.length * CHAR_WIDTH + TOGGLE_WIDTH;
    for (let i = 0; i < scanned; i++) {
      const width = String(rowKeys[i]?.[dim] ?? "").length * CHAR_WIDTH;
      if (width > widest) widest = width;
    }
    return clamp(widest + CELL_CHROME);
  });
}

/**
 * Merge runtime resize widths with config-backed widths: runtime wins.
 * Used as a single lookup helper for header cells.
 */
export function resolveEffectiveWidth(
  runtime: number | undefined,
  configured: number | undefined,
): number | undefined {
  if (runtime != null) return runtime;
  return configured;
}
