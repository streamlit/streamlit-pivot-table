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
 * styleHelpers — shared utilities for converting PivotStyle to CSS custom
 * properties and modifier class names.
 *
 * All helpers operate on the CSS modules `styles` object passed in by the
 * caller so this file has no direct CSS module dependency.
 */

import type { CSSProperties } from "react";
import type { PivotStyle, RegionStyle } from "../engine/types";

// ---------------------------------------------------------------------------
// Module augmentation: allow CSS custom properties on CSSProperties
// @types/react removed the index signature ("closed typing").  We need the
// augmentation so `--pivot-bg` etc. are assignable without double-casting.
// ---------------------------------------------------------------------------
declare module "react" {
  interface CSSProperties {
    [key: `--${string}`]: string | number | undefined;
  }
}

// ---------------------------------------------------------------------------
// Region → CSS variable slug mapping
//
// API naming note: row_total and column_total are intentionally "inverted"
// relative to their CSS class names:
//   row_total    = grand total of each row   → .totalsCol cells (rightmost col)
//   column_total = grand total of each column → .totalsRow cells (bottom row)
// The slug used in --pivot-*-bg follows the API name, not the CSS class name.
// ---------------------------------------------------------------------------
type RegionKey =
  | "column_header"
  | "row_header"
  | "data_cell"
  | "row_total"
  | "column_total"
  | "subtotal";

const REGION_MAP: Array<[RegionKey, string]> = [
  ["column_header", "column-header"],
  ["row_header", "row-header"],
  ["data_cell", "data-cell"],
  ["row_total", "row-total"], // targets .totalsCol cells via CSS selectors
  ["column_total", "column-total"], // targets .totalsRow cells via CSS selectors
  ["subtotal", "subtotal"],
];

// ---------------------------------------------------------------------------
// Density → virtualized row height mapping
// ---------------------------------------------------------------------------
export const DENSITY_ROW_HEIGHT: Record<string, number> = {
  compact: 24,
  default: 36,
  comfortable: 44,
};

// ---------------------------------------------------------------------------
// styleToCSS: convert PivotStyle to a CSSProperties object of --pivot-* vars
//
// stripe_color / row_hover_color === null are handled by modifier classes
// (stripesOff / hoverOff), NOT by setting --pivot-stripe-color: transparent.
// Only string values get a var; null triggers the class-based disable path.
// ---------------------------------------------------------------------------
export function styleToCSS(style?: PivotStyle): CSSProperties {
  if (!style) return {};
  const v: Record<string, string> = {};

  if (style.font_size) v["--pivot-font-size"] = style.font_size;
  if (style.background_color) v["--pivot-bg"] = style.background_color;
  if (style.text_color) v["--pivot-color"] = style.text_color;
  if (style.border_color) v["--pivot-border-color"] = style.border_color;

  // Only emit vars for real color strings; null → handled by modifier class
  if (typeof style.stripe_color === "string") {
    v["--pivot-stripe-color"] = style.stripe_color;
  }
  if (typeof style.row_hover_color === "string") {
    v["--pivot-row-hover-bg"] = style.row_hover_color;
  }

  for (const [key, slug] of REGION_MAP) {
    const r = style[key] as RegionStyle | undefined;
    if (!r) continue;
    if (r.background_color) v[`--pivot-${slug}-bg`] = r.background_color;
    if (r.text_color) v[`--pivot-${slug}-color`] = r.text_color;
    if (r.font_weight) v[`--pivot-${slug}-font-weight`] = r.font_weight;
    if (r.vertical_align)
      v[`--pivot-${slug}-vertical-align`] = r.vertical_align;
  }

  return v as CSSProperties;
}

// ---------------------------------------------------------------------------
// Modifier class helpers
// Each function returns "" when the modifier is not applicable so callers
// can safely join the array and filter(Boolean).
// ---------------------------------------------------------------------------

export function densityClass(
  style: PivotStyle | undefined,
  styles: Record<string, string>,
): string {
  if (style?.density === "compact") return styles.densityCompact ?? "";
  if (style?.density === "comfortable") return styles.densityComfortable ?? "";
  return "";
}

export function bordersClass(
  style: PivotStyle | undefined,
  styles: Record<string, string>,
): string {
  switch (style?.borders) {
    case "outer":
      return styles.bordersOuter ?? "";
    case "rows":
      return styles.bordersRows ?? "";
    case "columns":
      return styles.bordersColumns ?? "";
    case "none":
      return styles.bordersNone ?? "";
    default:
      return ""; // "all" or unset → default (all borders shown)
  }
}

/**
 * Returns the .stripesOff class when stripe_color is explicitly null.
 * This scopes out the stripe rules entirely rather than painting transparent
 * over them — important because each stripe selector has its own color-mix
 * fallback percentage and a transparent var wouldn't correctly disable all of
 * them (it would collapse them to the table background instead of removing them).
 */
export function stripesOffClass(
  style: PivotStyle | undefined,
  styles: Record<string, string>,
): string {
  return style?.stripe_color === null ? (styles.stripesOff ?? "") : "";
}

/**
 * Returns the .hoverOff class when row_hover_color is explicitly null.
 * This scopes out ALL hover rules (generic + hierarchy-specific + CF companion
 * box-shadow) rather than painting them transparent.
 */
export function hoverOffClass(
  style: PivotStyle | undefined,
  styles: Record<string, string>,
): string {
  return style?.row_hover_color === null ? (styles.hoverOff ?? "") : "";
}

// ---------------------------------------------------------------------------
// Per-measure data cell inline style
//
// Returns undefined when there is no override for this field so callers can
// spread it safely (spreading undefined in an object literal is a no-op in
// ES2018+).  Must be spread BEFORE any conditional-formatting style so CF
// still wins on qualifying cells.
//
// Apply only to non-total data cells.  Totals branches should NOT call this
// helper; they take styling from row_total / column_total region vars instead.
// ---------------------------------------------------------------------------
export function dataCellByMeasureStyle(
  valueField: string | undefined,
  style: PivotStyle | undefined,
): CSSProperties | undefined {
  if (!valueField || !style?.data_cell_by_measure) return undefined;
  const r = style.data_cell_by_measure[valueField];
  if (!r) return undefined;
  const s: CSSProperties = {};
  if (r.background_color) s.backgroundColor = r.background_color;
  if (r.text_color) s.color = r.text_color;
  if (r.font_weight)
    s.fontWeight = r.font_weight as CSSProperties["fontWeight"];
  if (r.vertical_align)
    s.verticalAlign = r.vertical_align as CSSProperties["verticalAlign"];
  return Object.keys(s).length > 0 ? s : undefined;
}
