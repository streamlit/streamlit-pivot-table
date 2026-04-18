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
