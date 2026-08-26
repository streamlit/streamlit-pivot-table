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

import { describe, it, expect } from "vitest";
import {
  WIDTH_SMALL,
  WIDTH_MEDIUM,
  WIDTH_LARGE,
  WIDTH_MIN,
  WIDTH_MAX,
  resolveFieldWidth,
  resolveEffectiveWidth,
  estimateRowHeaderWidths,
  ROW_HEADER_WIDTH_CAP,
} from "./fieldWidthResolver";
import { makeConfig } from "../test-utils";

describe("resolveFieldWidth", () => {
  it("returns undefined when no field_widths is set", () => {
    const config = makeConfig();
    expect(resolveFieldWidth(config, "revenue")).toBeUndefined();
  });

  it("returns undefined when field is missing from field_widths", () => {
    const config = makeConfig({ field_widths: { revenue: 150 } });
    expect(resolveFieldWidth(config, "profit")).toBeUndefined();
  });

  it("returns undefined when field argument is undefined", () => {
    const config = makeConfig({ field_widths: { revenue: 150 } });
    expect(resolveFieldWidth(config, undefined)).toBeUndefined();
  });

  it("resolves preset 'small' to WIDTH_SMALL", () => {
    const config = makeConfig({ field_widths: { revenue: "small" } });
    expect(resolveFieldWidth(config, "revenue")).toBe(WIDTH_SMALL);
  });

  it("resolves preset 'medium' to WIDTH_MEDIUM", () => {
    const config = makeConfig({ field_widths: { revenue: "medium" } });
    expect(resolveFieldWidth(config, "revenue")).toBe(WIDTH_MEDIUM);
  });

  it("resolves preset 'large' to WIDTH_LARGE", () => {
    const config = makeConfig({ field_widths: { revenue: "large" } });
    expect(resolveFieldWidth(config, "revenue")).toBe(WIDTH_LARGE);
  });

  it("returns numeric width as-is when within bounds", () => {
    const config = makeConfig({ field_widths: { revenue: 150 } });
    expect(resolveFieldWidth(config, "revenue")).toBe(150);
  });

  it("accepts the minimum bound", () => {
    const config = makeConfig({ field_widths: { revenue: WIDTH_MIN } });
    expect(resolveFieldWidth(config, "revenue")).toBe(WIDTH_MIN);
  });

  it("accepts the maximum bound", () => {
    const config = makeConfig({ field_widths: { revenue: WIDTH_MAX } });
    expect(resolveFieldWidth(config, "revenue")).toBe(WIDTH_MAX);
  });

  it("clamps below WIDTH_MIN to undefined", () => {
    const config = makeConfig({
      field_widths: { revenue: WIDTH_MIN - 1 },
    });
    expect(resolveFieldWidth(config, "revenue")).toBeUndefined();
  });

  it("clamps above WIDTH_MAX to undefined", () => {
    const config = makeConfig({
      field_widths: { revenue: WIDTH_MAX + 1 },
    });
    expect(resolveFieldWidth(config, "revenue")).toBeUndefined();
  });

  it("returns undefined for unknown preset strings", () => {
    const config = makeConfig({
      field_widths: {
        revenue: "extra-large" as unknown as "small" | "medium" | "large",
      },
    });
    expect(resolveFieldWidth(config, "revenue")).toBeUndefined();
  });

  it("rounds fractional numbers to integer pixels", () => {
    const config = makeConfig({ field_widths: { revenue: 150.7 } });
    expect(resolveFieldWidth(config, "revenue")).toBe(151);
  });

  it("returns undefined for NaN / Infinity", () => {
    const config = makeConfig({
      field_widths: { a: NaN, b: Infinity },
    });
    expect(resolveFieldWidth(config, "a")).toBeUndefined();
    expect(resolveFieldWidth(config, "b")).toBeUndefined();
  });
});

describe("resolveEffectiveWidth", () => {
  it("prefers runtime width when present", () => {
    expect(resolveEffectiveWidth(200, 150)).toBe(200);
  });

  it("falls back to configured width when runtime is undefined", () => {
    expect(resolveEffectiveWidth(undefined, 150)).toBe(150);
  });

  it("returns undefined when both are undefined", () => {
    expect(resolveEffectiveWidth(undefined, undefined)).toBeUndefined();
  });

  it("treats runtime width of 0 as a valid override (not undefined)", () => {
    expect(resolveEffectiveWidth(0, 150)).toBe(0);
  });
});

describe("estimateRowHeaderWidths", () => {
  const MIN = 120;

  it("returns no widths when there are no row dimensions", () => {
    expect(estimateRowHeaderWidths([], [], false, MIN)).toEqual([]);
  });

  it("never goes below the minimum width for short labels", () => {
    const widths = estimateRowHeaderWidths(["A"], [["x"], ["y"]], false, MIN);
    expect(widths).toEqual([MIN]);
  });

  it("widens past the minimum to fit a long dimension label", () => {
    const [width] = estimateRowHeaderWidths(
      ["Merchant_State"],
      [["CA"]],
      false,
      MIN,
    );
    expect(width).toBeGreaterThan(MIN);
  });

  it("widens to fit long values even when the label is short", () => {
    const [width] = estimateRowHeaderWidths(
      ["City"],
      [["San Francisco International"]],
      false,
      MIN,
    );
    expect(width).toBeGreaterThan(MIN);
  });

  it("gives hierarchy layout a single width covering the whole breadcrumb", () => {
    const widths = estimateRowHeaderWidths(
      ["Merchant_State", "Merchant_City"],
      [["California", "San Francisco"]],
      true,
      MIN,
    );
    expect(widths).toHaveLength(1);
    // Both dimension names plus their chevrons have to fit side by side.
    expect(widths[0]).toBeGreaterThan(
      estimateRowHeaderWidths(
        ["Merchant_State"],
        [["California"]],
        true,
        MIN,
      )[0],
    );
  });

  it("accounts for the indent applied to nested hierarchy levels", () => {
    const nested = estimateRowHeaderWidths(
      ["a", "b"],
      [["x", "San Francisco"]],
      true,
      MIN,
    );
    const flat = estimateRowHeaderWidths(
      ["a", "b"],
      [["San Francisco", "x"]],
      true,
      MIN,
    );
    expect(nested[0]).toBeGreaterThan(flat[0]);
  });

  it("caps the estimate so one outlier label cannot take over the table", () => {
    const [width] = estimateRowHeaderWidths(
      ["City"],
      [["x".repeat(500)]],
      false,
      MIN,
    );
    expect(width).toBe(ROW_HEADER_WIDTH_CAP);
  });

  it("produces one width per dimension in table layout", () => {
    const widths = estimateRowHeaderWidths(
      ["Country", "State", "City"],
      [["US", "CA", "SF"]],
      false,
      MIN,
    );
    expect(widths).toHaveLength(3);
  });
});
