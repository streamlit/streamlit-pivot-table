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

import { describe, expect, it } from "vitest";
import { PivotData, type DataRecord } from "../engine/PivotData";
import type {
  PivotConfigV1,
  ColorScaleRule,
  DataBarsRule,
  ThresholdRule,
} from "../engine/types";
import { computeCellStyle, computeColumnStats } from "./ConditionalFormat";

function makeConfig(overrides: Partial<PivotConfigV1> = {}): PivotConfigV1 {
  return {
    version: 1,
    rows: ["region"],
    columns: ["year"],
    values: ["revenue"],
    aggregation: "sum",
    show_totals: true,
    empty_cell_value: "-",
    interactive: true,
    ...overrides,
  };
}

const SAMPLE_DATA: DataRecord[] = [
  { region: "US", year: "2023", revenue: 100 },
  { region: "US", year: "2024", revenue: 200 },
  { region: "EU", year: "2023", revenue: 300 },
  { region: "EU", year: "2024", revenue: 400 },
];

describe("computeColumnStats", () => {
  it("computes min, max, range for a value field", () => {
    const config = makeConfig();
    const pd = new PivotData(SAMPLE_DATA, config);
    const stats = computeColumnStats(pd, "revenue");
    expect(stats.min).toBe(100);
    expect(stats.max).toBe(400);
    expect(stats.range).toBe(300);
  });

  it("caches results for same pivotData instance", () => {
    const config = makeConfig();
    const pd = new PivotData(SAMPLE_DATA, config);
    const s1 = computeColumnStats(pd, "revenue");
    const s2 = computeColumnStats(pd, "revenue");
    expect(s1).toBe(s2);
  });
});

describe("computeCellStyle - color scale", () => {
  const rule: ColorScaleRule = {
    type: "color_scale",
    apply_to: [],
    min_color: "#ff0000",
    max_color: "#0000ff",
  };

  it("returns min_color for minimum value", () => {
    const config = makeConfig({ conditional_formatting: [rule] });
    const pd = new PivotData(SAMPLE_DATA, config);
    const style = computeCellStyle(100, "revenue", [rule], pd, false);
    expect(style?.backgroundColor).toBe("rgb(255, 0, 0)");
  });

  it("returns max_color for maximum value", () => {
    const config = makeConfig({ conditional_formatting: [rule] });
    const pd = new PivotData(SAMPLE_DATA, config);
    const style = computeCellStyle(400, "revenue", [rule], pd, false);
    expect(style?.backgroundColor).toBe("rgb(0, 0, 255)");
  });

  it("returns blended color for midpoint", () => {
    const config = makeConfig({ conditional_formatting: [rule] });
    const pd = new PivotData(SAMPLE_DATA, config);
    const style = computeCellStyle(250, "revenue", [rule], pd, false);
    expect(style?.backgroundColor).toBe("rgb(128, 0, 128)");
  });

  it("returns undefined for null value", () => {
    const config = makeConfig({ conditional_formatting: [rule] });
    const pd = new PivotData(SAMPLE_DATA, config);
    expect(
      computeCellStyle(null, "revenue", [rule], pd, false),
    ).toBeUndefined();
  });

  it("3-color scale uses mid_color", () => {
    const rule3: ColorScaleRule = {
      type: "color_scale",
      apply_to: [],
      min_color: "#ff0000",
      mid_color: "#ffffff",
      max_color: "#0000ff",
    };
    const config = makeConfig({ conditional_formatting: [rule3] });
    const pd = new PivotData(SAMPLE_DATA, config);
    const style = computeCellStyle(250, "revenue", [rule3], pd, false);
    expect(style?.backgroundColor).toBe("rgb(255, 255, 255)");
  });

  it("auto-sets dark text on light backgrounds", () => {
    const lightRule: ColorScaleRule = {
      type: "color_scale",
      apply_to: [],
      min_color: "#ffffff",
      max_color: "#ffffff",
    };
    const config = makeConfig({ conditional_formatting: [lightRule] });
    const pd = new PivotData(SAMPLE_DATA, config);
    const style = computeCellStyle(100, "revenue", [lightRule], pd, false);
    expect(style?.color).toBe("#11181c");
  });

  it("auto-sets light text on dark backgrounds", () => {
    const darkRule: ColorScaleRule = {
      type: "color_scale",
      apply_to: [],
      min_color: "#000000",
      max_color: "#000000",
    };
    const config = makeConfig({ conditional_formatting: [darkRule] });
    const pd = new PivotData(SAMPLE_DATA, config);
    const style = computeCellStyle(100, "revenue", [darkRule], pd, false);
    expect(style?.color).toBe("#f0f2f6");
  });
});

describe("computeCellStyle - data bars", () => {
  const rule: DataBarsRule = {
    type: "data_bars",
    apply_to: [],
    fill: "solid",
  };

  it("min value gets 0% width", () => {
    const config = makeConfig({ conditional_formatting: [rule] });
    const pd = new PivotData(SAMPLE_DATA, config);
    const style = computeCellStyle(100, "revenue", [rule], pd, false);
    expect(style?.background).toContain("0%");
  });

  it("max value gets 100% width", () => {
    const config = makeConfig({ conditional_formatting: [rule] });
    const pd = new PivotData(SAMPLE_DATA, config);
    const style = computeCellStyle(400, "revenue", [rule], pd, false);
    expect(style?.background).toContain("100%");
  });
});

describe("computeCellStyle - threshold", () => {
  const rule: ThresholdRule = {
    type: "threshold",
    apply_to: [],
    conditions: [
      { operator: "gt", value: 300, background: "#ff0000" },
      { operator: "between", value: 200, value2: 300, background: "#ffff00" },
      { operator: "lt", value: 200, background: "#00ff00" },
    ],
  };

  it("first matching condition wins", () => {
    const config = makeConfig({ conditional_formatting: [rule] });
    const pd = new PivotData(SAMPLE_DATA, config);
    expect(
      computeCellStyle(400, "revenue", [rule], pd, false)?.backgroundColor,
    ).toBe("#ff0000");
    expect(
      computeCellStyle(250, "revenue", [rule], pd, false)?.backgroundColor,
    ).toBe("#ffff00");
    expect(
      computeCellStyle(100, "revenue", [rule], pd, false)?.backgroundColor,
    ).toBe("#00ff00");
  });

  it("skips totals when include_totals is false (default)", () => {
    const config = makeConfig({ conditional_formatting: [rule] });
    const pd = new PivotData(SAMPLE_DATA, config);
    expect(computeCellStyle(400, "revenue", [rule], pd, true)).toBeUndefined();
  });

  it("applies to totals when include_totals is true", () => {
    const ruleWithTotals: ThresholdRule = { ...rule, include_totals: true };
    const config = makeConfig({ conditional_formatting: [ruleWithTotals] });
    const pd = new PivotData(SAMPLE_DATA, config);
    expect(
      computeCellStyle(400, "revenue", [ruleWithTotals], pd, true)
        ?.backgroundColor,
    ).toBe("#ff0000");
  });

  it("auto-sets contrast text when background is provided without color", () => {
    const lightBgRule: ThresholdRule = {
      type: "threshold",
      apply_to: [],
      conditions: [{ operator: "gt", value: 0, background: "#ffffff" }],
    };
    const config = makeConfig({ conditional_formatting: [lightBgRule] });
    const pd = new PivotData(SAMPLE_DATA, config);
    const style = computeCellStyle(400, "revenue", [lightBgRule], pd, false);
    expect(style?.backgroundColor).toBe("#ffffff");
    expect(style?.color).toBe("#11181c");
  });

  it("preserves explicit color when provided", () => {
    const explicitRule: ThresholdRule = {
      type: "threshold",
      apply_to: [],
      conditions: [
        { operator: "gt", value: 0, background: "#ffffff", color: "#ff0000" },
      ],
    };
    const config = makeConfig({ conditional_formatting: [explicitRule] });
    const pd = new PivotData(SAMPLE_DATA, config);
    const style = computeCellStyle(400, "revenue", [explicitRule], pd, false);
    expect(style?.color).toBe("#ff0000");
  });
});

describe("computeCellStyle - apply_to filtering", () => {
  it("skips rule when field not in apply_to", () => {
    const rule: ColorScaleRule = {
      type: "color_scale",
      apply_to: ["profit"],
      min_color: "#ff0000",
      max_color: "#0000ff",
    };
    const config = makeConfig({ conditional_formatting: [rule] });
    const pd = new PivotData(SAMPLE_DATA, config);
    expect(computeCellStyle(200, "revenue", [rule], pd, false)).toBeUndefined();
  });
});
