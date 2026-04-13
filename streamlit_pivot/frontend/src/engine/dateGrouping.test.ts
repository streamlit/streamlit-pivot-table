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
import {
  extractParentBuckets,
  buildModifiedColKey,
  formatTemporalParentLabel,
  monthToQuarter,
} from "./dateGrouping";
import { getTemporalHierarchyLevels } from "./types";

describe("getTemporalHierarchyLevels", () => {
  it("year → [year]", () => {
    expect(getTemporalHierarchyLevels("year")).toEqual(["year"]);
  });
  it("quarter → [year, quarter]", () => {
    expect(getTemporalHierarchyLevels("quarter")).toEqual(["year", "quarter"]);
  });
  it("month → [year, quarter, month]", () => {
    expect(getTemporalHierarchyLevels("month")).toEqual([
      "year",
      "quarter",
      "month",
    ]);
  });
  it("day → [year, month, day] (skips quarter)", () => {
    expect(getTemporalHierarchyLevels("day")).toEqual(["year", "month", "day"]);
  });
  it("week → [year, week]", () => {
    expect(getTemporalHierarchyLevels("week")).toEqual(["year", "week"]);
  });
});

describe("monthToQuarter", () => {
  it("maps months to correct quarters", () => {
    expect(monthToQuarter(1)).toBe(1);
    expect(monthToQuarter(3)).toBe(1);
    expect(monthToQuarter(4)).toBe(2);
    expect(monthToQuarter(6)).toBe(2);
    expect(monthToQuarter(7)).toBe(3);
    expect(monthToQuarter(9)).toBe(3);
    expect(monthToQuarter(10)).toBe(4);
    expect(monthToQuarter(12)).toBe(4);
  });
});

describe("extractParentBuckets", () => {
  it("returns [] for year grain (no parents)", () => {
    expect(extractParentBuckets("2024", "year")).toEqual([]);
  });

  it("returns [year] for quarter grain", () => {
    expect(extractParentBuckets("2024-Q2", "quarter")).toEqual(["2024"]);
  });

  it("returns [year, quarter] for month grain", () => {
    expect(extractParentBuckets("2024-03", "month")).toEqual([
      "2024",
      "2024-Q1",
    ]);
  });

  it("returns [year, month] for day grain (skips quarter)", () => {
    expect(extractParentBuckets("2024-03-15", "day")).toEqual([
      "2024",
      "2024-03",
    ]);
  });

  it("returns [year] for week grain", () => {
    expect(extractParentBuckets("2024-W05", "week")).toEqual(["2024"]);
  });

  it("handles month-to-quarter mapping correctly", () => {
    expect(extractParentBuckets("2024-04", "month")).toEqual([
      "2024",
      "2024-Q2",
    ]);
    expect(extractParentBuckets("2024-10", "month")).toEqual([
      "2024",
      "2024-Q4",
    ]);
  });
});

describe("buildModifiedColKey", () => {
  it("replaces temporal segment with tp: prefix", () => {
    const result = buildModifiedColKey(["2024-03"], 0, "order_date", "2024");
    expect(result).toEqual(["tp:order_date:2024"]);
  });

  it("preserves sibling dimensions", () => {
    const result = buildModifiedColKey(
      ["East", "2024-03"],
      1,
      "order_date",
      "2024",
    );
    expect(result).toEqual(["East", "tp:order_date:2024"]);
  });

  it("does not mutate original array", () => {
    const original = ["East", "2024-03"];
    buildModifiedColKey(original, 1, "order_date", "2024");
    expect(original).toEqual(["East", "2024-03"]);
  });
});

describe("formatTemporalParentLabel", () => {
  it("formats year label", () => {
    expect(formatTemporalParentLabel("2024", "year")).toBe("2024");
  });

  it("formats quarter label", () => {
    const label = formatTemporalParentLabel("2024-Q2", "quarter");
    expect(label).toContain("Q2");
    expect(label).toContain("2024");
  });

  it("formats month label", () => {
    const label = formatTemporalParentLabel("2024-03", "month");
    expect(label).toBeTruthy();
  });
});
