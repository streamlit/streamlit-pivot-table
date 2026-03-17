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
  formatNumber,
  formatWithPattern,
  formatPercent,
  isSupportedFormatPattern,
} from "./formatters";

describe("formatNumber", () => {
  it("formats integers without decimals", () => {
    expect(formatNumber(1234)).toBe("1,234");
  });

  it("formats decimals with 2 places", () => {
    expect(formatNumber(1234.5)).toBe("1,234.50");
  });
});

describe("formatWithPattern", () => {
  it("formats grouped integer: ,.0f", () => {
    expect(formatWithPattern(1234.5, ",.0f")).toBe("1,235");
  });

  it("formats grouped 2-decimal: ,.2f", () => {
    expect(formatWithPattern(1234.5, ",.2f")).toBe("1,234.50");
  });

  it("formats currency: $,.0f", () => {
    const result = formatWithPattern(1234.5, "$,.0f");
    expect(result).toContain("1,235");
    expect(result).toContain("$");
  });

  it("formats currency 2-decimal: $,.2f", () => {
    const result = formatWithPattern(1234.5, "$,.2f");
    expect(result).toContain("1,234.50");
    expect(result).toContain("$");
  });

  it("formats percent: .1%", () => {
    expect(formatWithPattern(0.452, ".1%")).toBe("45.2%");
  });

  it("formats percent integer: .0%", () => {
    expect(formatWithPattern(0.452, ".0%")).toBe("45%");
  });

  it("caches formatters for same pattern", () => {
    const r1 = formatWithPattern(100, ",.0f");
    const r2 = formatWithPattern(200, ",.0f");
    expect(r1).toBe("100");
    expect(r2).toBe("200");
  });

  it("falls back for unrecognised pattern without grouping", () => {
    // Unrecognised patterns create a basic formatter without grouping
    const result = formatWithPattern(1234, "???");
    expect(result).toBe("1234");
  });
});

describe("formatPercent", () => {
  it("formats 0.452 as 45.2%", () => {
    expect(formatPercent(0.452)).toBe("45.2%");
  });

  it("formats 1.0 as 100.0%", () => {
    expect(formatPercent(1.0)).toBe("100.0%");
  });
});

describe("isSupportedFormatPattern", () => {
  it("accepts supported patterns", () => {
    expect(isSupportedFormatPattern(".1%")).toBe(true);
    expect(isSupportedFormatPattern("$,.0f")).toBe(true);
    expect(isSupportedFormatPattern(",.2f")).toBe(true);
  });

  it("rejects unsupported patterns", () => {
    expect(isSupportedFormatPattern("abc")).toBe(false);
    expect(isSupportedFormatPattern("$,.1%")).toBe(false);
    expect(isSupportedFormatPattern("")).toBe(false);
  });
});
