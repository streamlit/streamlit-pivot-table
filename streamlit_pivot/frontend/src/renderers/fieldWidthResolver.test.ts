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
