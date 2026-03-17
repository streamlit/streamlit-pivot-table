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
import type { AggregationType } from "./types";
import {
  createAggregator,
  getAggregatorFactory,
  toNumber,
  AGGREGATOR_REGISTRY,
} from "./aggregators";
import { AGGREGATION_TYPES, AGGREGATOR_CLASS } from "./types";

describe("toNumber", () => {
  it("converts valid numbers", () => {
    expect(toNumber(42)).toBe(42);
    expect(toNumber(3.14)).toBe(3.14);
    expect(toNumber("100")).toBe(100);
    expect(toNumber("3.5")).toBe(3.5);
  });

  it("converts BigInt values (Arrow int64)", () => {
    expect(toNumber(BigInt(42))).toBe(42);
    expect(toNumber(BigInt(-7))).toBe(-7);
    expect(toNumber(BigInt(0))).toBe(0);
  });

  it("returns null for non-numeric values", () => {
    expect(toNumber(null)).toBeNull();
    expect(toNumber(undefined)).toBeNull();
    expect(toNumber("")).toBeNull();
    expect(toNumber("  ")).toBeNull();
    expect(toNumber("abc")).toBeNull();
    expect(toNumber(NaN)).toBeNull();
  });
});

describe("SumAggregator", () => {
  it("sums numeric values", () => {
    const agg = createAggregator("sum");
    agg.push(10);
    agg.push(20);
    agg.push(30);
    expect(agg.value()).toBe(60);
    expect(agg.count()).toBe(3);
  });

  it("returns null when no non-null values pushed", () => {
    const agg = createAggregator("sum");
    agg.push(null);
    agg.push(undefined);
    expect(agg.value()).toBeNull();
    expect(agg.count()).toBe(0);
  });

  it("excludes nulls from sum", () => {
    const agg = createAggregator("sum");
    agg.push(10);
    agg.push(null);
    agg.push(20);
    expect(agg.value()).toBe(30);
    expect(agg.count()).toBe(2);
  });

  it("handles string-encoded numbers", () => {
    const agg = createAggregator("sum");
    agg.push("5");
    agg.push("15");
    expect(agg.value()).toBe(20);
  });
});

describe("AvgAggregator", () => {
  it("computes average", () => {
    const agg = createAggregator("avg");
    agg.push(10);
    agg.push(20);
    agg.push(30);
    expect(agg.value()).toBe(20);
  });

  it("returns null for empty input", () => {
    const agg = createAggregator("avg");
    expect(agg.value()).toBeNull();
  });

  it("excludes nulls", () => {
    const agg = createAggregator("avg");
    agg.push(10);
    agg.push(null);
    agg.push(20);
    expect(agg.value()).toBe(15);
    expect(agg.count()).toBe(2);
  });
});

describe("CountAggregator", () => {
  it("counts non-null values", () => {
    const agg = createAggregator("count");
    agg.push(1);
    agg.push("a");
    agg.push(null);
    agg.push(undefined);
    agg.push(0);
    expect(agg.value()).toBe(3);
  });

  it("returns 0 for all nulls", () => {
    const agg = createAggregator("count");
    agg.push(null);
    expect(agg.value()).toBe(0);
  });
});

describe("MinAggregator", () => {
  it("finds minimum", () => {
    const agg = createAggregator("min");
    agg.push(30);
    agg.push(10);
    agg.push(20);
    expect(agg.value()).toBe(10);
  });

  it("returns null for empty input", () => {
    const agg = createAggregator("min");
    expect(agg.value()).toBeNull();
  });

  it("handles negative numbers", () => {
    const agg = createAggregator("min");
    agg.push(-5);
    agg.push(0);
    agg.push(5);
    expect(agg.value()).toBe(-5);
  });
});

describe("MaxAggregator", () => {
  it("finds maximum", () => {
    const agg = createAggregator("max");
    agg.push(30);
    agg.push(10);
    agg.push(20);
    expect(agg.value()).toBe(30);
  });

  it("returns null for empty input", () => {
    const agg = createAggregator("max");
    expect(agg.value()).toBeNull();
  });
});

describe("commutativity", () => {
  it.each(["sum", "avg", "count", "min", "max"] as const)(
    "%s produces same result regardless of push order",
    (type) => {
      const values = [5, 3, 8, 1, null, 7, 2];
      const reversed = [...values].reverse();

      const agg1 = createAggregator(type);
      for (const v of values) agg1.push(v);

      const agg2 = createAggregator(type);
      for (const v of reversed) agg2.push(v);

      expect(agg1.value()).toBe(agg2.value());
      expect(agg1.count()).toBe(agg2.count());
    },
  );
});

describe("format", () => {
  it("formats integers without decimals", () => {
    const agg = createAggregator("sum");
    agg.push(42);
    expect(agg.format("-")).toBe("42");
  });

  it("formats floats with 2 decimals", () => {
    const agg = createAggregator("avg");
    agg.push(10);
    agg.push(20);
    agg.push(31);
    expect(agg.format("-")).toBe("20.33");
  });

  it("returns empty cell value when null", () => {
    const agg = createAggregator("sum");
    expect(agg.format("N/A")).toBe("N/A");
  });
});

describe("AggregatorFactory", () => {
  it("registry covers all aggregation types", () => {
    for (const type of AGGREGATION_TYPES) {
      expect(AGGREGATOR_REGISTRY.has(type)).toBe(true);
    }
  });

  it("factory class tags match AGGREGATOR_CLASS", () => {
    for (const type of AGGREGATION_TYPES) {
      const factory = getAggregatorFactory(type);
      expect(factory.aggregatorClass).toBe(AGGREGATOR_CLASS[type]);
    }
  });

  it("throws for unknown type", () => {
    expect(() => getAggregatorFactory("unknown" as never)).toThrow(
      "Unknown aggregation type",
    );
  });

  it("all new aggregators tagged non-additive", () => {
    const newTypes: AggregationType[] = [
      "count_distinct",
      "median",
      "percentile_90",
      "first",
      "last",
    ];
    for (const t of newTypes) {
      expect(getAggregatorFactory(t).aggregatorClass).toBe("non-additive");
    }
  });
});

// ---------------------------------------------------------------------------
// Phase 3b: New aggregator classes
// ---------------------------------------------------------------------------

describe("CountDistinctAggregator", () => {
  it("counts unique values", () => {
    const agg = createAggregator("count_distinct");
    [1, 2, 2, 3].forEach((v) => agg.push(v));
    expect(agg.value()).toBe(3);
  });

  it("returns null for empty input", () => {
    const agg = createAggregator("count_distinct");
    expect(agg.value()).toBeNull();
  });
});

describe("MedianAggregator", () => {
  it("returns middle value for odd count", () => {
    const agg = createAggregator("median");
    [1, 2, 3].forEach((v) => agg.push(v));
    expect(agg.value()).toBe(2);
  });

  it("returns average of middle two for even count", () => {
    const agg = createAggregator("median");
    [1, 2, 3, 4].forEach((v) => agg.push(v));
    expect(agg.value()).toBe(2.5);
  });

  it("returns null for empty input", () => {
    const agg = createAggregator("median");
    expect(agg.value()).toBeNull();
  });

  it("handles unsorted input", () => {
    const agg = createAggregator("median");
    [5, 1, 3].forEach((v) => agg.push(v));
    expect(agg.value()).toBe(3);
  });
});

describe("PercentileAggregator (90th)", () => {
  it("computes 90th percentile", () => {
    const agg = createAggregator("percentile_90");
    for (let i = 1; i <= 100; i++) agg.push(i);
    expect(agg.value()).toBeCloseTo(90.1, 0);
  });

  it("returns single value for one element", () => {
    const agg = createAggregator("percentile_90");
    agg.push(42);
    expect(agg.value()).toBe(42);
  });

  it("returns null for empty input", () => {
    const agg = createAggregator("percentile_90");
    expect(agg.value()).toBeNull();
  });
});

describe("FirstAggregator", () => {
  it("returns first pushed value", () => {
    const agg = createAggregator("first");
    [10, 20, 30].forEach((v) => agg.push(v));
    expect(agg.value()).toBe(10);
  });

  it("returns null for empty input", () => {
    const agg = createAggregator("first");
    expect(agg.value()).toBeNull();
  });
});

describe("LastAggregator", () => {
  it("returns last pushed value", () => {
    const agg = createAggregator("last");
    [10, 20, 30].forEach((v) => agg.push(v));
    expect(agg.value()).toBe(30);
  });

  it("returns null for empty input", () => {
    const agg = createAggregator("last");
    expect(agg.value()).toBeNull();
  });
});
