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

import {
  type AggregationType,
  type AggregatorClass,
  AGGREGATOR_CLASS,
} from "./types";
import { formatNumber } from "./formatters";

export interface Aggregator {
  push(value: unknown): void;
  value(): number | null;
  count(): number;
  format(emptyCellValue: string): string;
}

export interface AggregatorFactory {
  type: AggregationType;
  aggregatorClass: AggregatorClass;
  create(): Aggregator;
}

function toNumber(v: unknown): number | null {
  if (v == null) return null;
  if (typeof v === "number") return Number.isNaN(v) ? null : v;
  if (typeof v === "bigint") return Number(v);
  if (typeof v === "string") {
    if (v.trim() === "") return null;
    const n = Number(v);
    return Number.isNaN(n) ? null : n;
  }
  return null;
}

class SumAggregator implements Aggregator {
  private _sum = 0;
  private _count = 0;
  push(value: unknown): void {
    const n = toNumber(value);
    if (n !== null) {
      this._sum += n;
      this._count++;
    }
  }
  value(): number | null {
    return this._count === 0 ? null : this._sum;
  }
  count(): number {
    return this._count;
  }
  format(emptyCellValue: string): string {
    const v = this.value();
    return v === null ? emptyCellValue : formatNumber(v);
  }
}

class AvgAggregator implements Aggregator {
  private _sum = 0;
  private _count = 0;
  push(value: unknown): void {
    const n = toNumber(value);
    if (n !== null) {
      this._sum += n;
      this._count++;
    }
  }
  value(): number | null {
    return this._count === 0 ? null : this._sum / this._count;
  }
  count(): number {
    return this._count;
  }
  format(emptyCellValue: string): string {
    const v = this.value();
    return v === null ? emptyCellValue : formatNumber(v);
  }
}

class CountAggregator implements Aggregator {
  private _count = 0;
  push(value: unknown): void {
    if (value != null) {
      this._count++;
    }
  }
  value(): number | null {
    return this._count;
  }
  count(): number {
    return this._count;
  }
  format(emptyCellValue: string): string {
    const v = this.value();
    return v === null ? emptyCellValue : String(v);
  }
}

class MinAggregator implements Aggregator {
  private _min: number | null = null;
  private _count = 0;
  push(value: unknown): void {
    const n = toNumber(value);
    if (n !== null) {
      this._min = this._min === null ? n : Math.min(this._min, n);
      this._count++;
    }
  }
  value(): number | null {
    return this._min;
  }
  count(): number {
    return this._count;
  }
  format(emptyCellValue: string): string {
    const v = this.value();
    return v === null ? emptyCellValue : formatNumber(v);
  }
}

class MaxAggregator implements Aggregator {
  private _max: number | null = null;
  private _count = 0;
  push(value: unknown): void {
    const n = toNumber(value);
    if (n !== null) {
      this._max = this._max === null ? n : Math.max(this._max, n);
      this._count++;
    }
  }
  value(): number | null {
    return this._max;
  }
  count(): number {
    return this._count;
  }
  format(emptyCellValue: string): string {
    const v = this.value();
    return v === null ? emptyCellValue : formatNumber(v);
  }
}

class CountDistinctAggregator implements Aggregator {
  private _set = new Set<string>();
  private _count = 0;
  push(value: unknown): void {
    if (value != null) {
      this._set.add(String(value));
      this._count++;
    }
  }
  value(): number | null {
    return this._count === 0 ? null : this._set.size;
  }
  count(): number {
    return this._count;
  }
  format(emptyCellValue: string): string {
    const v = this.value();
    return v === null ? emptyCellValue : String(v);
  }
}

class MedianAggregator implements Aggregator {
  private _values: number[] = [];
  push(value: unknown): void {
    const n = toNumber(value);
    if (n !== null) this._values.push(n);
  }
  value(): number | null {
    if (this._values.length === 0) return null;
    const sorted = [...this._values].sort((a, b) => a - b);
    const mid = Math.floor(sorted.length / 2);
    return sorted.length % 2 !== 0
      ? sorted[mid]
      : (sorted[mid - 1] + sorted[mid]) / 2;
  }
  count(): number {
    return this._values.length;
  }
  format(emptyCellValue: string): string {
    const v = this.value();
    return v === null ? emptyCellValue : formatNumber(v);
  }
}

class PercentileAggregator implements Aggregator {
  private _values: number[] = [];
  private _k: number;
  constructor(k: number) {
    this._k = k;
  }
  push(value: unknown): void {
    const n = toNumber(value);
    if (n !== null) this._values.push(n);
  }
  value(): number | null {
    if (this._values.length === 0) return null;
    const sorted = [...this._values].sort((a, b) => a - b);
    const idx = (this._k / 100) * (sorted.length - 1);
    const lo = Math.floor(idx);
    const hi = Math.ceil(idx);
    if (lo === hi) return sorted[lo];
    return sorted[lo] + (sorted[hi] - sorted[lo]) * (idx - lo);
  }
  count(): number {
    return this._values.length;
  }
  format(emptyCellValue: string): string {
    const v = this.value();
    return v === null ? emptyCellValue : formatNumber(v);
  }
}

class FirstAggregator implements Aggregator {
  private _first: number | null = null;
  private _count = 0;
  push(value: unknown): void {
    const n = toNumber(value);
    if (n !== null) {
      if (this._count === 0) this._first = n;
      this._count++;
    }
  }
  value(): number | null {
    return this._first;
  }
  count(): number {
    return this._count;
  }
  format(emptyCellValue: string): string {
    const v = this.value();
    return v === null ? emptyCellValue : formatNumber(v);
  }
}

class LastAggregator implements Aggregator {
  private _last: number | null = null;
  private _count = 0;
  push(value: unknown): void {
    const n = toNumber(value);
    if (n !== null) {
      this._last = n;
      this._count++;
    }
  }
  value(): number | null {
    return this._last;
  }
  count(): number {
    return this._count;
  }
  format(emptyCellValue: string): string {
    const v = this.value();
    return v === null ? emptyCellValue : formatNumber(v);
  }
}

const AGGREGATOR_REGISTRY: Map<AggregationType, AggregatorFactory> = new Map([
  [
    "sum",
    {
      type: "sum" as const,
      aggregatorClass: AGGREGATOR_CLASS.sum,
      create: () => new SumAggregator(),
    },
  ],
  [
    "avg",
    {
      type: "avg" as const,
      aggregatorClass: AGGREGATOR_CLASS.avg,
      create: () => new AvgAggregator(),
    },
  ],
  [
    "count",
    {
      type: "count" as const,
      aggregatorClass: AGGREGATOR_CLASS.count,
      create: () => new CountAggregator(),
    },
  ],
  [
    "min",
    {
      type: "min" as const,
      aggregatorClass: AGGREGATOR_CLASS.min,
      create: () => new MinAggregator(),
    },
  ],
  [
    "max",
    {
      type: "max" as const,
      aggregatorClass: AGGREGATOR_CLASS.max,
      create: () => new MaxAggregator(),
    },
  ],
  [
    "count_distinct",
    {
      type: "count_distinct" as const,
      aggregatorClass: AGGREGATOR_CLASS.count_distinct,
      create: () => new CountDistinctAggregator(),
    },
  ],
  [
    "median",
    {
      type: "median" as const,
      aggregatorClass: AGGREGATOR_CLASS.median,
      create: () => new MedianAggregator(),
    },
  ],
  [
    "percentile_90",
    {
      type: "percentile_90" as const,
      aggregatorClass: AGGREGATOR_CLASS.percentile_90,
      create: () => new PercentileAggregator(90),
    },
  ],
  [
    "first",
    {
      type: "first" as const,
      aggregatorClass: AGGREGATOR_CLASS.first,
      create: () => new FirstAggregator(),
    },
  ],
  [
    "last",
    {
      type: "last" as const,
      aggregatorClass: AGGREGATOR_CLASS.last,
      create: () => new LastAggregator(),
    },
  ],
]);

export function getAggregatorFactory(type: AggregationType): AggregatorFactory {
  const factory = AGGREGATOR_REGISTRY.get(type);
  if (!factory) {
    throw new Error(`Unknown aggregation type: ${type}`);
  }
  return factory;
}

export function createAggregator(type: AggregationType): Aggregator {
  return getAggregatorFactory(type).create();
}

export { toNumber, AGGREGATOR_REGISTRY };
