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

import { formatDateValue, normalizeToUTC } from "./formatters";
import type { ColumnType, DateGrain } from "./types";
import { getTemporalHierarchyLevels } from "./types";

const MONTH_SHORT = [
  "Jan",
  "Feb",
  "Mar",
  "Apr",
  "May",
  "Jun",
  "Jul",
  "Aug",
  "Sep",
  "Oct",
  "Nov",
  "Dec",
];

const MONTH_LONG = [
  "January",
  "February",
  "March",
  "April",
  "May",
  "June",
  "July",
  "August",
  "September",
  "October",
  "November",
  "December",
];

type ShiftMode = "previous" | "previous_year";

function pad2(n: number): string {
  return String(n).padStart(2, "0");
}

export function isTemporalColumnType(
  colType: ColumnType | undefined,
): colType is "date" | "datetime" {
  return colType === "date" || colType === "datetime";
}

function utcDateOnly(date: Date): Date {
  return new Date(
    Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()),
  );
}

function toTemporalDate(raw: unknown): Date | null {
  if (raw instanceof Date) return new Date(raw.getTime());
  if (typeof raw === "number" && isFinite(raw)) return new Date(raw);
  if (typeof raw === "string") {
    const d = new Date(normalizeToUTC(raw));
    if (!isNaN(d.getTime())) return d;
  }
  return null;
}

export function canonicalTemporalKey(
  raw: unknown,
  colType: "date" | "datetime",
): string | null {
  const d = toTemporalDate(raw);
  if (!d) return null;
  return colType === "datetime"
    ? d.toISOString()
    : utcDateOnly(d).toISOString().slice(0, 10);
}

function getIsoWeekParts(date: Date): { year: number; week: number } {
  const d = utcDateOnly(date);
  const day = d.getUTCDay() || 7;
  d.setUTCDate(d.getUTCDate() + 4 - day);
  const year = d.getUTCFullYear();
  const yearStart = new Date(Date.UTC(year, 0, 1));
  const diffDays = Math.floor((d.getTime() - yearStart.getTime()) / 86400000);
  return { year, week: Math.floor(diffDays / 7) + 1 };
}

function isoWeekStart(year: number, week: number): Date {
  const jan4 = new Date(Date.UTC(year, 0, 4));
  const jan4Day = jan4.getUTCDay() || 7;
  const monday = new Date(jan4);
  monday.setUTCDate(jan4.getUTCDate() - jan4Day + 1 + (week - 1) * 7);
  return utcDateOnly(monday);
}

function isoWeeksInYear(year: number): number {
  return getIsoWeekParts(new Date(Date.UTC(year, 11, 28))).week;
}

function parseBucketDate(key: string, grain: DateGrain): Date | null {
  if (!key) return null;
  if (grain === "year") {
    const year = Number(key);
    return Number.isFinite(year) ? new Date(Date.UTC(year, 0, 1)) : null;
  }
  if (grain === "quarter") {
    const match = key.match(/^(\d{4})-Q([1-4])$/);
    if (!match) return null;
    return new Date(Date.UTC(Number(match[1]), (Number(match[2]) - 1) * 3, 1));
  }
  if (grain === "month") {
    const match = key.match(/^(\d{4})-(\d{2})$/);
    if (!match) return null;
    return new Date(Date.UTC(Number(match[1]), Number(match[2]) - 1, 1));
  }
  if (grain === "week") {
    const match = key.match(/^(\d{4})-W(\d{2})$/);
    if (!match) return null;
    return isoWeekStart(Number(match[1]), Number(match[2]));
  }
  const d = new Date(`${key}T00:00:00.000Z`);
  return isNaN(d.getTime()) ? null : d;
}

export function bucketTemporalKey(
  raw: unknown,
  colType: "date" | "datetime",
  grain: DateGrain | undefined,
): string | null {
  if (!grain) return canonicalTemporalKey(raw, colType);
  const d = toTemporalDate(raw);
  if (!d) return null;
  const utc = utcDateOnly(d);
  if (grain === "year") {
    return String(utc.getUTCFullYear());
  }
  if (grain === "quarter") {
    return `${utc.getUTCFullYear()}-Q${Math.floor(utc.getUTCMonth() / 3) + 1}`;
  }
  if (grain === "month") {
    return `${utc.getUTCFullYear()}-${pad2(utc.getUTCMonth() + 1)}`;
  }
  if (grain === "week") {
    const iso = getIsoWeekParts(utc);
    return `${iso.year}-W${pad2(iso.week)}`;
  }
  return utc.toISOString().slice(0, 10);
}

function formatBucketPattern(
  key: string,
  grain: DateGrain,
  pattern: string,
): string {
  const rep = parseBucketDate(key, grain);
  if (!rep) return key;
  const iso = getIsoWeekParts(rep);
  const quarter = Math.floor(rep.getUTCMonth() / 3) + 1;
  return pattern.replace(
    /YYYY|YY|MMMM|MMM|MM|M|DD|D|QQ|Q|WW/g,
    (token: string) => {
      switch (token) {
        case "YYYY":
          return String(rep.getUTCFullYear());
        case "YY":
          return String(rep.getUTCFullYear()).slice(-2);
        case "MMMM":
          return MONTH_LONG[rep.getUTCMonth()]!;
        case "MMM":
          return MONTH_SHORT[rep.getUTCMonth()]!;
        case "MM":
          return pad2(rep.getUTCMonth() + 1);
        case "M":
          return String(rep.getUTCMonth() + 1);
        case "DD":
          return pad2(rep.getUTCDate());
        case "D":
          return String(rep.getUTCDate());
        case "QQ":
          return `Q${quarter}`;
        case "Q":
          return String(quarter);
        case "WW":
          return pad2(iso.week);
        default:
          return token;
      }
    },
  );
}

export function formatTemporalBucketLabel(
  key: string,
  grain: DateGrain,
  pattern?: string,
): string {
  if (!key) return key;
  if (pattern) return formatBucketPattern(key, grain, pattern);
  if (grain === "year") return key;
  if (grain === "quarter") {
    const match = key.match(/^(\d{4})-Q([1-4])$/);
    return match ? `Q${match[2]} ${match[1]}` : key;
  }
  if (grain === "month") {
    const rep = parseBucketDate(key, grain);
    return rep
      ? `${MONTH_SHORT[rep.getUTCMonth()]} ${rep.getUTCFullYear()}`
      : key;
  }
  if (grain === "week") return key;
  return formatDateValue(key);
}

export function shiftTemporalBucketKey(
  key: string,
  grain: DateGrain,
  mode: ShiftMode,
): string | null {
  if (!key) return null;
  if (grain === "year") {
    const year = Number(key);
    if (!Number.isFinite(year)) return null;
    return String(year - 1);
  }
  if (grain === "quarter") {
    const match = key.match(/^(\d{4})-Q([1-4])$/);
    if (!match) return null;
    const year = Number(match[1]);
    const quarter = Number(match[2]);
    if (mode === "previous_year") return `${year - 1}-Q${quarter}`;
    if (quarter > 1) return `${year}-Q${quarter - 1}`;
    return `${year - 1}-Q4`;
  }
  if (grain === "month") {
    const d = parseBucketDate(key, grain);
    if (!d) return null;
    if (mode === "previous_year") d.setUTCFullYear(d.getUTCFullYear() - 1);
    else d.setUTCMonth(d.getUTCMonth() - 1);
    return `${d.getUTCFullYear()}-${pad2(d.getUTCMonth() + 1)}`;
  }
  if (grain === "week") {
    const match = key.match(/^(\d{4})-W(\d{2})$/);
    if (!match) return null;
    const year = Number(match[1]);
    const week = Number(match[2]);
    if (mode === "previous_year") {
      const prevYear = year - 1;
      if (week > isoWeeksInYear(prevYear)) return null;
      return `${prevYear}-W${pad2(week)}`;
    }
    const d = isoWeekStart(year, week);
    d.setUTCDate(d.getUTCDate() - 7);
    const iso = getIsoWeekParts(d);
    return `${iso.year}-W${pad2(iso.week)}`;
  }
  const d = parseBucketDate(key, grain);
  if (!d) return null;
  if (mode === "previous_year") d.setUTCFullYear(d.getUTCFullYear() - 1);
  else d.setUTCDate(d.getUTCDate() - 1);
  return d.toISOString().slice(0, 10);
}

// ---------------------------------------------------------------------------
// Temporal hierarchy helpers
// ---------------------------------------------------------------------------

export function monthToQuarter(month: number): number {
  return Math.floor((month - 1) / 3) + 1;
}

/**
 * Derive parent bucket keys from a leaf bucket key, outermost to innermost
 * (excluding the leaf itself). Returns [] for year grain (no parents).
 */
export function extractParentBuckets(
  leafKey: string,
  leafGrain: DateGrain,
): string[] {
  const levels = getTemporalHierarchyLevels(leafGrain);
  if (levels.length <= 1) return [];

  const parents: string[] = [];
  for (let i = 0; i < levels.length - 1; i++) {
    const parentGrain = levels[i]!;
    const bucket = deriveParentBucket(leafKey, leafGrain, parentGrain);
    if (bucket !== null) parents.push(bucket);
  }
  return parents;
}

function deriveParentBucket(
  leafKey: string,
  leafGrain: DateGrain,
  parentGrain: DateGrain,
): string | null {
  if (parentGrain === "year") {
    const match = leafKey.match(/^(\d{4})/);
    return match ? match[1]! : null;
  }
  if (parentGrain === "quarter") {
    if (leafGrain === "month") {
      const match = leafKey.match(/^(\d{4})-(\d{2})$/);
      if (!match) return null;
      const q = monthToQuarter(Number(match[2]));
      return `${match[1]}-Q${q}`;
    }
    if (leafGrain === "quarter") {
      return leafKey;
    }
    return null;
  }
  if (parentGrain === "month") {
    if (leafGrain === "day") {
      const match = leafKey.match(/^(\d{4}-\d{2})/);
      return match ? match[1]! : null;
    }
    return null;
  }
  return null;
}

/**
 * Human-readable label for a parent header cell.
 */
export function formatTemporalParentLabel(
  parentKey: string,
  parentGrain: DateGrain,
  pattern?: string,
): string {
  return formatTemporalBucketLabel(parentKey, parentGrain, pattern);
}

/**
 * Build a modified column key by replacing the temporal field's segment
 * with a "tp:{fieldName}:{parentBucket}" token.
 */
export function buildModifiedAxisKey(
  fullKey: string[],
  temporalFieldIndex: number,
  fieldName: string,
  parentBucket: string,
): string[] {
  const modified = [...fullKey];
  modified[temporalFieldIndex] = `tp:${fieldName}:${parentBucket}`;
  return modified;
}

export function buildModifiedColKey(
  fullColKey: string[],
  temporalFieldIndex: number,
  fieldName: string,
  parentBucket: string,
): string[] {
  return buildModifiedAxisKey(
    fullColKey,
    temporalFieldIndex,
    fieldName,
    parentBucket,
  );
}

export function buildModifiedRowKey(
  fullRowKey: string[],
  temporalFieldIndex: number,
  fieldName: string,
  parentBucket: string,
): string[] {
  return buildModifiedAxisKey(
    fullRowKey,
    temporalFieldIndex,
    fieldName,
    parentBucket,
  );
}
