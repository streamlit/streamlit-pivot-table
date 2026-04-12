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
 * Locale-aware number formatting utilities.
 *
 * Uses Intl.NumberFormat for proper digit grouping (e.g. 1,234,567.89 in en-US)
 * and decimal handling based on the user's browser locale.
 */

const integerFormatter = new Intl.NumberFormat(undefined, {
  maximumFractionDigits: 0,
});

const decimalFormatter = new Intl.NumberFormat(undefined, {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

export function formatNumber(n: number): string {
  return Number.isInteger(n)
    ? integerFormatter.format(n)
    : decimalFormatter.format(n);
}

// ---------------------------------------------------------------------------
// Pattern-based formatting (Phase 3d)
// ---------------------------------------------------------------------------

const patternCache = new Map<string, Intl.NumberFormat>();

const CURRENCY_SYMBOLS: Record<string, string> = {
  $: "USD",
  "€": "EUR",
  "£": "GBP",
  "¥": "JPY",
};

/**
 * Validate whether a format string matches the supported lightweight pattern syntax.
 */
export function isSupportedFormatPattern(pattern: string): boolean {
  const trimmed = pattern.trim();
  if (!trimmed) return false;

  let cursor = 0;
  let hasCurrencyPrefix = false;

  const firstChar = trimmed[cursor];
  if (firstChar && firstChar in CURRENCY_SYMBOLS) {
    hasCurrencyPrefix = true;
    cursor++;
    const codeMatch = trimmed.slice(cursor).match(/^([A-Z]{3})/);
    if (codeMatch) {
      cursor += 3;
    }
  }

  if (trimmed[cursor] === ",") {
    cursor++;
  }

  const rest = trimmed.slice(cursor);
  const match = rest.match(/^\.(\d+)([f%])$/);
  if (!match) return false;

  // Currency+percent combinations are not supported by this formatter.
  if (hasCurrencyPrefix && match[2] === "%") return false;

  return true;
}

/**
 * Parse a lightweight format pattern string and format a number.
 *
 * Supported patterns:
 *   "$,.0f"     -> currency integer (USD):  $1,235
 *   ",.2f"      -> grouped 2-decimal:       1,234.56
 *   ",.0f"      -> grouped integer:         1,234
 *   ".1%"       -> percent 1-decimal:       45.2%
 *   ".2%"       -> percent 2-decimal:       45.20%
 *   ".0%"       -> percent integer:         45%
 *   "$,.2f"     -> currency 2-decimal:      $1,234.56
 *   "$EUR,.2f"  -> EUR currency 2-decimal:  €1,234.56
 *   "€,.2f"     -> EUR via symbol:          €1,234.56
 *   "£,.0f"     -> GBP via symbol:          £1,235
 *
 * Falls back to formatNumber() for unrecognised patterns.
 */
export function formatWithPattern(value: number, pattern: string): string {
  let cached = patternCache.get(pattern);
  if (cached) return cached.format(value);

  const opts: Intl.NumberFormatOptions = {};
  let cursor = 0;

  // Currency prefix: "$", "€", "£", "¥", or "$XXX" (ISO 4217 code)
  const firstChar = pattern[cursor];
  if (firstChar && firstChar in CURRENCY_SYMBOLS) {
    opts.style = "currency";
    opts.currencyDisplay = "narrowSymbol";
    cursor++;
    const codeMatch = pattern.slice(cursor).match(/^([A-Z]{3})/);
    if (codeMatch) {
      opts.currency = codeMatch[1];
      cursor += 3;
    } else {
      opts.currency = CURRENCY_SYMBOLS[firstChar];
    }
  }

  // "," for grouping
  if (pattern[cursor] === ",") {
    opts.useGrouping = true;
    cursor++;
  } else {
    opts.useGrouping = false;
  }

  // ".Nf" or ".N%" suffix
  const rest = pattern.slice(cursor);
  const match = rest.match(/^\.(\d+)([f%])$/);
  if (match) {
    const decimals = parseInt(match[1], 10);
    if (match[2] === "%") {
      if (opts.style !== "currency") opts.style = "percent";
      opts.minimumFractionDigits = decimals;
      opts.maximumFractionDigits = decimals;
    } else {
      opts.minimumFractionDigits = decimals;
      opts.maximumFractionDigits = decimals;
    }
  }

  try {
    cached = new Intl.NumberFormat(undefined, opts);
    patternCache.set(pattern, cached);
    return cached.format(value);
  } catch {
    return formatNumber(value);
  }
}

const pctFormatter = new Intl.NumberFormat(undefined, {
  style: "percent",
  minimumFractionDigits: 1,
  maximumFractionDigits: 1,
});

/**
 * Format a fractional value (0.452) as a percentage string (45.2%).
 */
export function formatPercent(n: number): string {
  return pctFormatter.format(n);
}

// ---------------------------------------------------------------------------
// Date / datetime / integer display formatters (Phase 1c)
// ---------------------------------------------------------------------------

const ISO_NAIVE_RE = /^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2})?(\.\d+)?)?$/;

/**
 * Normalize a naive ISO datetime string to UTC by appending "Z".
 * JS `new Date("2024-01-15T12:30:00")` parses as LOCAL TIME (browser-dependent),
 * but `new Date("2024-01-15T12:30:00Z")` parses as UTC.
 * Date-only strings like "2024-01-15" are already UTC per the JS spec.
 */
export function normalizeToUTC(s: string): string {
  if (ISO_NAIVE_RE.test(s) && (s.includes("T") || s.includes(" "))) {
    return s.replace(" ", "T") + "Z";
  }
  return s;
}

function toDate(raw: unknown): Date | null {
  if (raw instanceof Date) return raw;
  if (typeof raw === "number") return new Date(raw);
  if (typeof raw === "string") {
    const d = new Date(normalizeToUTC(raw));
    if (!isNaN(d.getTime())) return d;
  }
  return null;
}

const dateFmt = new Intl.DateTimeFormat(undefined, {
  year: "numeric",
  month: "short",
  day: "numeric",
  timeZone: "UTC",
});

const dateTimeFmt = new Intl.DateTimeFormat(undefined, {
  year: "numeric",
  month: "short",
  day: "numeric",
  hour: "2-digit",
  minute: "2-digit",
  timeZone: "UTC",
});

const intLabelFmt = new Intl.NumberFormat(undefined, {
  maximumFractionDigits: 0,
});

export function formatDateValue(raw: unknown): string {
  const d = toDate(raw);
  return d ? dateFmt.format(d) : String(raw);
}

export function formatDateTimeValue(raw: unknown): string {
  const d = toDate(raw);
  return d ? dateTimeFmt.format(d) : String(raw);
}

export function formatIntegerLabel(raw: unknown): string {
  if (typeof raw === "number" && isFinite(raw)) {
    return intLabelFmt.format(raw);
  }
  if (typeof raw === "string") {
    const n = Number(raw);
    if (isFinite(n)) return intLabelFmt.format(n);
  }
  return String(raw);
}

// ---------------------------------------------------------------------------
// Date pattern formatting (Phase 2b)
// ---------------------------------------------------------------------------

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

type TokenResolver = (d: Date) => string;

const TOKEN_RESOLVERS: [string, TokenResolver][] = [
  ["YYYY", (d) => String(d.getUTCFullYear())],
  ["YY", (d) => String(d.getUTCFullYear()).slice(-2)],
  ["MMMM", (d) => MONTH_LONG[d.getUTCMonth()]],
  ["MMM", (d) => MONTH_SHORT[d.getUTCMonth()]],
  ["MM", (d) => String(d.getUTCMonth() + 1).padStart(2, "0")],
  ["M", (d) => String(d.getUTCMonth() + 1)],
  ["DD", (d) => String(d.getUTCDate()).padStart(2, "0")],
  ["D", (d) => String(d.getUTCDate())],
  ["HH", (d) => String(d.getUTCHours()).padStart(2, "0")],
  ["mm", (d) => String(d.getUTCMinutes()).padStart(2, "0")],
  ["ss", (d) => String(d.getUTCSeconds()).padStart(2, "0")],
];

const TOKEN_RE = /YYYY|YY|MMMM|MMM|MM|M|DD|D|HH|mm|ss/g;

const patternResolverCache = new Map<
  string,
  { parts: { literal: string; resolve?: TokenResolver }[] }
>();

function buildPatternParts(
  pattern: string,
): { literal: string; resolve?: TokenResolver }[] {
  const resolverMap = new Map(TOKEN_RESOLVERS);
  const parts: { literal: string; resolve?: TokenResolver }[] = [];
  let lastIndex = 0;
  TOKEN_RE.lastIndex = 0;
  let m: RegExpExecArray | null;
  while ((m = TOKEN_RE.exec(pattern)) !== null) {
    if (m.index > lastIndex) {
      parts.push({ literal: pattern.slice(lastIndex, m.index) });
    }
    parts.push({ literal: "", resolve: resolverMap.get(m[0]) });
    lastIndex = TOKEN_RE.lastIndex;
  }
  if (lastIndex < pattern.length) {
    parts.push({ literal: pattern.slice(lastIndex) });
  }
  return parts;
}

export function formatDateWithPattern(raw: unknown, pattern: string): string {
  const d = toDate(raw);
  if (!d) return String(raw);

  let cached = patternResolverCache.get(pattern);
  if (!cached) {
    cached = { parts: buildPatternParts(pattern) };
    patternResolverCache.set(pattern, cached);
  }

  let result = "";
  for (const part of cached.parts) {
    result += part.literal;
    if (part.resolve) result += part.resolve(d);
  }
  return result;
}
