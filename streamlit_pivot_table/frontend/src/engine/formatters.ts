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
