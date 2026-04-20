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
 * URL-safety helpers shared by LinkCell (anchor `href`) and ImageCell
 * (`<img src>`). A hostile dataset cell must not be able to produce a
 * script-executing link or data URI, so both renderers go through these
 * scheme-allowlist gates before any value reaches the DOM.
 *
 * Policy:
 *   - Relative URLs (no scheme) and protocol-relative URLs (leading `//`)
 *     are passed through unchanged. They inherit the app's origin/scheme
 *     and cannot introduce a new scheme.
 *   - Any value with a scheme not on the allowlist (javascript:, vbscript:,
 *     file:, ftp:, unknown:, etc.) is rejected — callers fall back to
 *     plain text.
 *   - `data:` URIs are rejected for links entirely. For images, only a
 *     narrow set of raster MIME types are permitted; SVG is excluded
 *     because `<img src="data:image/svg+xml,...">` can execute embedded
 *     script/event handlers in some renderers.
 */

// RFC 3986 scheme production: ALPHA *( ALPHA / DIGIT / "+" / "-" / "." ) ":"
const URL_SCHEME_RE = /^([a-zA-Z][a-zA-Z0-9+.\-]*):/;

const SAFE_LINK_SCHEMES: ReadonlySet<string> = new Set([
  "http",
  "https",
  "mailto",
  "tel",
]);

const SAFE_IMAGE_SCHEMES: ReadonlySet<string> = new Set(["http", "https"]);

// Raster MIME types that are safe to surface via `<img src="data:...">`.
// Kept deliberately narrow: SVG is excluded (script/event-handler risk).
// The enclosing regex anchors the MIME token so `data:image/pngfoo` etc.
// cannot sneak through a loose prefix match.
const SAFE_IMAGE_DATA_MIMES = [
  "png",
  "jpeg",
  "jpg",
  "gif",
  "webp",
  "avif",
  "bmp",
  "x-icon",
  "vnd\\.microsoft\\.icon",
].join("|");
const SAFE_IMAGE_DATA_RE = new RegExp(
  `^data:image/(?:${SAFE_IMAGE_DATA_MIMES})(?:;[^,]*)?,`,
  "i",
);

/**
 * Normalize a raw cell value into a string candidate for URL processing.
 * Returns `null` for null / undefined / empty-after-trim inputs.
 */
function normalizeCandidate(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  const s = String(value).trim();
  return s === "" ? null : s;
}

/**
 * Returns the URL's lowercased scheme (without the trailing `:`), or `null`
 * if the string is schemeless (relative or protocol-relative).
 */
function extractScheme(candidate: string): string | null {
  const match = URL_SCHEME_RE.exec(candidate);
  return match ? match[1].toLowerCase() : null;
}

/**
 * Validate a value for use as an anchor `href`. Returns the trimmed string
 * when safe, or `null` when the value is missing or uses a disallowed
 * scheme.
 */
export function toSafeHref(value: unknown): string | null {
  const candidate = normalizeCandidate(value);
  if (candidate === null) return null;
  const scheme = extractScheme(candidate);
  if (scheme === null) return candidate;
  return SAFE_LINK_SCHEMES.has(scheme) ? candidate : null;
}

/**
 * Validate a value for use as `<img src>`. Returns the trimmed string when
 * safe, or `null` when the value is missing or uses a disallowed scheme /
 * non-image `data:` MIME type / SVG MIME type.
 */
export function toSafeImageSrc(value: unknown): string | null {
  const candidate = normalizeCandidate(value);
  if (candidate === null) return null;
  const scheme = extractScheme(candidate);
  if (scheme === null) return candidate;
  if (SAFE_IMAGE_SCHEMES.has(scheme)) return candidate;
  if (scheme === "data" && SAFE_IMAGE_DATA_RE.test(candidate)) return candidate;
  return null;
}
