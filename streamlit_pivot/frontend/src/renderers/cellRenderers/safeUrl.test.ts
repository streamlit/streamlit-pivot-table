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

import { toSafeHref, toSafeImageSrc } from "./safeUrl";

describe("toSafeHref", () => {
  it.each([[null], [undefined], [""], ["   "], ["\t\n"]])(
    "returns null for empty / missing input %p",
    (value) => {
      expect(toSafeHref(value)).toBeNull();
    },
  );

  it.each([
    "https://example.com",
    "HTTPS://example.com",
    "http://example.com/path?q=1",
    "mailto:foo@example.com",
    "MAILTO:foo@example.com",
    "tel:+15551234567",
    "/relative/path",
    "relative/path",
    "//cdn.example.com/a",
    "#anchor",
    "?query=only",
  ])("allows safe URL %s", (url) => {
    expect(toSafeHref(url)).toBe(url.trim());
  });

  it.each([
    "javascript:alert(1)",
    " JavaScript:alert(1) ",
    "vbscript:msgbox(1)",
    "data:text/html,<script>alert(1)</script>",
    "data:image/png;base64,iVBORw0KGgo=",
    "file:///etc/passwd",
    "ftp://example.com/x",
    "chrome://settings",
    "about:blank",
  ])("rejects disallowed scheme %s", (url) => {
    expect(toSafeHref(url)).toBeNull();
  });

  it("trims surrounding whitespace before validation", () => {
    expect(toSafeHref("   https://example.com   ")).toBe("https://example.com");
  });

  it("coerces non-string values via String()", () => {
    expect(toSafeHref(12345)).toBe("12345");
    expect(toSafeHref({ toString: () => "https://x" })).toBe("https://x");
  });
});

describe("toSafeImageSrc", () => {
  it.each([[null], [undefined], [""], ["   "]])(
    "returns null for empty / missing input %p",
    (value) => {
      expect(toSafeImageSrc(value)).toBeNull();
    },
  );

  it.each([
    "https://cdn.example.com/a.png",
    "HTTP://cdn.example.com/a.png",
    "/assets/a.png",
    "assets/a.png",
    "//cdn.example.com/a.png",
    "data:image/png;base64,iVBORw0KGgo=",
    "data:image/jpeg;base64,/9j/4AAQ",
    "data:image/jpg;base64,/9j/4AAQ",
    "data:image/gif;base64,R0lGODlh",
    "data:image/webp;base64,UklGRg==",
    "data:image/avif;base64,AAA",
    "data:image/bmp;base64,Qk0",
    "data:image/x-icon;base64,AAAB",
    "data:image/vnd.microsoft.icon;base64,AAAB",
    "DATA:IMAGE/PNG;base64,iVBORw0KGgo=",
    "data:image/png,%89PNG",
  ])("allows safe image source %s", (url) => {
    expect(toSafeImageSrc(url)).toBe(url.trim());
  });

  it.each([
    "javascript:alert(1)",
    "vbscript:msgbox(1)",
    "file:///etc/passwd",
    "ftp://example.com/x.png",
    "mailto:foo@example.com",
    "tel:+15551234567",
    "data:text/html,<script>alert(1)</script>",
    "data:application/octet-stream,AAAA",
    "data:image/svg+xml,<svg onload=alert(1)/>",
    "data:image/svg+xml;base64,PHN2Zz4=",
    // Fuzzy prefix match (e.g. data:image/pngfoo) must be rejected — the
    // MIME token is anchored by the regex, not a loose startsWith.
    "data:image/pngfoo;base64,AAAA",
    "data:image/jpegx,AAAA",
    // Data URI with no comma separator is malformed and rejected.
    "data:image/png",
    "data:image/png;base64",
  ])("rejects disallowed image source %s", (url) => {
    expect(toSafeImageSrc(url)).toBeNull();
  });

  it("trims surrounding whitespace before validation", () => {
    expect(toSafeImageSrc("   https://cdn.example.com/a.png   ")).toBe(
      "https://cdn.example.com/a.png",
    );
  });
});
