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
import { render } from "@testing-library/react";

import type { PivotConfigV1 } from "../engine/types";
import { makeConfig } from "../test-utils";

import { renderCellContent, hasCellRenderer } from "./cellRenderer";

function wrap(node: React.ReactNode): HTMLElement {
  const { container } = render(<>{node}</>);
  return container;
}

function cfgWith(
  overrides: Partial<PivotConfigV1["field_renderers"]> = {},
  base: Partial<PivotConfigV1> = {},
): PivotConfigV1 {
  return makeConfig({
    ...base,
    field_renderers: overrides as PivotConfigV1["field_renderers"],
  });
}

describe("renderCellContent - dispatcher", () => {
  it("returns displayText string when no renderer is configured", () => {
    const config = makeConfig();
    const result = renderCellContent({
      rawValue: "https://x",
      displayText: "x",
      field: "website",
      config,
      isTotal: false,
    });
    expect(result).toBe("x");
  });

  it("returns displayText string when a different field has a renderer", () => {
    const config = cfgWith({ other: { type: "link" } });
    const result = renderCellContent({
      rawValue: "hello",
      displayText: "hello",
      field: "website",
      config,
      isTotal: false,
    });
    expect(result).toBe("hello");
  });

  it("hasCellRenderer returns true only when field has a renderer", () => {
    const config = cfgWith({ a: { type: "link" }, b: { type: "image" } });
    expect(hasCellRenderer(config, "a")).toBe(true);
    expect(hasCellRenderer(config, "b")).toBe(true);
    expect(hasCellRenderer(config, "c")).toBe(false);
    expect(hasCellRenderer(makeConfig(), "a")).toBe(false);
  });
});

describe("renderCellContent - LinkColumn", () => {
  it("renders an anchor with href from raw value and default anchor text", () => {
    const config = cfgWith({ website: { type: "link" } });
    const container = wrap(
      renderCellContent({
        rawValue: "https://example.com",
        displayText: "example.com",
        field: "website",
        config,
        isTotal: false,
      }),
    );
    const a = container.querySelector(
      "[data-testid='pivot-link-cell']",
    ) as HTMLAnchorElement;
    expect(a).not.toBeNull();
    expect(a.getAttribute("href")).toBe("https://example.com");
    expect(a.textContent).toBe("example.com");
    expect(a.getAttribute("target")).toBe("_blank");
    expect(a.getAttribute("rel")).toBe("noopener noreferrer");
  });

  it("substitutes {} in display_text template with the raw value", () => {
    const config = cfgWith({
      website: { type: "link", display_text: "Visit {}" },
    });
    const container = wrap(
      renderCellContent({
        rawValue: "example.com",
        displayText: "example.com",
        field: "website",
        config,
        isTotal: false,
      }),
    );
    const a = container.querySelector(
      "[data-testid='pivot-link-cell']",
    ) as HTMLAnchorElement;
    expect(a.textContent).toBe("Visit example.com");
  });

  it("uses display_text verbatim when no {} placeholder is present", () => {
    const config = cfgWith({
      website: { type: "link", display_text: "Go" },
    });
    const container = wrap(
      renderCellContent({
        rawValue: "https://example.com",
        displayText: "example.com",
        field: "website",
        config,
        isTotal: false,
      }),
    );
    const a = container.querySelector(
      "[data-testid='pivot-link-cell']",
    ) as HTMLAnchorElement;
    expect(a.textContent).toBe("Go");
  });

  it("falls back to plain text when raw value is empty / null", () => {
    const config = cfgWith({ website: { type: "link" } });
    for (const raw of [null, undefined, "", "   "]) {
      const container = wrap(
        renderCellContent({
          rawValue: raw,
          displayText: "(empty)",
          field: "website",
          config,
          isTotal: false,
        }),
      );
      expect(
        container.querySelector("[data-testid='pivot-link-cell']"),
      ).toBeNull();
      expect(container.textContent).toBe("(empty)");
    }
  });

  it("falls back to plain text on Total/Subtotal rows", () => {
    const config = cfgWith({ website: { type: "link" } });
    const container = wrap(
      renderCellContent({
        rawValue: "https://example.com",
        displayText: "example.com Total",
        field: "website",
        config,
        isTotal: true,
      }),
    );
    expect(
      container.querySelector("[data-testid='pivot-link-cell']"),
    ).toBeNull();
    expect(container.textContent).toBe("example.com Total");
  });

  it.each([
    ["javascript:alert(1)"],
    ["JavaScript:alert(1)"],
    ["  javascript:alert(1)  "],
    ["data:text/html,<script>alert(1)</script>"],
    ["vbscript:msgbox(1)"],
    ["file:///etc/passwd"],
    ["ftp://example.com/x"],
  ])("falls back to plain text for disallowed scheme %s", (hostile: string) => {
    const config = cfgWith({ website: { type: "link" } });
    const container = wrap(
      renderCellContent({
        rawValue: hostile,
        displayText: hostile,
        field: "website",
        config,
        isTotal: false,
      }),
    );
    expect(
      container.querySelector("[data-testid='pivot-link-cell']"),
    ).toBeNull();
    expect(container.textContent).toBe(hostile);
  });

  it.each([
    ["https://example.com"],
    ["HTTP://example.com"],
    ["mailto:support@example.com"],
    ["tel:+15551234567"],
    ["/relative/path?x=1"],
    ["relative/path"],
    ["//cdn.example.com/asset.png"],
  ])("allows safe / relative URL %s", (safe: string) => {
    const config = cfgWith({ website: { type: "link" } });
    const container = wrap(
      renderCellContent({
        rawValue: safe,
        displayText: safe,
        field: "website",
        config,
        isTotal: false,
      }),
    );
    const a = container.querySelector(
      "[data-testid='pivot-link-cell']",
    ) as HTMLAnchorElement | null;
    expect(a).not.toBeNull();
    expect(a!.getAttribute("href")).toBe(safe);
  });
});

describe("renderCellContent - ImageColumn", () => {
  it("renders an img with src from raw value and displayText as alt", () => {
    const config = cfgWith({ logo: { type: "image" } });
    const container = wrap(
      renderCellContent({
        rawValue: "/assets/a.png",
        displayText: "a.png",
        field: "logo",
        config,
        isTotal: false,
      }),
    );
    const img = container.querySelector(
      "[data-testid='pivot-image-cell']",
    ) as HTMLImageElement;
    expect(img).not.toBeNull();
    expect(img.getAttribute("src")).toBe("/assets/a.png");
    expect(img.getAttribute("alt")).toBe("a.png");
    expect(img.getAttribute("loading")).toBe("lazy");
  });

  it("applies breadcrumb variant class when variant=breadcrumb", () => {
    const config = cfgWith({ logo: { type: "image" } });
    const containerA = wrap(
      renderCellContent({
        rawValue: "/a.png",
        displayText: "a",
        field: "logo",
        config,
        isTotal: false,
        variant: "cell",
      }),
    );
    const containerB = wrap(
      renderCellContent({
        rawValue: "/a.png",
        displayText: "a",
        field: "logo",
        config,
        isTotal: false,
        variant: "breadcrumb",
      }),
    );
    const imgA = containerA.querySelector(
      "[data-testid='pivot-image-cell']",
    ) as HTMLImageElement;
    const imgB = containerB.querySelector(
      "[data-testid='pivot-image-cell']",
    ) as HTMLImageElement;
    expect(imgA.className).not.toBe(imgB.className);
  });

  it("falls back to plain text on empty raw value", () => {
    const config = cfgWith({ logo: { type: "image" } });
    const container = wrap(
      renderCellContent({
        rawValue: "",
        displayText: "(empty)",
        field: "logo",
        config,
        isTotal: false,
      }),
    );
    expect(
      container.querySelector("[data-testid='pivot-image-cell']"),
    ).toBeNull();
    expect(container.textContent).toBe("(empty)");
  });

  it("falls back to plain text on Total rows", () => {
    const config = cfgWith({ logo: { type: "image" } });
    const container = wrap(
      renderCellContent({
        rawValue: "/a.png",
        displayText: "a",
        field: "logo",
        config,
        isTotal: true,
      }),
    );
    expect(
      container.querySelector("[data-testid='pivot-image-cell']"),
    ).toBeNull();
    expect(container.textContent).toBe("a");
  });

  it.each([
    ["javascript:alert(1)"],
    ["JavaScript:alert(1)"],
    ["vbscript:msgbox(1)"],
    ["file:///etc/passwd"],
    ["ftp://example.com/x.png"],
    ["data:text/html,<script>alert(1)</script>"],
    ["data:image/svg+xml,<svg onload=alert(1)/>"],
    ["data:application/octet-stream,AAAA"],
  ])(
    "falls back to plain text for disallowed image scheme %s",
    (hostile: string) => {
      const config = cfgWith({ logo: { type: "image" } });
      const container = wrap(
        renderCellContent({
          rawValue: hostile,
          displayText: "(bad)",
          field: "logo",
          config,
          isTotal: false,
        }),
      );
      expect(
        container.querySelector("[data-testid='pivot-image-cell']"),
      ).toBeNull();
      expect(container.textContent).toBe("(bad)");
    },
  );

  it.each([
    ["https://cdn.example.com/a.png"],
    ["HTTP://cdn.example.com/a.png"],
    ["/assets/a.png"],
    ["assets/a.png"],
    ["//cdn.example.com/a.png"],
    ["data:image/png;base64,iVBORw0KGgo="],
    ["data:image/jpeg;base64,/9j/4AAQ"],
    ["data:image/gif;base64,R0lGODlh"],
    ["data:image/webp;base64,UklGRg=="],
  ])("allows safe image URL %s", (safe: string) => {
    const config = cfgWith({ logo: { type: "image" } });
    const container = wrap(
      renderCellContent({
        rawValue: safe,
        displayText: "alt",
        field: "logo",
        config,
        isTotal: false,
      }),
    );
    const img = container.querySelector(
      "[data-testid='pivot-image-cell']",
    ) as HTMLImageElement | null;
    expect(img).not.toBeNull();
    expect(img!.getAttribute("src")).toBe(safe);
  });
});

describe("renderCellContent - CheckboxColumn", () => {
  const CHECKED = "\u2611";
  const UNCHECKED = "\u2610";

  it("renders ☑ for true-like values", () => {
    const config = cfgWith({ active: { type: "checkbox" } });
    for (const raw of [true, 1, "true", "TRUE", "yes", "y", "1", "t"]) {
      const container = wrap(
        renderCellContent({
          rawValue: raw,
          displayText: String(raw),
          field: "active",
          config,
          isTotal: false,
        }),
      );
      const node = container.querySelector(
        "[data-testid='pivot-checkbox-cell']",
      );
      expect(node).not.toBeNull();
      expect(node?.getAttribute("data-checked")).toBe("true");
      expect(node?.textContent).toBe(CHECKED);
    }
  });

  it("renders ☐ for false-like values", () => {
    const config = cfgWith({ active: { type: "checkbox" } });
    for (const raw of [false, 0, "false", "FALSE", "no", "n", "0", "f"]) {
      const container = wrap(
        renderCellContent({
          rawValue: raw,
          displayText: String(raw),
          field: "active",
          config,
          isTotal: false,
        }),
      );
      const node = container.querySelector(
        "[data-testid='pivot-checkbox-cell']",
      );
      expect(node).not.toBeNull();
      expect(node?.getAttribute("data-checked")).toBe("false");
      expect(node?.textContent).toBe(UNCHECKED);
    }
  });

  it("falls back to plain text for non-boolean values", () => {
    const config = cfgWith({ active: { type: "checkbox" } });
    for (const raw of ["maybe", "", null, 3.14]) {
      const container = wrap(
        renderCellContent({
          rawValue: raw,
          displayText: "(empty)",
          field: "active",
          config,
          isTotal: false,
        }),
      );
      expect(
        container.querySelector("[data-testid='pivot-checkbox-cell']"),
      ).toBeNull();
    }
  });

  it("falls back to plain text on Total rows", () => {
    const config = cfgWith({ active: { type: "checkbox" } });
    const container = wrap(
      renderCellContent({
        rawValue: true,
        displayText: "Subtotal",
        field: "active",
        config,
        isTotal: true,
      }),
    );
    expect(
      container.querySelector("[data-testid='pivot-checkbox-cell']"),
    ).toBeNull();
    expect(container.textContent).toBe("Subtotal");
  });
});

describe("renderCellContent - TextColumn.max_chars", () => {
  it("renders raw text without truncation wrapper when length <= max_chars", () => {
    const config = cfgWith({ notes: { type: "text", max_chars: 10 } });
    const container = wrap(
      renderCellContent({
        rawValue: "abc",
        displayText: "abc",
        field: "notes",
        config,
        isTotal: false,
      }),
    );
    expect(
      container.querySelector("[data-testid='pivot-text-cell-truncated']"),
    ).toBeNull();
    expect(container.textContent).toBe("abc");
  });

  it("truncates with ellipsis when length > max_chars", () => {
    const config = cfgWith({ notes: { type: "text", max_chars: 5 } });
    const container = wrap(
      renderCellContent({
        rawValue: "abcdefghij",
        displayText: "abcdefghij",
        field: "notes",
        config,
        isTotal: false,
      }),
    );
    const span = container.querySelector(
      "[data-testid='pivot-text-cell-truncated']",
    );
    expect(span).not.toBeNull();
    expect(span?.textContent).toBe("abcd\u2026");
    expect(span?.getAttribute("title")).toBe("abcdefghij");
  });

  it("truncation applies even on Total/Subtotal rows", () => {
    const config = cfgWith({ notes: { type: "text", max_chars: 4 } });
    const container = wrap(
      renderCellContent({
        rawValue: "",
        displayText: "North America Total",
        field: "notes",
        config,
        isTotal: true,
      }),
    );
    const span = container.querySelector(
      "[data-testid='pivot-text-cell-truncated']",
    );
    expect(span?.textContent).toBe("Nor\u2026");
    expect(span?.getAttribute("title")).toBe("North America Total");
  });
});
