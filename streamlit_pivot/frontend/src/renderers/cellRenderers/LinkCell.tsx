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

import type { ReactNode } from "react";

import styles from "./CellRenderers.module.css";
import { toSafeHref } from "./safeUrl";

export interface LinkCellProps {
  /** Raw cell value; used as the anchor href after String() coercion. */
  rawValue: unknown;
  /** Pre-formatted display string; used as default anchor text. */
  displayText: string;
  /**
   * Optional template. Accepts a plain string (used verbatim) or a template
   * containing `{}` which is substituted with the cell value at render time.
   * Mirrors Streamlit's `LinkColumn(display_text=...)` convention.
   */
  displayTemplate?: string;
}

/**
 * Renders a dimension cell as an anchor. When the cell value doesn't look
 * like a URL (empty / null / whitespace, or an unsafe scheme — see
 * `safeUrl.ts` for the scheme allowlist), falls back to plain text so
 * totals, missing values, and hostile data can't emit broken or dangerous
 * `<a href="…">…</a>` markup.
 */
export function LinkCell({
  rawValue,
  displayText,
  displayTemplate,
}: LinkCellProps): ReactNode {
  const href = toSafeHref(rawValue);
  if (href === null) {
    return displayText;
  }
  const anchorText = resolveAnchorText(displayTemplate, displayText, rawValue);
  return (
    <a
      className={styles.link}
      href={href}
      target="_blank"
      rel="noopener noreferrer"
      data-testid="pivot-link-cell"
      onClick={(e) => e.stopPropagation()}
    >
      {anchorText}
    </a>
  );
}

function resolveAnchorText(
  template: string | undefined,
  fallback: string,
  rawValue: unknown,
): string {
  if (!template) return fallback;
  if (template.includes("{}")) {
    const subst =
      rawValue === null || rawValue === undefined ? "" : String(rawValue);
    return template.split("{}").join(subst);
  }
  return template;
}
