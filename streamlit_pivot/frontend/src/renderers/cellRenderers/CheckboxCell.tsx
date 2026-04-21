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

export interface CheckboxCellProps {
  rawValue: unknown;
  /** Fallback text when the raw value isn't a recognizable boolean. */
  displayText: string;
}

/**
 * Renders a dimension cell as an SVG checkbox matching st.dataframe's visual
 * style. Values that don't parse as a boolean fall back to plain text.
 */
export function CheckboxCell({
  rawValue,
  displayText,
}: CheckboxCellProps): ReactNode {
  const parsed = parseBool(rawValue);
  if (parsed === null) {
    return displayText;
  }
  return (
    <span
      className={styles.checkboxWrapper}
      role="img"
      aria-label={parsed ? "checked" : "unchecked"}
      data-testid="pivot-checkbox-cell"
      data-checked={parsed ? "true" : "false"}
    >
      {parsed ? <CheckedIcon /> : <UncheckedIcon />}
    </span>
  );
}

function CheckedIcon() {
  return (
    <svg
      className={styles.checkboxSvg}
      viewBox="0 0 16 16"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      aria-hidden="true"
    >
      <rect
        x="0.75"
        y="0.75"
        width="14.5"
        height="14.5"
        rx="3"
        className={styles.checkboxCheckedRect}
      />
      <path
        d="M4 8L6.5 10.75L12 5.25"
        stroke="white"
        strokeWidth="1.6"
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}

function UncheckedIcon() {
  return (
    <svg
      className={styles.checkboxSvg}
      viewBox="0 0 16 16"
      fill="none"
      xmlns="http://www.w3.org/2000/svg"
      aria-hidden="true"
    >
      <rect
        x="0.75"
        y="0.75"
        width="14.5"
        height="14.5"
        rx="3"
        className={styles.checkboxUncheckedRect}
      />
    </svg>
  );
}

function parseBool(value: unknown): boolean | null {
  if (typeof value === "boolean") return value;
  if (typeof value === "number") {
    if (value === 1) return true;
    if (value === 0) return false;
    return null;
  }
  if (typeof value === "string") {
    const s = value.trim().toLowerCase();
    if (s === "true" || s === "yes" || s === "y" || s === "1" || s === "t") {
      return true;
    }
    if (s === "false" || s === "no" || s === "n" || s === "0" || s === "f") {
      return false;
    }
    return null;
  }
  return null;
}
