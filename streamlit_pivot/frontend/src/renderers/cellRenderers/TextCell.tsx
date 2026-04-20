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

const ELLIPSIS = "\u2026";

export interface TextCellProps {
  displayText: string;
  /** Positive integer; cells longer than this are truncated with an ellipsis. */
  maxChars: number;
}

/**
 * Character-level truncation helper. Truncates to `maxChars` UTF-16 code
 * units (including the trailing ellipsis; this matches JavaScript's native
 * `String.length` / `slice` semantics and mirrors Streamlit's
 * `TextColumn(max_chars=...)` behavior) and exposes the full text via the
 * `title` attribute for hover inspection. Applies on every row including
 * Total / Subtotal rows — truncation is a pure text operation with no
 * semantic coupling to the cell value.
 *
 * NOTE: astral-plane characters (emoji, some supplementary-plane CJK) are
 * two code units. At the truncation boundary a surrogate pair can be split
 * — acceptable for a lightweight preview; callers that need strict
 * grapheme-level truncation should pre-format the value.
 */
export function TextCell({ displayText, maxChars }: TextCellProps): ReactNode {
  if (maxChars <= 0 || displayText.length <= maxChars) {
    return displayText;
  }
  const sliceLen = Math.max(0, maxChars - 1);
  const truncated = displayText.slice(0, sliceLen) + ELLIPSIS;
  return (
    <span title={displayText} data-testid="pivot-text-cell-truncated">
      {truncated}
    </span>
  );
}
