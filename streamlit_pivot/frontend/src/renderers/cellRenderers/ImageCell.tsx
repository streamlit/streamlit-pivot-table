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
import { toSafeImageSrc } from "./safeUrl";

export interface ImageCellProps {
  rawValue: unknown;
  displayText: string;
  /**
   * "cell" (default, regular table cell) or "breadcrumb" (hierarchy mode).
   * The breadcrumb variant caps image height at 1em so it doesn't blow out
   * the 12px/1.2 line-height breadcrumb bar.
   */
  variant?: "cell" | "breadcrumb";
}

/**
 * Renders a dimension cell as an image. Falls back to plain text when the
 * raw value isn't a usable src (empty / null, or an unsafe scheme — see
 * `safeUrl.ts` for the scheme / data-MIME allowlist), keeping totals rows
 * and hostile-data rows legible without producing broken or dangerous
 * markup.
 */
export function ImageCell({
  rawValue,
  displayText,
  variant = "cell",
}: ImageCellProps): ReactNode {
  const src = toSafeImageSrc(rawValue);
  if (src === null) {
    return displayText;
  }
  const className =
    variant === "breadcrumb" ? styles.imageBreadcrumb : styles.image;
  return (
    <img
      className={className}
      src={src}
      alt={displayText}
      data-testid="pivot-image-cell"
      loading="lazy"
    />
  );
}
