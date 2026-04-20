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
 * Shared dispatcher for Tier 2 column_config cell renderers. See
 * `renderers/cellRenderers/` for the per-renderer implementations.
 *
 * The dispatcher resolves `config.field_renderers[field]` and delegates to
 * the appropriate renderer component. On Total / Subtotal sentinel rows the
 * Link / Image / Checkbox renderers degrade to plain text because the cell
 * value is a label rather than data; TextColumn.max_chars still truncates
 * since that's a pure text operation.
 */

import type { ReactNode } from "react";

import type { PivotConfigV1 } from "../engine/types";

import { CheckboxCell } from "./cellRenderers/CheckboxCell";
import { ImageCell } from "./cellRenderers/ImageCell";
import { LinkCell } from "./cellRenderers/LinkCell";
import { TextCell } from "./cellRenderers/TextCell";

export interface CellRenderOptions {
  /** The raw cell value (as stored in the row data) — used for href / src / boolean. */
  rawValue: unknown;
  /** The pre-formatted display string from pivotData.formatDimLabel / similar. */
  displayText: string;
  /** The field id (canonical, never a label). */
  field: string;
  /** Pivot config (reads field_renderers). */
  config: PivotConfigV1;
  /**
   * True when this cell is a Total / Subtotal sentinel row. Link / Image /
   * Checkbox fall back to `displayText`; Text truncation still applies.
   */
  isTotal: boolean;
  /**
   * Layout context; affects only ImageCell (tighter max-height in breadcrumb
   * variant to fit the hierarchy breadcrumb bar).
   */
  variant?: "cell" | "breadcrumb";
}

/**
 * Returns the ReactNode to render inside a dimension cell. When no renderer
 * is configured for the field (or the spec is unrecognized), returns the
 * `displayText` string — equivalent to the default rendering path.
 */
export function renderCellContent(opts: CellRenderOptions): ReactNode {
  const spec = opts.config.field_renderers?.[opts.field];
  if (!spec) return opts.displayText;

  if (opts.isTotal) {
    if (spec.type === "text") {
      return (
        <TextCell displayText={opts.displayText} maxChars={spec.max_chars} />
      );
    }
    return opts.displayText;
  }

  switch (spec.type) {
    case "link":
      return (
        <LinkCell
          rawValue={opts.rawValue}
          displayText={opts.displayText}
          displayTemplate={spec.display_text}
        />
      );
    case "image":
      return (
        <ImageCell
          rawValue={opts.rawValue}
          displayText={opts.displayText}
          variant={opts.variant ?? "cell"}
        />
      );
    case "checkbox":
      return (
        <CheckboxCell rawValue={opts.rawValue} displayText={opts.displayText} />
      );
    case "text":
      return (
        <TextCell displayText={opts.displayText} maxChars={spec.max_chars} />
      );
  }
}

/**
 * Returns true when the field has any Tier 2 renderer configured. Exported
 * as a micro-optimization for render paths that want to skip the dispatcher
 * call entirely on common no-op cells.
 */
export function hasCellRenderer(config: PivotConfigV1, field: string): boolean {
  return !!config.field_renderers?.[field];
}
