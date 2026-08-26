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
 * Shared collapse-state helpers for the row and column group axes.
 *
 * Collapsed state is a flat list of group-key strings, plus an "__ALL__"
 * sentinel written by Collapse All and by `collapse_row_groups`. Two rules keep
 * the controls honest:
 *
 *  1. The sentinel is expanded before any mutation. Left in place it would keep
 *     collapsing whatever a click was meant to reveal, so the control does
 *     nothing when pressed.
 *  2. Collapsing a group records every group nested beneath it. Those groups are
 *     hidden either way, but recording them is what makes a later expand reveal
 *     the immediate children rather than springing the whole subtree open.
 */

import { makeKeyString } from "../engine/PivotData";

/** Number of levels on an axis that can hold groups (all but the innermost). */
export function countCollapsibleLevels(keys: string[][]): number {
  return Math.max((keys[0]?.length ?? 1) - 1, 0);
}

/**
 * Expand the "__ALL__" sentinel into explicit group keys, one set per
 * collapsible level. Must be called before any mutation to collapsed state.
 *
 * Every level is recorded, not just the top one. Both spellings render the same
 * table — nothing beneath a collapsed parent is visible either way — but
 * recording only the top level means expanding it reveals the entire subtree.
 */
export function normalizeCollapsed(
  collapsed: string[],
  keys: string[][],
  collapsibleLevels: number,
): Set<string> {
  const result = new Set(collapsed);
  if (result.has("__ALL__")) {
    result.delete("__ALL__");
    for (let level = 0; level < collapsibleLevels; level++) {
      for (const key of keys) {
        result.add(makeKeyString(key.slice(0, level + 1)));
      }
    }
  }
  return result;
}

/**
 * Toggle a single group node, returning the collapsed list to store.
 *
 * Collapsing also records the groups nested under this node, so that expanding
 * it again reveals one level rather than everything it was hiding.
 */
export function toggleCollapsedGroupNode(
  collapsed: string[],
  groupKeyStr: string,
  keys: string[][],
): string[] {
  const collapsibleLevels = countCollapsibleLevels(keys);
  const working = normalizeCollapsed(collapsed, keys, collapsibleLevels);

  if (working.has(groupKeyStr)) {
    working.delete(groupKeyStr);
    return [...working].sort();
  }

  working.add(groupKeyStr);
  for (const key of keys) {
    for (let level = 0; level < collapsibleLevels; level++) {
      if (makeKeyString(key.slice(0, level + 1)) !== groupKeyStr) continue;
      for (let deeper = level + 1; deeper < collapsibleLevels; deeper++) {
        working.add(makeKeyString(key.slice(0, deeper + 1)));
      }
    }
  }
  return [...working].sort();
}
