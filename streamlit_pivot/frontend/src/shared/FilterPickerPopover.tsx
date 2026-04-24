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

import {
  type FC,
  type ReactElement,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { createPortal } from "react-dom";
import type { DimensionFilter } from "../engine/types";
import { useClickOutside } from "./useClickOutside";
import menuStyles from "../renderers/HeaderMenu.module.css";

// ---------------------------------------------------------------------------
// FilterPickerPopover
// ---------------------------------------------------------------------------

export interface FilterPickerPopoverProps {
  field: string;
  filter: DimensionFilter | undefined;
  /** Resolved unique values: precomputed sidecar (hybrid) or from pivotData (client-only). */
  uniqueValues: string[];
  formatLabel?: (key: string) => string;
  menuLimit?: number;
  onFilterChange: (filter: DimensionFilter | undefined) => void;
  onClose: () => void;
  anchorRect: DOMRect;
  /** Element to portal into — must be inside the component tree so CSS vars are inherited. */
  portalTarget?: Element | null;
}

const DEFAULT_MENU_LIMIT = 50;

export const FilterPickerPopover: FC<FilterPickerPopoverProps> = ({
  field,
  filter,
  uniqueValues,
  formatLabel,
  menuLimit = DEFAULT_MENU_LIMIT,
  onFilterChange,
  onClose,
  anchorRect,
  portalTarget,
}): ReactElement => {
  const containerRef = useRef<HTMLDivElement>(null);
  const [search, setSearch] = useState("");

  useClickOutside(containerRef, onClose, true);

  // Focus search input on mount
  useEffect(() => {
    requestAnimationFrame(() => {
      containerRef.current
        ?.querySelector<HTMLInputElement>("input[type='text']")
        ?.focus();
    });
  }, []);

  const includedSet = filter?.include ? new Set(filter.include) : null;
  const excludedSet = filter?.exclude ? new Set(filter.exclude) : null;

  const isChecked = useCallback(
    (val: string): boolean => {
      if (includedSet) return includedSet.has(val);
      if (excludedSet) return !excludedSet.has(val);
      return true;
    },
    [includedSet, excludedSet],
  );

  const filteredValues = useMemo(() => {
    if (!search) return uniqueValues;
    const lower = search.toLowerCase();
    return uniqueValues.filter((v) => {
      if (v.toLowerCase().includes(lower)) return true;
      const label = formatLabel?.(v);
      return label ? label.toLowerCase().includes(lower) : false;
    });
  }, [uniqueValues, search, formatLabel]);

  const visibleValues = filteredValues.slice(0, menuLimit);
  const overflowCount = filteredValues.length - visibleValues.length;

  const toggleValue = useCallback(
    (val: string) => {
      const checked = isChecked(val);
      if (checked) {
        if (includedSet) {
          const newInclude = [...includedSet].filter((v) => v !== val);
          onFilterChange(
            newInclude.length > 0 ? { include: newInclude } : undefined,
          );
        } else {
          const newExclude = [...(excludedSet ?? []), val];
          onFilterChange({ exclude: newExclude });
        }
      } else {
        if (excludedSet) {
          const newExclude = [...excludedSet].filter((v) => v !== val);
          onFilterChange(
            newExclude.length > 0 ? { exclude: newExclude } : undefined,
          );
        } else if (includedSet) {
          const newInclude = [...includedSet, val];
          onFilterChange(
            newInclude.length === uniqueValues.length
              ? undefined
              : { include: newInclude },
          );
        }
      }
    },
    [isChecked, includedSet, excludedSet, uniqueValues, onFilterChange],
  );

  const selectAll = useCallback(
    () => onFilterChange(undefined),
    [onFilterChange],
  );
  const clearAll = useCallback(
    () => onFilterChange({ exclude: [...uniqueValues] }),
    [uniqueValues, onFilterChange],
  );

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === "Escape") {
        e.stopPropagation();
        onClose();
      }
    },
    [onClose],
  );

  // Position popover below the anchor chip, keeping it within viewport
  const style = useMemo<React.CSSProperties>(() => {
    const top = anchorRect.bottom + 4;
    const left = Math.min(anchorRect.left, window.innerWidth - 220);
    return {
      position: "fixed",
      top,
      left: Math.max(4, left),
      zIndex: 10000,
    };
  }, [anchorRect]);

  return createPortal(
    <div
      ref={containerRef}
      className={menuStyles.headerMenu}
      style={style}
      role="dialog"
      aria-label={`Filter ${field}`}
      onKeyDown={handleKeyDown}
      data-testid={`filter-picker-${field}`}
    >
      <div className={menuStyles.menuTitle}>
        {formatLabel?.(field) ?? field}
      </div>
      <div className={menuStyles.filterSection}>
        <input
          type="text"
          className={menuStyles.searchInput}
          placeholder="Search values…"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          data-testid={`filter-picker-search-${field}`}
        />
        <div className={menuStyles.actionRow}>
          <button
            type="button"
            className={menuStyles.actionButton}
            onClick={selectAll}
          >
            Select All
          </button>
          <button
            type="button"
            className={menuStyles.actionButton}
            onClick={clearAll}
          >
            Clear All
          </button>
        </div>
        <div
          className={menuStyles.itemList}
          role="listbox"
          aria-multiselectable="true"
        >
          {visibleValues.map((val) => (
            <label
              key={val}
              className={menuStyles.filterItem}
              role="option"
              aria-selected={isChecked(val)}
              data-testid={`filter-picker-item-${val}`}
            >
              <input
                type="checkbox"
                checked={isChecked(val)}
                onChange={() => toggleValue(val)}
                tabIndex={-1}
              />
              <span>{(formatLabel?.(val) ?? val) || "(empty)"}</span>
            </label>
          ))}
          {overflowCount > 0 && (
            <div className={menuStyles.moreIndicator}>
              and {overflowCount} more…
            </div>
          )}
          {visibleValues.length === 0 && (
            <div className={menuStyles.moreIndicator}>No matching values</div>
          )}
        </div>
      </div>
    </div>,
    portalTarget ?? document.body,
  );
};
