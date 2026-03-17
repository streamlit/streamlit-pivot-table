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

import { type RefObject, useCallback, useEffect } from "react";

/**
 * Adds WAI-ARIA listbox keyboard navigation to a dropdown panel.
 *
 * - Arrow Down / Up moves focus between focusable items
 * - Home / End jumps to first / last item
 * - Escape closes the panel and returns focus to the trigger
 * - When opened, focuses the first (or designated initial) item
 *
 * Works inside Shadow DOM by using `getRootNode().activeElement`.
 */
export function useListboxKeyboard(
  panelRef: RefObject<HTMLElement | null>,
  triggerRef: RefObject<HTMLElement | null>,
  isOpen: boolean,
  onClose: () => void,
  itemSelector = '[role="option"], button',
): (e: React.KeyboardEvent) => void {
  const getItems = useCallback((): HTMLElement[] => {
    if (!panelRef.current) return [];
    return Array.from(
      panelRef.current.querySelectorAll<HTMLElement>(itemSelector),
    );
  }, [panelRef, itemSelector]);

  const focusItemAt = useCallback(
    (index: number) => {
      const items = getItems();
      if (items.length === 0) return;
      const clamped = Math.max(0, Math.min(index, items.length - 1));
      items[clamped]?.focus();
    },
    [getItems],
  );

  useEffect(() => {
    if (isOpen) {
      requestAnimationFrame(() => focusItemAt(0));
    }
  }, [isOpen, focusItemAt]);

  const onKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      const items = getItems();
      if (items.length === 0) return;

      const root = panelRef.current?.getRootNode() as Document | ShadowRoot;
      const active = root?.activeElement as HTMLElement | null;
      const idx = active ? items.indexOf(active) : -1;

      switch (e.key) {
        case "ArrowDown":
          e.preventDefault();
          focusItemAt(idx < items.length - 1 ? idx + 1 : 0);
          break;
        case "ArrowUp":
          e.preventDefault();
          focusItemAt(idx > 0 ? idx - 1 : items.length - 1);
          break;
        case "Home":
          e.preventDefault();
          focusItemAt(0);
          break;
        case "End":
          e.preventDefault();
          focusItemAt(items.length - 1);
          break;
        case "Escape":
          e.preventDefault();
          onClose();
          triggerRef.current?.focus();
          break;
        case "Tab":
          e.preventDefault();
          if (e.shiftKey) {
            focusItemAt(idx > 0 ? idx - 1 : items.length - 1);
          } else {
            focusItemAt(idx < items.length - 1 ? idx + 1 : 0);
          }
          break;
      }
    },
    [getItems, panelRef, triggerRef, onClose, focusItemAt],
  );

  return onKeyDown;
}
