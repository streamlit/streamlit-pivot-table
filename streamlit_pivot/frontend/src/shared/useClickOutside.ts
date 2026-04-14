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

import { RefObject, useEffect } from "react";

/**
 * Calls `onClose` when a mousedown event occurs outside the referenced element.
 * Uses `composedPath()` so the check works correctly across Shadow DOM
 * boundaries — unlike `event.target`, which gets retargeted to the shadow host
 * when the listener is on `document`.
 *
 * An optional `excludeRef` can be provided so that clicks on that element
 * (or its descendants) are also treated as "inside" and do not trigger close.
 */
export function useClickOutside(
  ref: RefObject<HTMLElement | null>,
  onClose: () => void,
  active = true,
  excludeRef?: RefObject<HTMLElement | null>,
): void {
  useEffect(() => {
    if (!active) return;
    const handler = (e: Event) => {
      const path = e.composedPath();
      if (ref.current && !path.includes(ref.current)) {
        if (excludeRef?.current && path.includes(excludeRef.current)) return;
        onClose();
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [ref, onClose, active, excludeRef]);
}
