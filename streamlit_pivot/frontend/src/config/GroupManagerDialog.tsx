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
  type KeyboardEvent,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { createPortal } from "react-dom";
import type { MemberGroup } from "../engine/types";
import styles from "./GroupManagerDialog.module.css";

export interface GroupManagerDialogProps {
  /** The field name (dimension key) being managed. */
  field: string;
  /** Display label for the field (e.g. "Region", "Year"). */
  fieldLabel: string;
  /** All active groups for this field only. */
  memberGroups: MemberGroup[];
  /** All unique values available to be grouped. */
  uniqueValues: string[];
  /** Optional formatter for canonical dim keys → display labels. */
  formatLabel?: (key: string) => string;
  /**
   * Fired whenever groups change (add or remove). Receives the updated list of
   * groups for this field only. The parent is responsible for merging with groups
   * from other fields and committing to config.
   */
  onGroupsChange: (groups: MemberGroup[]) => void;
  /** Close the dialog without further changes. */
  onClose: () => void;
  /** Element to portal into (for CSS var inheritance). Defaults to document.body. */
  portalTarget?: Element | null;
}

/**
 * A fixed, centered modal dialog for creating and removing member groups for a
 * single dimension field. The dialog commits changes immediately (add / remove)
 * so the user can see the effect in the table while the dialog stays open.
 *
 * Placement: rendered by PivotRoot via a React portal so it floats above the
 * table without being clipped by overflow containers.
 */
const GroupManagerDialog: FC<GroupManagerDialogProps> = ({
  field,
  fieldLabel,
  memberGroups,
  uniqueValues,
  formatLabel,
  onGroupsChange,
  onClose,
  portalTarget,
}): ReactElement => {
  const dialogRef = useRef<HTMLDivElement>(null);

  // --- Local form state ---
  const [groupName, setGroupName] = useState("");
  const [selectedMembers, setSelectedMembers] = useState<Set<string>>(
    new Set(),
  );

  // Names of already-grouped values (excluded from the "create" checklist).
  const existingGroupNames = useMemo(
    () => new Set(memberGroups.map((g) => g.name)),
    [memberGroups],
  );

  // Values that can still be selected for a new group (ungrouped only).
  const groupedValues = useMemo(() => {
    const s = new Set<string>();
    for (const g of memberGroups) {
      for (const m of g.members) s.add(m);
    }
    return s;
  }, [memberGroups]);

  const ungroupedValues = useMemo(
    () => uniqueValues.filter((v) => !groupedValues.has(v)),
    [uniqueValues, groupedValues],
  );

  const toggleMember = useCallback((val: string) => {
    setSelectedMembers((prev) => {
      const next = new Set(prev);
      if (next.has(val)) next.delete(val);
      else next.add(val);
      return next;
    });
  }, []);

  const canAdd = groupName.trim() !== "" && selectedMembers.size >= 2;

  const handleAddGroup = useCallback(() => {
    const name = groupName.trim();
    if (!name || selectedMembers.size < 2) return;
    const members = [...selectedMembers];
    // Merge with existing group of the same name, or append a new entry.
    const idx = memberGroups.findIndex((g) => g.name === name);
    let updated: MemberGroup[];
    if (idx >= 0) {
      updated = memberGroups.map((g, i) =>
        i === idx
          ? { ...g, members: [...new Set([...g.members, ...members])] }
          : g,
      );
    } else {
      updated = [...memberGroups, { field, name, members }];
    }
    onGroupsChange(updated);
    // Keep dialog open — clear the form so the user can add another group.
    setGroupName("");
    setSelectedMembers(new Set());
  }, [groupName, selectedMembers, memberGroups, field, onGroupsChange]);

  const handleRemoveGroup = useCallback(
    (name: string) => {
      onGroupsChange(memberGroups.filter((g) => g.name !== name));
    },
    [memberGroups, onGroupsChange],
  );

  // Close on Escape key.
  useEffect(() => {
    const onKeyDown = (e: globalThis.KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    document.addEventListener("keydown", onKeyDown);
    return () => document.removeEventListener("keydown", onKeyDown);
  }, [onClose]);

  // Trap focus inside the dialog.
  useEffect(() => {
    const el = dialogRef.current;
    if (!el) return;
    const focusable = el.querySelectorAll<HTMLElement>(
      'button, input, [tabindex]:not([tabindex="-1"])',
    );
    focusable[0]?.focus();
  }, []);

  // Keyboard navigation: Tab key cycles within the dialog.
  const handleDialogKeyDown = useCallback(
    (e: KeyboardEvent<HTMLDivElement>) => {
      if (e.key !== "Tab") return;
      const el = dialogRef.current;
      if (!el) return;
      const focusable = Array.from(
        el.querySelectorAll<HTMLElement>(
          'button:not([disabled]), input:not([disabled]), [tabindex]:not([tabindex="-1"])',
        ),
      );
      if (focusable.length === 0) return;
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      if (e.shiftKey) {
        if (document.activeElement === first) {
          e.preventDefault();
          last.focus();
        }
      } else {
        if (document.activeElement === last) {
          e.preventDefault();
          first.focus();
        }
      }
    },
    [],
  );

  const label = (key: string) => (formatLabel?.(key) ?? key) || "(empty)";

  const content = (
    <>
      {/* Backdrop */}
      <div
        className={styles.backdrop}
        onClick={onClose}
        aria-hidden="true"
        data-testid="group-manager-backdrop"
      />

      {/* Dialog panel */}
      <div
        ref={dialogRef}
        role="dialog"
        aria-modal="true"
        aria-label={`Manage groups for ${fieldLabel}`}
        className={styles.dialog}
        onKeyDown={handleDialogKeyDown}
        data-testid="group-manager-dialog"
      >
        {/* Header */}
        <div className={styles.header}>
          <span className={styles.title}>
            Groups · <span className={styles.fieldName}>{fieldLabel}</span>
          </span>
          <button
            type="button"
            className={styles.closeBtn}
            onClick={onClose}
            aria-label="Close"
            data-testid="group-manager-close"
          >
            ×
          </button>
        </div>

        {/* Active groups list */}
        <div className={styles.body}>
          {memberGroups.length > 0 && (
            <section aria-label="Active groups">
              <div className={styles.sectionLabel}>Active groups</div>
              <div className={styles.groupList}>
                {memberGroups.map((g) => (
                  <div
                    key={g.name}
                    className={styles.groupRow}
                    data-testid={`group-manager-group-${g.name}`}
                  >
                    <div className={styles.groupInfo}>
                      <span className={styles.groupName}>{g.name}</span>
                      <span className={styles.groupMembers}>
                        {g.members.slice(0, 3).map(label).join(", ")}
                        {g.members.length > 3
                          ? ` +${g.members.length - 3} more`
                          : ""}
                      </span>
                    </div>
                    <button
                      type="button"
                      className={styles.removeBtn}
                      onClick={() => handleRemoveGroup(g.name)}
                      aria-label={`Remove group ${g.name}`}
                      data-testid={`group-manager-remove-${g.name}`}
                    >
                      ×
                    </button>
                  </div>
                ))}
              </div>
              <div className={styles.divider} />
            </section>
          )}

          {/* Create a group */}
          <section aria-label="Create a group">
            <div className={styles.sectionLabel}>Create a group</div>

            <input
              type="text"
              className={styles.nameInput}
              placeholder="Group name…"
              value={groupName}
              onChange={(e) => setGroupName(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter" && canAdd) handleAddGroup();
              }}
              aria-label="Group name"
              data-testid="group-manager-name-input"
            />

            {ungroupedValues.length === 0 ? (
              <p className={styles.emptyNote}>
                All members are already in a group.
              </p>
            ) : (
              <div
                className={styles.memberList}
                role="group"
                aria-label="Select members"
              >
                {ungroupedValues.map((val) => (
                  <label
                    key={val}
                    className={styles.memberItem}
                    data-testid={`group-manager-member-${val}`}
                  >
                    <input
                      type="checkbox"
                      checked={selectedMembers.has(val)}
                      onChange={() => toggleMember(val)}
                    />
                    <span>{label(val)}</span>
                  </label>
                ))}
              </div>
            )}

            <div className={styles.addRow}>
              <button
                type="button"
                className={styles.addBtn}
                disabled={!canAdd}
                onClick={handleAddGroup}
                data-testid="group-manager-add-btn"
              >
                Add Group
              </button>
              {selectedMembers.size === 1 && (
                <span className={styles.addHint}>
                  Select at least 2 members
                </span>
              )}
            </div>
          </section>
        </div>

        {/* Footer */}
        <div className={styles.footer}>
          <button
            type="button"
            className={styles.doneBtn}
            onClick={onClose}
            data-testid="group-manager-done"
          >
            Done
          </button>
        </div>
      </div>
    </>
  );

  const target = portalTarget ?? document.body;
  return createPortal(content, target) as ReactElement;
};

export default GroupManagerDialog;
