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
  type CSSProperties,
  type FC,
  type ReactElement,
  type ReactNode,
} from "react";
import { useSortable } from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import type { AggregationType, ShowValuesAs } from "../engine/types";
import styles from "./Toolbar.module.css";

const SEPARATOR = "::";

export function makeItemId(zone: string, field: string): string {
  return `${zone}${SEPARATOR}${field}`;
}

export function parseItemId(id: string): { zone: string; field: string } {
  const idx = id.indexOf(SEPARATOR);
  if (idx === -1) return { zone: "", field: id };
  return { zone: id.slice(0, idx), field: id.slice(idx + SEPARATOR.length) };
}

export interface FieldChipProps {
  field: string;
  displayLabel?: string;
  testId: string;
  isFrozen?: boolean;
  disabled?: boolean;
  isValuesField?: boolean;
  aggregationLabel?: string;
  showAsBadge?: boolean;
  showAsBadgeTitle?: string;
  hasFilter?: boolean;
  onRemove?: (field: string) => void;
  aggregationControl?: ReactNode;
  /** Replaces the static % badge with an interactive display-mode picker */
  displayControl?: ReactNode;
}

const GripDotsIcon: FC = () => (
  <svg
    className={styles.dragHandle}
    width="8"
    height="14"
    viewBox="0 0 8 14"
    fill="currentColor"
    aria-hidden="true"
  >
    <circle cx="2" cy="2" r="1.2" />
    <circle cx="6" cy="2" r="1.2" />
    <circle cx="2" cy="7" r="1.2" />
    <circle cx="6" cy="7" r="1.2" />
    <circle cx="2" cy="12" r="1.2" />
    <circle cx="6" cy="12" r="1.2" />
  </svg>
);

function ChipContent({
  field,
  displayLabel,
  testId,
  isFrozen,
  disabled,
  isValuesField,
  aggregationLabel,
  showAsBadge,
  showAsBadgeTitle,
  hasFilter,
  onRemove,
  aggregationControl,
  displayControl,
  showDragHandle,
}: FieldChipProps & { showDragHandle?: boolean }): ReactElement {
  return (
    <>
      {showDragHandle && <GripDotsIcon />}
      <span
        className={styles.chipLabelText}
        data-testid={`${testId}-chip-label-${field}`}
      >
        <span className={styles.chipPrimaryText}>{displayLabel ?? field}</span>
        {isValuesField && aggregationLabel && (
          <span
            className={styles.chipAggregationLabel}
            data-testid={`${testId}-aggregation-label-${field}`}
          >
            {" "}
            ({aggregationLabel})
          </span>
        )}
      </span>
      {displayControl ??
        (showAsBadge && (
          <span
            className={styles.chipShowAsBadge}
            title={showAsBadgeTitle}
            data-testid={`${testId}-show-as-badge-${field}`}
          >
            %
          </span>
        ))}
      {hasFilter && (
        <span
          className={styles.chipFilterDot}
          title={`Filtered: ${field}`}
          data-testid={`${testId}-filter-indicator-${field}`}
        />
      )}
      {aggregationControl}
      {!disabled && !isFrozen && onRemove && (
        <button
          className={styles.chipRemove}
          onClick={(e) => {
            e.stopPropagation();
            onRemove(field);
          }}
          aria-label={`Remove ${field}`}
          data-testid={`${testId}-remove-${field}`}
        >
          ×
        </button>
      )}
    </>
  );
}

export interface SortableFieldChipProps extends FieldChipProps {
  id: string;
  zone: string;
  activeId?: string | null;
}

export const SortableFieldChip: FC<SortableFieldChipProps> = ({
  id,
  zone,
  field,
  isFrozen,
  disabled,
  activeId,
  ...chipProps
}): ReactElement => {
  const isDragDisabled = !!isFrozen || !!disabled;
  const {
    attributes,
    listeners,
    setNodeRef,
    transform,
    transition,
    isDragging,
  } = useSortable({
    id,
    disabled: isDragDisabled,
    data: { zone, field },
  });

  const style: CSSProperties = {
    transform: CSS.Transform.toString(transform),
    transition,
    opacity: isDragging ? 0.3 : undefined,
  };

  return (
    <span
      ref={setNodeRef}
      className={`${styles.chip} ${isDragging ? styles.chipDragging : ""}`}
      style={style}
      {...attributes}
      {...(isDragDisabled ? {} : listeners)}
      data-testid={`${chipProps.testId}-chip-${field}`}
    >
      <ChipContent
        field={field}
        isFrozen={isFrozen}
        disabled={disabled}
        showDragHandle={!isDragDisabled}
        {...chipProps}
      />
    </span>
  );
};

export const DragOverlayChip: FC<FieldChipProps> = (props): ReactElement => {
  return (
    <span className={`${styles.chip} ${styles.chipOverlay}`}>
      <ChipContent {...props} showDragHandle={true} />
    </span>
  );
};

// ---------------------------------------------------------------------------
// Synthetic (fx) measure chip — sortable within the Values zone.
// ---------------------------------------------------------------------------

export interface SyntheticChipDisplayProps {
  syntheticId: string;
  label: string;
  testId: string;
  disabled?: boolean;
  onRemove?: (syntheticId: string) => void;
  showDragHandle?: boolean;
}

function SyntheticChipContent({
  syntheticId,
  label,
  testId,
  disabled,
  onRemove,
  showDragHandle,
}: SyntheticChipDisplayProps): ReactElement {
  return (
    <>
      {showDragHandle && <GripDotsIcon />}
      <span className={styles.aggChipIcon}>fx</span>
      <span
        className={styles.chipLabelText}
        data-testid={`${testId}-synthetic-label-${syntheticId}`}
      >
        <span className={styles.chipPrimaryText}>{label}</span>
      </span>
      {!disabled && onRemove && (
        <button
          type="button"
          className={styles.chipRemove}
          onMouseDown={(e) => e.stopPropagation()}
          onClick={(e) => {
            e.stopPropagation();
            onRemove(syntheticId);
          }}
          aria-label={`Remove ${label}`}
          data-testid={`${testId}-synthetic-remove-${syntheticId}`}
        >
          ×
        </button>
      )}
    </>
  );
}

export interface SortableSyntheticChipProps extends SyntheticChipDisplayProps {
  id: string;
}

export const SortableSyntheticChip: FC<SortableSyntheticChipProps> = ({
  id,
  syntheticId,
  label,
  testId,
  disabled,
  onRemove,
}): ReactElement => {
  const {
    attributes,
    listeners,
    setNodeRef,
    transform,
    transition,
    isDragging,
  } = useSortable({
    id,
    disabled,
    data: { type: "synthetic", syntheticId },
  });

  const style: CSSProperties = {
    transform: CSS.Transform.toString(transform),
    transition,
    opacity: isDragging ? 0.3 : undefined,
  };

  return (
    <span
      ref={setNodeRef}
      className={`${styles.chip} ${isDragging ? styles.chipDragging : ""}`}
      style={style}
      {...attributes}
      {...(disabled ? {} : listeners)}
      data-testid={`${testId}-synthetic-${syntheticId}`}
    >
      <SyntheticChipContent
        syntheticId={syntheticId}
        label={label}
        testId={testId}
        disabled={disabled}
        onRemove={onRemove}
        showDragHandle={!disabled}
      />
    </span>
  );
};

export const SyntheticDragOverlayChip: FC<SyntheticChipDisplayProps> = (
  props,
): ReactElement => {
  return (
    <span className={`${styles.chip} ${styles.chipOverlay}`}>
      <SyntheticChipContent {...props} showDragHandle={true} />
    </span>
  );
};
