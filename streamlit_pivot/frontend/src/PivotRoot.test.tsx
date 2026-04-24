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

import { describe, expect, it, vi } from "vitest";
import { render, act, screen, fireEvent } from "@testing-library/react";
import { tableToIPC, tableFromArrays } from "apache-arrow";
import PivotRoot from "./PivotRoot";
import type { PivotRootProps } from "./PivotRoot";
import { makeConfig } from "./test-utils";

function makeArrowBytes(data: Record<string, unknown[]>): Uint8Array {
  return tableToIPC(tableFromArrays(data));
}

describe("PivotRoot - source_row_count metric", () => {
  it("uses source_row_count for sourceRows when provided (hybrid mode)", async () => {
    const dataframe = makeArrowBytes({
      region: ["US", "EU"],
      year: ["2023", "2023"],
      revenue: [100, 200],
    });

    const setStateValue = vi.fn();
    const setTriggerValue = vi.fn();
    const config = makeConfig();

    let container: HTMLElement;
    act(() => {
      const result = render(
        <PivotRoot
          config={config}
          dataframe={dataframe}
          height={null}
          max_height={500}
          source_row_count={100000}
          execution_mode="threshold_hybrid"
          server_mode_reason="forced"
          setStateValue={setStateValue}
          setTriggerValue={setTriggerValue}
        />,
      );
      container = result.container;
    });

    await act(async () => {
      await new Promise((r) => setTimeout(r, 50));
    });

    const perfEl = container!.querySelector("[data-perf-metrics]");
    expect(perfEl).not.toBeNull();
    const metrics = JSON.parse(
      perfEl!.getAttribute("data-perf-metrics") ?? "{}",
    );
    expect(metrics.sourceRows).toBe(100000);
  });

  it("falls back to pivotData.recordCount when source_row_count is absent", async () => {
    const dataframe = makeArrowBytes({
      region: ["US", "EU", "JP"],
      year: ["2023", "2023", "2024"],
      revenue: [100, 200, 300],
    });

    const setStateValue = vi.fn();
    const setTriggerValue = vi.fn();
    const config = makeConfig();

    let container: HTMLElement;
    act(() => {
      const result = render(
        <PivotRoot
          config={config}
          dataframe={dataframe}
          height={null}
          max_height={500}
          execution_mode="client_only"
          setStateValue={setStateValue}
          setTriggerValue={setTriggerValue}
        />,
      );
      container = result.container;
    });

    await act(async () => {
      await new Promise((r) => setTimeout(r, 50));
    });

    const perfEl = container!.querySelector("[data-perf-metrics]");
    expect(perfEl).not.toBeNull();
    const metrics = JSON.parse(
      perfEl!.getAttribute("data-perf-metrics") ?? "{}",
    );
    expect(metrics.sourceRows).toBe(3);
  });
});

// ---------------------------------------------------------------------------
// Adaptive grain invalidation effect
// ---------------------------------------------------------------------------

function makeTemporalArrowBytes() {
  return makeArrowBytes({
    region: ["US", "EU"],
    order_date: ["2024-01-01", "2024-06-01"],
    ship_date: ["2024-01-05", "2024-06-05"],
    revenue: [100, 200],
  });
}

function makeBaseProps(
  overrides: Partial<PivotRootProps> = {},
): PivotRootProps {
  return {
    config: makeConfig({
      rows: ["region"],
      columns: ["order_date"],
      values: ["revenue"],
    }),
    dataframe: makeTemporalArrowBytes(),
    height: null,
    max_height: 500,
    execution_mode: "client_only" as const,
    original_column_types: { order_date: "date", ship_date: "date" },
    setStateValue: vi.fn(),
    setTriggerValue: vi.fn(),
    ...overrides,
  };
}

async function flush() {
  await act(async () => {
    await new Promise((r) => setTimeout(r, 50));
  });
}

function findConfigCall(
  setStateValue: ReturnType<typeof vi.fn>,
): Record<string, unknown> | undefined {
  const call = setStateValue.mock.calls.find(
    (args: unknown[]) => args[0] === "config",
  );
  return call ? (call[1] as Record<string, unknown>) : undefined;
}

describe("PivotRoot - adaptive grain invalidation", () => {
  it("clears filters for affected auto field when adaptive grain changes", async () => {
    const setStateValue = vi.fn();
    const config = makeConfig({
      rows: ["region"],
      columns: ["order_date"],
      values: ["revenue"],
      filters: {
        order_date: { include: ["2024-Q1"] },
        region: { include: ["US"] },
      },
    });
    const props = makeBaseProps({
      config,
      adaptive_date_grains: { order_date: "quarter" },
      setStateValue,
    });

    const { rerender } = render(<PivotRoot {...props} />);
    await flush();
    setStateValue.mockClear();

    rerender(
      <PivotRoot {...props} adaptive_date_grains={{ order_date: "year" }} />,
    );
    await flush();

    const patched = findConfigCall(setStateValue);
    expect(patched).toBeDefined();
    expect(patched!.filters).toBeDefined();
    const filters = patched!.filters as Record<string, unknown>;
    expect(filters["region"]).toBeDefined();
    expect(filters["order_date"]).toBeUndefined();
  });

  it("clears collapsed_col_groups (not collapsed_groups) when column field affected", async () => {
    const setStateValue = vi.fn();
    const config = makeConfig({
      rows: ["region"],
      columns: ["order_date"],
      values: ["revenue"],
      collapsed_groups: ["region:US"],
      collapsed_col_groups: ["order_date:2024-Q1"],
    });
    const props = makeBaseProps({
      config,
      adaptive_date_grains: { order_date: "quarter" },
      setStateValue,
    });

    const { rerender } = render(<PivotRoot {...props} />);
    await flush();
    setStateValue.mockClear();

    rerender(
      <PivotRoot {...props} adaptive_date_grains={{ order_date: "year" }} />,
    );
    await flush();

    const patched = findConfigCall(setStateValue);
    expect(patched).toBeDefined();
    expect(patched!.collapsed_groups).toEqual(["region:US"]);
    expect(patched!.collapsed_col_groups).toBeUndefined();
  });

  it("clears collapsed_groups (not collapsed_col_groups) when row field affected", async () => {
    const setStateValue = vi.fn();
    const config = makeConfig({
      rows: ["order_date"],
      columns: ["region"],
      values: ["revenue"],
      collapsed_groups: ["order_date:2024-Q1"],
      collapsed_col_groups: ["region:US"],
    });
    const props = makeBaseProps({
      config,
      adaptive_date_grains: { order_date: "quarter" },
      setStateValue,
    });

    const { rerender } = render(<PivotRoot {...props} />);
    await flush();
    setStateValue.mockClear();

    rerender(
      <PivotRoot {...props} adaptive_date_grains={{ order_date: "year" }} />,
    );
    await flush();

    const patched = findConfigCall(setStateValue);
    expect(patched).toBeDefined();
    expect(patched!.collapsed_groups).toBeUndefined();
    expect(patched!.collapsed_col_groups).toEqual(["region:US"]);
  });

  it("strips period comparison show_values_as when comparison axis field is affected", async () => {
    const setStateValue = vi.fn();
    const config = makeConfig({
      rows: ["region"],
      columns: ["order_date"],
      values: ["revenue"],
      show_values_as: { revenue: "diff_from_prev" },
    });
    const props = makeBaseProps({
      config,
      adaptive_date_grains: { order_date: "quarter" },
      setStateValue,
    });

    const { rerender } = render(<PivotRoot {...props} />);
    await flush();
    setStateValue.mockClear();

    rerender(
      <PivotRoot {...props} adaptive_date_grains={{ order_date: "year" }} />,
    );
    await flush();

    const patched = findConfigCall(setStateValue);
    expect(patched).toBeDefined();
    expect(patched!.show_values_as).toBeUndefined();
  });

  it("preserves period comparison show_values_as when comparison axis field is NOT affected", async () => {
    const setStateValue = vi.fn();
    // order_date on columns is the comparison axis; ship_date on rows is affected
    const config = makeConfig({
      rows: ["ship_date"],
      columns: ["order_date"],
      values: ["revenue"],
      show_values_as: { revenue: "diff_from_prev" },
    });
    const props = makeBaseProps({
      config,
      adaptive_date_grains: { order_date: "quarter", ship_date: "quarter" },
      setStateValue,
    });

    const { rerender } = render(<PivotRoot {...props} />);
    await flush();
    setStateValue.mockClear();

    // Only ship_date grain changes; order_date (comparison axis) is stable
    rerender(
      <PivotRoot
        {...props}
        adaptive_date_grains={{ order_date: "quarter", ship_date: "year" }}
      />,
    );
    await flush();

    const patched = findConfigCall(setStateValue);
    expect(patched).toBeDefined();
    expect(patched!.show_values_as).toEqual({ revenue: "diff_from_prev" });
  });

  it("skips fields with explicit date_grains overrides", async () => {
    const setStateValue = vi.fn();
    const config = makeConfig({
      rows: ["region"],
      columns: ["order_date"],
      values: ["revenue"],
      date_grains: { order_date: "month" },
      filters: { order_date: { include: ["2024-01"] } },
    });
    const props = makeBaseProps({
      config,
      adaptive_date_grains: { order_date: "quarter" },
      setStateValue,
    });

    const { rerender } = render(<PivotRoot {...props} />);
    await flush();
    setStateValue.mockClear();

    rerender(
      <PivotRoot {...props} adaptive_date_grains={{ order_date: "year" }} />,
    );
    await flush();

    expect(findConfigCall(setStateValue)).toBeUndefined();
  });

  it("does not fire when adaptive grains are unchanged", async () => {
    const setStateValue = vi.fn();
    const grains = { order_date: "quarter" as const };
    const props = makeBaseProps({
      adaptive_date_grains: grains,
      setStateValue,
    });

    const { rerender } = render(<PivotRoot {...props} />);
    await flush();
    setStateValue.mockClear();

    // Same object reference — no change
    rerender(<PivotRoot {...props} adaptive_date_grains={grains} />);
    await flush();

    expect(findConfigCall(setStateValue)).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// PivotRoot — FilterBar visibility and handleRemoveFilterField
// ---------------------------------------------------------------------------

function makeSimpleDataframe(): Uint8Array {
  return makeArrowBytes({
    region: ["East", "West"],
    category: ["A", "B"],
    revenue: [100, 200],
  });
}

describe("PivotRoot - FilterBar rendering", () => {
  it("renders FilterBar when filter_fields is non-empty and show_sections is not false", async () => {
    const props = makeBaseProps({
      config: makeConfig({
        rows: ["region"],
        columns: [],
        values: ["revenue"],
        filter_fields: ["category"],
      }),
      dataframe: makeSimpleDataframe(),
      original_column_types: {},
    });
    render(<PivotRoot {...props} />);
    await flush();
    expect(screen.getByTestId("filter-bar")).toBeInTheDocument();
  });

  it("does not render FilterBar when filter_fields is empty", async () => {
    const props = makeBaseProps({
      config: makeConfig({
        rows: ["region"],
        columns: [],
        values: ["revenue"],
      }),
      dataframe: makeSimpleDataframe(),
      original_column_types: {},
    });
    render(<PivotRoot {...props} />);
    await flush();
    expect(screen.queryByTestId("filter-bar")).not.toBeInTheDocument();
  });

  it("does not render FilterBar when show_sections=false", async () => {
    const props = makeBaseProps({
      config: makeConfig({
        rows: ["region"],
        columns: [],
        values: ["revenue"],
        filter_fields: ["category"],
        show_sections: false,
      }),
      dataframe: makeSimpleDataframe(),
      original_column_types: {},
    });
    render(<PivotRoot {...props} />);
    await flush();
    expect(screen.queryByTestId("filter-bar")).not.toBeInTheDocument();
  });
});

describe("PivotRoot - handleRemoveFilterField", () => {
  it("clicking × on FilterBar chip removes field from filter_fields and clears its filter", async () => {
    const setStateValue = vi.fn();
    const props = makeBaseProps({
      config: makeConfig({
        rows: ["region"],
        columns: [],
        values: ["revenue"],
        filter_fields: ["category", "region"],
        filters: { category: { include: ["A"] } },
      }),
      dataframe: makeSimpleDataframe(),
      original_column_types: {},
      setStateValue,
    });
    render(<PivotRoot {...props} />);
    await flush();

    // Click × on the category chip
    fireEvent.click(screen.getByTestId("filter-chip-clear-category"));
    await flush();

    const updatedConfig = findConfigCall(setStateValue);
    expect(updatedConfig).toBeDefined();
    expect((updatedConfig!.filter_fields as string[]) ?? []).not.toContain(
      "category",
    );
    expect(updatedConfig!.filter_fields as string[]).toContain("region");
    // filter for category should be cleared
    expect(
      (updatedConfig!.filters as Record<string, unknown> | undefined)?.[
        "category"
      ],
    ).toBeUndefined();
  });

  it("removing last filter field clears filter_fields entirely", async () => {
    const setStateValue = vi.fn();
    const props = makeBaseProps({
      config: makeConfig({
        rows: ["region"],
        columns: [],
        values: ["revenue"],
        filter_fields: ["category"],
        filters: { category: { include: ["A"] } },
      }),
      dataframe: makeSimpleDataframe(),
      original_column_types: {},
      setStateValue,
    });
    render(<PivotRoot {...props} />);
    await flush();

    fireEvent.click(screen.getByTestId("filter-chip-clear-category"));
    await flush();

    const updatedConfig = findConfigCall(setStateValue);
    expect(updatedConfig).toBeDefined();
    expect(
      (updatedConfig!.filter_fields as string[] | undefined) ?? [],
    ).toHaveLength(0);
    expect(updatedConfig!.filters).toBeUndefined();
  });

  it("removing a dual-role field from filter_fields preserves its header-menu filter", async () => {
    const setStateValue = vi.fn();
    const props = makeBaseProps({
      config: makeConfig({
        rows: ["region"],
        columns: [],
        values: ["revenue"],
        // region is both in rows AND filter_fields — it is dual-role
        filter_fields: ["region", "category"],
        filters: {
          region: { include: ["East"] }, // applied via header-menu / FilterBar
          category: { include: ["A"] },
        },
      }),
      dataframe: makeSimpleDataframe(),
      original_column_types: {},
      setStateValue,
    });
    render(<PivotRoot {...props} />);
    await flush();

    // Remove region from the FilterBar — it must stay in rows, filter must survive
    fireEvent.click(screen.getByTestId("filter-chip-clear-region"));
    await flush();

    const updatedConfig = findConfigCall(setStateValue);
    expect(updatedConfig).toBeDefined();
    // region removed from filter_fields
    expect(
      (updatedConfig!.filter_fields as string[] | undefined) ?? [],
    ).not.toContain("region");
    // but its filter entry MUST be kept (dual-role: still in rows)
    expect(
      (updatedConfig!.filters as Record<string, unknown>)?.["region"],
    ).toEqual({ include: ["East"] });
    // category filter is unaffected
    expect(
      (updatedConfig!.filters as Record<string, unknown>)?.["category"],
    ).toEqual({ include: ["A"] });
  });
});
