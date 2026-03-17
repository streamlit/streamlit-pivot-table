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
import { render, screen, fireEvent } from "@testing-library/react";
import DrilldownPanel from "./DrilldownPanel";
import { PivotData, type DataRecord } from "../engine/PivotData";
import type { CellClickPayload } from "../engine/types";
import { makeConfig } from "../test-utils";

const SAMPLE_DATA: DataRecord[] = [
  { region: "US", year: "2023", revenue: 100, profit: 40 },
  { region: "US", year: "2024", revenue: 150, profit: 60 },
  { region: "EU", year: "2023", revenue: 200, profit: 80 },
  { region: "EU", year: "2024", revenue: 250, profit: 100 },
  { region: "US", year: "2023", revenue: 50, profit: 20 },
];

function makePivotData(data: DataRecord[] = SAMPLE_DATA) {
  return new PivotData(data, makeConfig());
}

function makePayload(
  overrides: Partial<CellClickPayload> = {},
): CellClickPayload {
  return {
    rowKey: ["US"],
    colKey: ["2023"],
    value: 150,
    filters: { region: "US", year: "2023" },
    ...overrides,
  };
}

describe("DrilldownPanel", () => {
  it("renders the panel with header and table", () => {
    const pd = makePivotData();
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload()}
        onClose={vi.fn()}
      />,
    );
    expect(screen.getByTestId("drilldown-panel")).toBeInTheDocument();
    expect(screen.getByTestId("drilldown-table")).toBeInTheDocument();
    expect(screen.getByText(/region: US/)).toBeInTheDocument();
  });

  it("shows correct record count", () => {
    const pd = makePivotData();
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload()}
        onClose={vi.fn()}
      />,
    );
    // US + 2023 = 2 records
    expect(screen.getByText("2 records")).toBeInTheDocument();
  });

  it("displays all columns from the source data", () => {
    const pd = makePivotData();
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload()}
        onClose={vi.fn()}
      />,
    );
    const table = screen.getByTestId("drilldown-table");
    const headers = table.querySelectorAll("th");
    const headerTexts = Array.from(headers).map((h) => h.textContent);
    expect(headerTexts).toContain("region");
    expect(headerTexts).toContain("year");
    expect(headerTexts).toContain("revenue");
    expect(headerTexts).toContain("profit");
  });

  it("calls onClose when close button is clicked", () => {
    const onClose = vi.fn();
    const pd = makePivotData();
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload()}
        onClose={onClose}
      />,
    );
    fireEvent.click(screen.getByTestId("drilldown-close"));
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it("calls onClose on Escape key", () => {
    const onClose = vi.fn();
    const pd = makePivotData();
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload()}
        onClose={onClose}
      />,
    );
    fireEvent.keyDown(screen.getByTestId("drilldown-panel"), { key: "Escape" });
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it("shows empty message when no records match", () => {
    const pd = makePivotData();
    render(
      <DrilldownPanel
        pivotData={pd}
        payload={makePayload({ filters: { region: "APAC" } })}
        onClose={vi.fn()}
      />,
    );
    expect(screen.getByText(/No matching records found/)).toBeInTheDocument();
  });

  it("shows total key labels for totals", () => {
    const pd = makePivotData();
    const payload = makePayload({
      rowKey: ["Total"],
      colKey: ["2023"],
      filters: { year: "2023" },
    });
    render(
      <DrilldownPanel pivotData={pd} payload={payload} onClose={vi.fn()} />,
    );
    // Should show all 2023 records (3 total: 2 US + 1 EU)
    expect(screen.getByText("3 records")).toBeInTheDocument();
  });
});
