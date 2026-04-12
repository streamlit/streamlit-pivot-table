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
import { render, act } from "@testing-library/react";
import { tableToIPC, tableFromArrays } from "apache-arrow";
import PivotRoot from "./PivotRoot";
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
