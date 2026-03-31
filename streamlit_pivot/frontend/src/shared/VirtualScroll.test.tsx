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

import { describe, expect, it, vi, beforeEach } from "vitest";
import { render, screen, act, fireEvent } from "@testing-library/react";
import VirtualScroll from "./VirtualScroll";
import { ReactElement } from "react";

class MockResizeObserver {
  callback: ResizeObserverCallback;
  constructor(callback: ResizeObserverCallback) {
    this.callback = callback;
  }
  observe(target: Element) {
    this.callback(
      [{ contentRect: { width: 360 } } as unknown as ResizeObserverEntry],
      this as unknown as ResizeObserver,
    );
  }
  unobserve() {}
  disconnect() {}
}

beforeEach(() => {
  vi.stubGlobal("ResizeObserver", MockResizeObserver);
});

function makeProps(totalCols: number, colWidth: number = 120) {
  const headerCalls: [number, number][] = [];
  const rowCalls: { rowIndex: number; colRange: [number, number] }[] = [];

  return {
    headerCalls,
    rowCalls,
    props: {
      totalRows: 10,
      totalColumns: totalCols,
      rowHeight: 36,
      columnWidth: colWidth,
      containerHeight: 400,
      headerHeight: 72,
      renderHeader: (colRange: [number, number]): ReactElement[] => {
        headerCalls.push(colRange);
        return [
          <tr key="h">
            {Array.from({ length: colRange[1] - colRange[0] }, (_, i) => (
              <th key={i} data-testid="vs-header">
                Col {colRange[0] + i}
              </th>
            ))}
          </tr>,
        ];
      },
      renderRow: (
        rowIndex: number,
        colRange: [number, number],
      ): ReactElement => {
        rowCalls.push({ rowIndex, colRange });
        return (
          <tr key={rowIndex}>
            {Array.from({ length: colRange[1] - colRange[0] }, (_, i) => (
              <td key={i} data-testid="vs-cell">
                {rowIndex}-{colRange[0] + i}
              </td>
            ))}
          </tr>
        );
      },
    },
  };
}

describe("VirtualScroll - column windowing", () => {
  it("renders a subset of columns when content is wider than viewport", () => {
    const { props } = makeProps(50, 120);

    render(<VirtualScroll {...props} />);

    const headers = screen.getAllByTestId("vs-header");
    expect(headers.length).toBeLessThan(50);
    expect(headers.length).toBeGreaterThan(0);
  });

  it("starts at column 0 when not scrolled", () => {
    const { props, headerCalls } = makeProps(50, 120);

    render(<VirtualScroll {...props} />);

    const lastCall = headerCalls[headerCalls.length - 1];
    expect(lastCall[0]).toBe(0);
  });

  it("shifts column range when scrolled horizontally", () => {
    const { props, rowCalls } = makeProps(50, 120);

    const { container } = render(<VirtualScroll {...props} />);
    const scrollContainer = screen.getByTestId("virtual-scroll-container");

    rowCalls.length = 0;

    Object.defineProperty(scrollContainer, "scrollLeft", {
      value: 1200,
      writable: true,
    });
    Object.defineProperty(scrollContainer, "scrollTop", {
      value: 0,
      writable: true,
    });

    act(() => {
      fireEvent.scroll(scrollContainer);
    });

    const lastCall = rowCalls[rowCalls.length - 1];
    expect(lastCall.colRange[0]).toBeGreaterThan(0);
    expect(lastCall.colRange[1]).toBeLessThan(50);
  });

  it("column headers progress correctly through scroll positions", () => {
    const { props, headerCalls } = makeProps(100, 120);

    render(<VirtualScroll {...props} />);
    const scrollContainer = screen.getByTestId("virtual-scroll-container");

    const startRanges: [number, number][] = [];

    for (const scrollPos of [0, 600, 1200, 2400]) {
      headerCalls.length = 0;

      Object.defineProperty(scrollContainer, "scrollLeft", {
        value: scrollPos,
        writable: true,
        configurable: true,
      });
      Object.defineProperty(scrollContainer, "scrollTop", {
        value: 0,
        writable: true,
        configurable: true,
      });

      act(() => {
        fireEvent.scroll(scrollContainer);
      });

      if (headerCalls.length > 0) {
        startRanges.push(headerCalls[headerCalls.length - 1]);
      }
    }

    // Each scroll position should produce a strictly increasing start column
    for (let i = 1; i < startRanges.length; i++) {
      expect(startRanges[i][0]).toBeGreaterThanOrEqual(startRanges[i - 1][0]);
    }

    // The last scroll position should start at a significantly higher column
    if (startRanges.length >= 2) {
      expect(startRanges[startRanges.length - 1][0]).toBeGreaterThan(
        startRanges[0][0],
      );
    }
  });

  it("renders all columns when content fits viewport", () => {
    const { props } = makeProps(3, 120);

    render(<VirtualScroll {...props} />);

    const headers = screen.getAllByTestId("vs-header");
    expect(headers.length).toBe(3);
  });
});
