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
  FrontendRenderer,
  FrontendRendererArgs,
} from "@streamlit/component-v2-lib";
import { StrictMode } from "react";
import { createRoot, Root } from "react-dom/client";

import PivotRoot, { PivotRootState } from "./PivotRoot";
import { DEFAULT_CONFIG, PivotTableData } from "./engine/types";

const reactRoots: WeakMap<FrontendRendererArgs["parentElement"], Root> =
  new WeakMap();

const PivotTableRoot: FrontendRenderer<PivotRootState, PivotTableData> = (
  args,
) => {
  const { data, parentElement, setStateValue, setTriggerValue } = args;

  const rootElement = parentElement.querySelector(".react-root");

  if (!rootElement) {
    throw new Error("Unexpected: React root element not found");
  }

  let reactRoot = reactRoots.get(parentElement);
  if (!reactRoot) {
    reactRoot = createRoot(rootElement);
    reactRoots.set(parentElement, reactRoot);
  }

  reactRoot.render(
    <StrictMode>
      <PivotRoot
        config={data?.config ?? DEFAULT_CONFIG}
        dataframe={data?.dataframe ?? null}
        height={data?.height ?? null}
        max_height={data?.max_height}
        null_handling={data?.null_handling}
        hidden_attributes={data?.hidden_attributes}
        hidden_from_aggregators={data?.hidden_from_aggregators}
        hidden_from_drag_drop={data?.hidden_from_drag_drop}
        sorters={data?.sorters}
        locked={data?.locked}
        menu_limit={data?.menu_limit}
        enable_drilldown={data?.enable_drilldown}
        export_filename={data?.export_filename}
        setStateValue={setStateValue}
        setTriggerValue={setTriggerValue}
      />
    </StrictMode>,
  );

  return () => {
    const root = reactRoots.get(parentElement);
    if (root) {
      root.unmount();
      reactRoots.delete(parentElement);
    }
  };
};

export default PivotTableRoot;
