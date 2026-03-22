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
  type AggregationType,
  normalizeAggregationConfig,
  type PivotConfigV1,
} from "./engine/types";

type TestConfigOverrides = Partial<Omit<PivotConfigV1, "aggregation">> & {
  aggregation?: AggregationType | PivotConfigV1["aggregation"];
};

export function makeConfig(
  overrides: TestConfigOverrides = {},
): PivotConfigV1 {
  const { aggregation: aggregationOverride, ...restOverrides } = overrides;
  const values = overrides.values ?? ["revenue"];
  const config = {
    version: 1,
    rows: ["region"],
    columns: ["year"],
    values,
    show_totals: true,
    empty_cell_value: "-",
    interactive: true,
    ...restOverrides,
  } as PivotConfigV1;
  config.values = values;
  config.aggregation = normalizeAggregationConfig(aggregationOverride, values);
  return config;
}
