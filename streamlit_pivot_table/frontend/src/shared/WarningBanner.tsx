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

import { FC, ReactElement } from "react";
import styles from "./WarningBanner.module.css";

export interface WarningBannerProps {
  messages: string[];
}

const WarningBanner: FC<WarningBannerProps> = ({
  messages,
}): ReactElement | null => {
  if (messages.length === 0) return null;

  return (
    <div
      data-testid="pivot-warning-banner"
      className={styles.banner}
      role="alert"
    >
      {messages.map((msg, i) => (
        <div key={i}>{msg}</div>
      ))}
    </div>
  );
};

export default WarningBanner;
