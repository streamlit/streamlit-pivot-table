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

import { Component, ErrorInfo, ReactNode } from "react";

interface Props {
  children: ReactNode;
}

interface State {
  error: Error | null;
}

/**
 * Catches uncaught errors in the pivot component tree and renders
 * a fallback UI instead of crashing the entire Streamlit page.
 */
class ErrorBoundary extends Component<Props, State> {
  state: State = { error: null };

  static getDerivedStateFromError(error: Error): State {
    return { error };
  }

  componentDidCatch(error: Error, info: ErrorInfo): void {
    console.error("[PivotTable] Uncaught error:", error, info.componentStack);
  }

  render(): ReactNode {
    if (this.state.error) {
      return (
        <div
          data-testid="pivot-error"
          style={{
            padding: "16px",
            color: "var(--st-text-color)",
            backgroundColor: "var(--st-background-color)",
            border: "1px solid var(--st-yellow-color, var(--st-border-color))",
            borderRadius: "var(--st-base-radius)",
            fontFamily: "var(--st-font)",
            fontSize: "0.875rem",
          }}
        >
          <strong>Pivot Table Error</strong>
          <p style={{ margin: "8px 0 0", opacity: 0.8 }}>
            {this.state.error.message}
          </p>
        </div>
      );
    }
    return this.props.children;
  }
}

export default ErrorBoundary;
