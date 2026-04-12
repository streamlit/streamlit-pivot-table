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

import react from "@vitejs/plugin-react";
import process from "node:process";
import { defineConfig, loadEnv, UserConfig } from "vite";

/**
 * Vite configuration for Streamlit Custom Component v2 development using React.
 *
 * @see https://vitejs.dev/config/ for complete Vite configuration options.
 */
export default defineConfig(() => {
  const isProd = process.env.NODE_ENV === "production";
  const isDev = !isProd;

  return {
    base: "./",
    plugins: [react()],
    define: {
      // We are building in library mode, we need to define the NODE_ENV
      // variable to prevent issues when executing the JS.
      "process.env.NODE_ENV": JSON.stringify(process.env.NODE_ENV),
    },
    build: {
      minify: isDev ? false : "esbuild",
      outDir: "build",
      sourcemap: isDev,
      lib: {
        entry: "./src/index.tsx",
        name: "MyComponent",
        formats: ["es"],
        fileName: "index-[hash]",
      },
      rollupOptions: {
        output: {
          // Vite library mode may split the entry into a tiny re-export stub
          // plus the main bundle, both named index-*. Streamlit CCv2 matches
          // `js="index-*.js"` and requires exactly one file. Use chunkFileNames
          // to push non-entry chunks to a different naming pattern.
          chunkFileNames: "chunk-[hash].js",
        },
      },
      ...(!isDev && {
        esbuild: {
          drop: ["console", "debugger"],
          minifyIdentifiers: true,
          minifySyntax: true,
          minifyWhitespace: true,
        },
      }),
    },
  } satisfies UserConfig;
});
