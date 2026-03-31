# Copyright 2025 Snowflake Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import subprocess
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
FRONTEND_DIR = SCRIPT_DIR.parent / "streamlit_pivot/frontend"
OUTPUT_FILE = SCRIPT_DIR.parent / "NOTICES"

if __name__ == "__main__":
    with open(OUTPUT_FILE, "w") as outfile:
        custom_format = '{"licenseText":true}'
        process = subprocess.Popen(
            [
                "npx",
                "license-checker-rseidelsohn",
                "--production",
                "--plainVertical",
                "--customPath",
                "/dev/stdin",
                "--excludePackages",
                "streamlit_pivot",
            ],
            cwd=str(FRONTEND_DIR),
            stdin=subprocess.PIPE,
            stdout=outfile,
            stderr=subprocess.PIPE,
            text=True,
        )
        process.communicate(input=custom_format)
        if process.returncode != 0:
            raise subprocess.CalledProcessError(process.returncode, process.args)
