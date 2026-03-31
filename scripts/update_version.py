#!/usr/bin/env python
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

"""Update the version string in all relevant files.

Usage:
    python scripts/update_version.py 1.2.3
"""

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PYPROJECT_FILES = [
    ROOT / "pyproject.toml",
    ROOT / "streamlit_pivot" / "pyproject.toml",
]
FRONTEND_DIR = ROOT / "streamlit_pivot" / "frontend"
TEST_REQUIREMENTS = ROOT / "e2e_playwright" / "test-requirements.txt"
PYPROJECT_VERSION_RE = re.compile(r'^(version\s*=\s*")([^"]+)(")', re.MULTILINE)
WHEEL_RE = re.compile(r"(dist/streamlit_pivot-)[^-]+(-py3-none-any\.whl)")


def update_version(new_version: str) -> None:
    for filepath in PYPROJECT_FILES:
        content = filepath.read_text()
        updated, count = PYPROJECT_VERSION_RE.subn(rf"\g<1>{new_version}\g<3>", content)
        if count == 0:
            print(f"WARNING: No version string found in {filepath}")
        else:
            filepath.write_text(updated)
            print(f"Updated {filepath} -> {new_version}")

    for name in ("package.json", "package-lock.json"):
        pkg_file = FRONTEND_DIR / name
        data = json.loads(pkg_file.read_text())
        data["version"] = new_version
        if (
            name == "package-lock.json"
            and "packages" in data
            and "" in data["packages"]
        ):
            data["packages"][""]["version"] = new_version
        pkg_file.write_text(json.dumps(data, indent=2) + "\n")
        print(f"Updated {pkg_file} -> {new_version}")

    content = TEST_REQUIREMENTS.read_text()
    updated, count = WHEEL_RE.subn(rf"\g<1>{new_version}\g<2>", content)
    if count:
        TEST_REQUIREMENTS.write_text(updated)
        print(f"Updated {TEST_REQUIREMENTS} -> {new_version}")
    else:
        print(f"WARNING: No wheel reference found in {TEST_REQUIREMENTS}")


def main() -> None:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <new-version>")
        sys.exit(1)

    new_version = sys.argv[1]
    if not re.match(r"^\d+\.\d+\.\d+$", new_version):
        print(f"Invalid version format: {new_version} (expected X.Y.Z)")
        sys.exit(1)

    update_version(new_version)
    print("Done! Remember to commit the changes.")


if __name__ == "__main__":
    main()
