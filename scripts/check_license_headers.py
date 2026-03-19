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

import re
import subprocess
import sys
from pathlib import Path

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use "
        f"it as a module.To run this script, run the ./{__file__} command"
    )

SCRIPT_DIR = Path(__file__).resolve().parent
LICENSE_TEXT = (SCRIPT_DIR / "license-template.txt").read_text().splitlines()[0]

IGNORE_PATTERN = re.compile(
    # Exclude CI files.
    r"^\.(github)/"
    # Exclude images.
    r"|\.(?:png|jpg|jpeg|gif|ttf|woff|otf|eot|woff2|ico|svg)$"
    # Exclude files, because they make it obvious which product they relate to.
    r"|(LICENSE|NOTICES|CODE_OF_CONDUCT\.md|README\.md|SKILL\.md)$"
    # Exclude files, because they do not support comments
    r"|\.(json|prettierrc|nvmrc)$"
    # Exclude yarn.lock
    r"|yarn\.lock$"
    # Exclude .yarn folder
    r"|streamlit_pivot_table\/frontend\/\.yarn/"
    # .gitignore
    r"|\.(gitignore)$"
    # MANIFEST.in
    r"|MANIFEST\.in$"
    # .env file
    r"|streamlit_pivot_table\/frontend\/\.env$"
    # Vendored files
    r"|^streamlit_pivot_table/frontend/public/"
    # Exclude e2e_playwright/test-requirements.txt
    r"|e2e_playwright/test-requirements.txt$"
    # Exclude CSV data files
    r"|\.csv$"
    # Exclude pytest.ini (does not support standard comments)
    r"|pytest\.ini$"
    # Exclude the pyproject.toml
    r"|pyproject\.toml$",
    re.IGNORECASE,
)


def main():
    git_files = sorted(
        subprocess.check_output(["git", "ls-files", "--no-empty-directory"])
        .decode()
        .strip()
        .splitlines()
    )

    invalid_files_count = 0
    for fileloc in git_files:
        print("Checking file:", fileloc)
        if IGNORE_PATTERN.search(fileloc):
            continue
        filepath = Path(fileloc)
        if not filepath.is_file():
            continue

        try:
            file_content = filepath.read_text()
            if LICENSE_TEXT not in file_content:
                print("Found file without license header", fileloc)
                invalid_files_count += 1
        except Exception:
            print(
                f"Failed to open the file: {fileloc}. Is it binary file?",
            )
            invalid_files_count += 1

    print("Invalid files count:", invalid_files_count)
    if invalid_files_count > 0:
        sys.exit(1)


main()
