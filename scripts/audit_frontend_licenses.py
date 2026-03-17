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

"""Audit the licenses of all frontend dependencies. If any dependency has an
unacceptable license, print it out and exit with an error code.
"""

import json
import subprocess
import sys
from pathlib import Path
from typing import NoReturn, TypeAlias, cast

PackageInfo: TypeAlias = tuple[str, str]

SCRIPT_DIR = Path(__file__).resolve().parent
FRONTEND_DIR = SCRIPT_DIR.parent / "streamlit_pivot_table/frontend"

ACCEPTABLE_LICENSES = {
    "MIT",
    "Apache-2.0",
    "Apache-2.0 WITH LLVM-exception",
    "0BSD",
    "BlueOak-1.0.0",
    "BSD-2-Clause",
    "BSD-3-Clause",
    "ISC",
    "CC0-1.0",
    "CC-BY-3.0",
    "CC-BY-4.0",
    "Python-2.0",
    "Zlib",
    "Unlicense",
    "WTFPL",
    "BSD",
    "(MIT OR Apache-2.0)",
    "(MPL-2.0 OR Apache-2.0)",
    "(MIT OR CC0-1.0)",
    "(Apache-2.0 OR MPL-1.1)",
    "(BSD-3-Clause OR GPL-2.0)",
    "(MIT AND BSD-3-Clause)",
    "(MIT AND Zlib)",
    "(WTFPL OR MIT)",
    "(AFL-2.1 OR BSD-3-Clause)",
    "(BSD-2-Clause OR MIT OR Apache-2.0)",
    "Apache*",
    "(MIT OR GPL-3.0-or-later)",
    "Apache-2.0 AND MIT",
}

PACKAGE_EXCEPTIONS: set[PackageInfo] = set()


def get_license_type(package: PackageInfo) -> str:
    return package[1]


def check_licenses(licenses) -> NoReturn:
    packages = []
    for license in licenses:
        license_json = json.loads(license)
        if license_json.get("type") == "warning":
            continue
        if "value" not in license_json:
            continue
        license_name = license_json["value"]
        for package_name in license_json["children"].keys():
            packages.append(cast("PackageInfo", (package_name, license_name)))

    unused_exceptions = PACKAGE_EXCEPTIONS.difference(set(packages))
    if len(unused_exceptions) > 0:
        for exception in sorted(list(unused_exceptions)):
            print(f"Unused package exception, please remove: {exception}")

    bad_packages = [
        package
        for package in packages
        if (get_license_type(package) not in ACCEPTABLE_LICENSES)
        and (package not in PACKAGE_EXCEPTIONS)
        and "workspace-aggregator" not in package[0]
    ]

    if len(bad_packages) > 0:
        for package in bad_packages:
            print(f"Unacceptable license: '{get_license_type(package)}' (in {package})")
        print(f"{len(bad_packages)} unacceptable licenses")
        sys.exit(1)

    print(f"No unacceptable licenses: {len(packages)} checked")
    sys.exit(0)


def main() -> NoReturn:
    licenses_output = (
        subprocess.check_output(
            ["yarn", "licenses", "list", "--json", "--production", "--recursive"],
            cwd=str(FRONTEND_DIR),
        )
        .decode()
        .splitlines()
    )
    check_licenses(licenses_output)


if __name__ == "__main__":
    main()
