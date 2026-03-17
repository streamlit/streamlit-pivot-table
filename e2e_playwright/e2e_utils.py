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

"""Utilities for running Streamlit apps in E2E tests.

Provides StreamlitRunner, a context manager that starts a Streamlit server
in a subprocess, waits for it to become healthy, and tears it down on exit.
"""

import contextlib
import logging
import os
import shlex
import socket
import subprocess
import sys
import time
import typing
from contextlib import closing
from tempfile import TemporaryFile

import requests

LOGGER = logging.getLogger(__file__)


def _find_free_port() -> int:
    """Find and return a free port on the local machine."""
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return int(s.getsockname()[1])


class AsyncSubprocess:
    """Wraps subprocess.Popen to capture output safely via a temp file.

    Using a temp file instead of subprocess.PIPE avoids deadlocks when the
    child process produces large amounts of output.
    """

    def __init__(
        self,
        args: typing.List[str],
        cwd: typing.Optional[str] = None,
        env: typing.Optional[typing.Dict[str, str]] = None,
    ):
        self.args = args
        self.cwd = cwd
        self.env = env
        self._proc: typing.Optional[subprocess.Popen] = None
        self._stdout_file: typing.Optional[typing.IO] = None

    def start(self):
        self._stdout_file = TemporaryFile("w+")
        LOGGER.info("Running command: %s", shlex.join(self.args))
        self._proc = subprocess.Popen(
            self.args,
            cwd=self.cwd,
            stdout=self._stdout_file,
            stderr=subprocess.STDOUT,
            text=True,
            env={**os.environ.copy(), **self.env} if self.env else None,
        )

    def stop(self):
        if self._proc is not None:
            self._proc.terminate()
            self._proc.wait()
            self._proc = None
        if self._stdout_file is not None:
            self._stdout_file.close()
            self._stdout_file = None

    def terminate(self) -> typing.Optional[str]:
        """Terminate the process and return its stdout/stderr as a string."""
        if self._proc is not None:
            self._proc.terminate()
            self._proc.wait()
            self._proc = None

        stdout = None
        if self._stdout_file is not None:
            self._stdout_file.seek(0)
            stdout = self._stdout_file.read()
            self._stdout_file.close()
            self._stdout_file = None

        return stdout

    def __enter__(self) -> "AsyncSubprocess":
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()


class StreamlitRunner:
    """Context manager that runs a Streamlit script on a free port.

    Usage::

        with StreamlitRunner(Path("my_app.py")) as runner:
            page.goto(runner.server_url)
            ...
    """

    def __init__(
        self,
        script_path: os.PathLike,
        server_port: typing.Optional[int] = None,
    ):
        self._process: typing.Optional[AsyncSubprocess] = None
        self.server_port = server_port
        self.script_path = script_path

    def __enter__(self) -> "StreamlitRunner":
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def start(self):
        self.server_port = self.server_port or _find_free_port()
        self._process = AsyncSubprocess(
            [
                sys.executable,
                "-m",
                "streamlit",
                "run",
                str(self.script_path),
                f"--server.port={self.server_port}",
                "--server.headless=true",
                "--browser.gatherUsageStats=false",
                "--global.developmentMode=false",
            ]
        )
        self._process.start()
        if not self.is_server_running():
            self._process.stop()
            raise RuntimeError("Streamlit app failed to start")

    def stop(self):
        if self._process is not None:
            self._process.stop()

    def is_server_running(self, timeout: int = 30) -> bool:
        """Poll the Streamlit health endpoint until it responds 'ok'."""
        with requests.Session() as http_session:
            start_time = time.time()
            while True:
                with contextlib.suppress(requests.RequestException):
                    response = http_session.get(self.server_url + "/_stcore/health")
                    if response.text == "ok":
                        return True
                time.sleep(3)
                if time.time() - start_time > timeout:
                    return False

    @property
    def server_url(self) -> str:
        if not self.server_port:
            raise RuntimeError("Unknown server port")
        return f"http://localhost:{self.server_port}"
