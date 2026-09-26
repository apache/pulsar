#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

"""Serves the performance reports over HTTP, with the text files a run writes shown as text in the browser.

Python's http.server guesses types from file extensions: it offers YAML, CSV, HDR histogram logs (.hdr, which it
takes for an image format), collapsed stacks and logs as downloads. This server shows them as plain text; JFR
recordings and capture streams stay downloads.

It binds to the loopback interface by default, so that it is reachable only from the machine itself or through an
SSH tunnel (ssh -N -L 8000:127.0.0.1:8000 perf-host).
"""

import argparse
import functools
import http.server
from pathlib import Path

TEXT = "text/plain; charset=utf-8"
# The same extensions as ReportsServer.TEXT_EXTENSIONS, the server of the report tool's serveReports Gradle task
TEXT_EXTENSIONS = [".yaml", ".yml", ".csv", ".hdr", ".hgrm", ".collapsed", ".log", ".md", ".txt"]


class ReportRequestHandler(http.server.SimpleHTTPRequestHandler):
    extensions_map = {
        **http.server.SimpleHTTPRequestHandler.extensions_map,
        **{extension: TEXT for extension in TEXT_EXTENSIONS},
    }


def default_reports_root():
    """performance.reportsDir from ~/.gradle/gradle.properties, as the launcher's Gradle tasks read it, else the
    repository's build/performance."""
    repository = Path(__file__).resolve().parents[2]
    gradle_properties = Path.home() / ".gradle" / "gradle.properties"
    if gradle_properties.is_file():
        for line in gradle_properties.read_text(encoding="utf-8").splitlines():
            key, separator, value = line.partition("=")
            if separator and key.strip() == "performance.reportsDir":
                return repository / value.strip()
    return repository / "build" / "performance"


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("directory", nargs="?", type=Path, default=None,
                        help="the reports root; default: performance.reportsDir in ~/.gradle/gradle.properties, "
                             "else build/performance in the repository")
    parser.add_argument("--bind", default="127.0.0.1", help="address to bind to (default: %(default)s)")
    parser.add_argument("--port", type=int, default=8000, help="port to listen on (default: %(default)s)")
    arguments = parser.parse_args()
    directory = (arguments.directory or default_reports_root()).resolve()
    if not directory.is_dir():
        parser.error(f"{directory} is not a directory")
    handler = functools.partial(ReportRequestHandler, directory=str(directory))
    with http.server.ThreadingHTTPServer((arguments.bind, arguments.port), handler) as server:
        print(f"Serving {directory} at http://{arguments.bind}:{arguments.port}/", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
