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

"""Exercise the collector shared by unit, integration and system test jobs."""

import io
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest


COLLECTOR = Path(__file__).resolve().parents[1] / "pulsar_ci_tool.sh"


class NettyLeakReportsTest(unittest.TestCase):
    def test_host_and_container_reports(self):
        for mode in ("report", "fail_on_leak"):
            for source in ("none", "host", "container", "both"):
                with self.subTest(mode=mode, source=source), tempfile.TemporaryDirectory() as directory:
                    work = Path(directory)
                    (work / "pulsar-build").mkdir()
                    dumps = work / "dumps"
                    if source in ("host", "both"):
                        dumps.mkdir()
                        (dumps / "netty_leak_host.txt").write_text("host-jvm-leak\n")
                    if source in ("container", "both"):
                        logs = work / "tests/integration/build/container-logs/probe"
                        logs.mkdir(parents=True)
                        with tarfile.open(logs / "var-log-pulsar.tar.gz", "w:gz") as archive:
                            content = b"container-jvm-leak\n"
                            entry = tarfile.TarInfo("pulsar/netty_leak_container.txt")
                            entry.size = len(content)
                            archive.addfile(entry, io.BytesIO(content))
                    env = dict(os.environ, NETTY_LEAK_DETECTION=mode, NETTY_LEAK_DUMP_DIR=str(dumps))
                    result = subprocess.run(
                        ["bash", str(COLLECTOR), "report_netty_leaks"],
                        cwd=work, env=env, capture_output=True, text=True, timeout=30,
                    )
                    expected_exit = int(mode == "fail_on_leak" and source != "none")
                    self.assertEqual(result.returncode, expected_exit, result.stdout + result.stderr)
                    marker = "netty_leaks_not_found" if source == "none" else "netty_leaks_found"
                    self.assertTrue((work / "pulsar-build" / marker).exists())
                    if source == "none":
                        self.assertIn("No netty leaks found.", result.stdout)
                        continue
                    report = (dumps / "leak_report.txt").read_text()
                    annotation = "::error::" if mode == "fail_on_leak" else "::warning::"
                    self.assertIn(annotation + "Netty leaks found.", report)
                    if source in ("host", "both"):
                        self.assertIn("host-jvm-leak", report)
                    if source in ("container", "both"):
                        self.assertIn("container-jvm-leak", report)
                        self.assertTrue((dumps / "container-logs/probe/netty_leak_container.txt").exists())


if __name__ == "__main__":
    unittest.main()
