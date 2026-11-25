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

"""Tests of record_test_durations in pulsar_ci_tool.sh, on reports in the form Gradle writes for TestNG.

Run: python3 -m unittest discover -s pulsar-build -p 'test_record_test_durations.py'
"""

import os
import subprocess
import tempfile
import unittest

CI_TOOL = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pulsar_ci_tool.sh")


class RecordTestDurationsTest(unittest.TestCase):

    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)

    def record(self, *classes):
        """Writes a testShard1 report for each (class, timestamp, time) and returns the recorded durations."""
        results_dir = os.path.join(self.dir.name, "pulsar-broker", "build", "test-results", "testShard1")
        os.makedirs(results_dir)
        for cls, timestamp, seconds in classes:
            with open(os.path.join(results_dir, f"TEST-{cls}.xml"), "w") as f:
                f.write('<?xml version="1.0" encoding="UTF-8"?>\n'
                        f'<testsuite name="{cls}" tests="1" skipped="0" failures="0" errors="0" '
                        f'timestamp="{timestamp}" hostname="runner" time="{seconds}">\n'
                        f'  <testcase name="testA" classname="{cls}" time="{seconds}"/>\n'
                        '</testsuite>\n')
        durations_file = os.path.join(self.dir.name, "test-durations", "GROUP.txt")
        env = {name: value for name, value in os.environ.items() if name != "GITHUB_WORKSPACE"}
        subprocess.run(["bash", CI_TOOL, "record_test_durations", durations_file], cwd=self.dir.name, env=env,
                       check=True, capture_output=True)
        with open(durations_file) as f:
            return {cls: float(seconds) for seconds, cls in (line.split() for line in f)}

    def test_class_that_starts_on_a_whole_second(self):
        # Gradle writes no fraction for a whole second, so as strings "06:15:23Z" sorts after "06:15:23.432Z"
        durations = self.record(("org.apache.pulsar.broker.ATest", "2026-11-08T06:15:23Z", "0.4"),
                                ("org.apache.pulsar.broker.BTest", "2026-11-08T06:15:23.432Z", "20.1"),
                                ("org.apache.pulsar.broker.CTest", "2026-11-08T06:15:44.100Z", "3.5"))
        self.assertEqual(durations, {"org.apache.pulsar.broker.ATest": 0.432,
                                     "org.apache.pulsar.broker.BTest": 20.668,
                                     "org.apache.pulsar.broker.CTest": 3.5})

    def test_shard_that_runs_past_midnight(self):
        durations = self.record(("org.apache.pulsar.broker.ATest", "2026-11-30T23:59:58.500Z", "1.2"),
                                ("org.apache.pulsar.broker.BTest", "2026-12-01T00:00:03Z", "2.0"))
        self.assertEqual(durations, {"org.apache.pulsar.broker.ATest": 4.5,
                                     "org.apache.pulsar.broker.BTest": 2.0})


if __name__ == "__main__":
    unittest.main()
