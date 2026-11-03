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

"""Reports the test methods that failed their first attempt and were retried (see RetryAnalyzer).

Usage: report_retried_tests.py <directory with TEST-*.xml reports>

Gradle's JUnit XML report lists a retried attempt as a skipped testcase that keeps the failure, followed by
the retry. A retried data provider row gets a new index, so rows are matched by method and arguments. The job
log shows no reason for a retry (unit test jobs don't show the retry at all), and green jobs upload no test
reports, so this prints one warning annotation per test that passed only on its retry, adds a table of all
retried tests to the job summary, and writes them to retried-tests.tsv in the report directory.
"""

import glob
import os
import re
import sys
import xml.etree.ElementTree as ET

DATA_PROVIDER_INDEX = re.compile(r"\[\d+\]")
# buildtools' own tests of the retry support (RetryAnalyzerTest, DefaultTestRetriesTest, ...) fail on purpose
RETRY_SUPPORT_TESTS = re.compile(r"^org\.apache\.pulsar\.tests\.[A-Z]")


def retried_tests(report):
    pending = {}
    for _, element in ET.iterparse(report):
        if element.tag != "testcase" or RETRY_SUPPORT_TESTS.match(element.get("classname") or ""):
            continue
        name = element.get("name")
        key = (element.get("classname"), DATA_PROVIDER_INDEX.sub("", name, count=1))
        skipped = element.find("skipped")
        if skipped is not None:
            message = skipped.get("message") or skipped.get("type") or ""
            # a SkipException, including the fail fast one, is never retried
            if not (skipped.get("type") or "").endswith("SkipException") and "Skipped after failure" not in message:
                pending.setdefault(key, []).append((message, float(element.get("time") or 0)))
        elif pending.get(key):
            message, seconds = pending[key].pop(0)
            failed = element.find("failure") is not None or element.find("error") is not None
            yield key[0], name, "failed again" if failed else "passed", seconds, message
        element.clear()


def escape(value):
    return value.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def main():
    report_dir = sys.argv[1]
    found = []
    for report in sorted(glob.glob(os.path.join(report_dir, "TEST-*.xml"))):
        try:
            found.extend(retried_tests(report))
        except ET.ParseError as e:
            print(f"Skipping {report}: {e}")
    if not found:
        print("No retried tests.")
        return
    with open(os.path.join(report_dir, "retried-tests.tsv"), "w") as f:
        f.write("class\tmethod\tretry\tfirst_attempt_seconds\tfirst_attempt_failure\n")
        for cls, name, outcome, seconds, message in found:
            first_line = message.splitlines()[0] if message else ""
            f.write(f"{cls}\t{name}\t{outcome}\t{seconds:.3f}\t{first_line}\n")
            if outcome == "passed":
                print(f"::warning title=Retried test::{escape(cls)}.{escape(name)} passed only on its retry."
                      f" First attempt ({seconds:.1f} s): {escape(first_line[:500])}")
            else:
                print(f"Retried test {cls}.{name}: {outcome}. First attempt ({seconds:.1f} s): {first_line[:500]}")
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a") as f:
            f.write(f"### Retried tests ({len(found)})\n\n"
                    "| Test | Retry | First attempt | First attempt failure |\n|---|---|---|---|\n")
            for cls, name, outcome, seconds, message in found:
                first_line = (message.splitlines()[0] if message else "").replace("|", "\\|")[:300]
                f.write(f"| {cls.split('.')[-1]}.{name} | {outcome} | {seconds:.1f} s | {first_line} |\n")
            f.write("\n")


if __name__ == "__main__":
    main()
