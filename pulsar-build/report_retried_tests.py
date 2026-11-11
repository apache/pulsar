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

Gradle's JUnit XML report lists a retried attempt as a skipped testcase that keeps the failure. TestNG runs the
retry right after it, so the next testcase of the same method in the report is the retry. Its name can differ: a
retried data provider row gets a new index, and Gradle renders the arguments again for each attempt. The job log
shows no reason for a retry (unit test jobs don't show the retry at all), and green jobs upload no test reports, so
this prints a warning annotation for each first attempt that failed without its retry failing, adds a table of all
retried tests to the job summary, and writes them to retried-tests.tsv in the report directory.
"""

import glob
import os
import re
import sys
import xml.etree.ElementTree as ET

# "method[index](arguments)", the name Gradle gives a data provider row
DATA_PROVIDER_ROW = re.compile(r"^(.*?)\[\d+\]\(.*\)$", re.DOTALL)
# buildtools' tests of the retry support, which fail their first attempt on purpose
RETRY_FIXTURES = frozenset({
    "org.apache.pulsar.tests.DefaultTestRetriesTest",
    "org.apache.pulsar.tests.RetryAnalyzerTest",
    "org.apache.pulsar.tests.TestRetrySupportBeforeMethodRetryTest",
    "org.apache.pulsar.tests.TestRetrySupportRetryTest",
})


def method_of(name):
    match = DATA_PROVIDER_ROW.match(name)
    return match.group(1) if match else name


def failed_attempt(skipped):
    """A retried attempt keeps its failure. A SkipException, the fail fast skip and a skip without a cause (a
    failed dependency) are never retried."""
    kind = skipped.get("type") or ""
    message = skipped.get("message") or ""
    return bool(kind) and not kind.endswith("SkipException") and "Skipped after failure" not in message


def retried_tests(report):
    """Yields (class, method, retry outcome, first attempt seconds, first attempt failure) per failed attempt.

    The outcome is that of the method's next testcase: "passed", "failed again" or "skipped" (the retry was skipped,
    e.g. by fail fast), and "unknown" when the report has no later testcase of the method. Test methods that run in
    parallel invocations can interleave in the report, so their attempts may be paired with another invocation.
    """
    pending = {}
    for _, element in ET.iterparse(report):
        if element.tag != "testcase":
            continue
        cls = element.get("classname") or ""
        name = element.get("name") or ""
        if cls not in RETRY_FIXTURES:
            key = (cls, method_of(name))
            skipped = element.find("skipped")
            if skipped is not None and failed_attempt(skipped):
                # with more than one retry, a retried attempt can follow another one
                pending.setdefault(key, []).append(
                    (name, float(element.get("time") or 0), skipped.get("message") or skipped.get("type")))
            elif key in pending:
                failed = element.find("failure") is not None or element.find("error") is not None
                outcome = "skipped" if skipped is not None else "failed again" if failed else "passed"
                for first_name, seconds, message in pending.pop(key):
                    yield cls, first_name, outcome, seconds, message
        element.clear()
    for (cls, _), attempts in pending.items():
        for first_name, seconds, message in attempts:
            yield cls, first_name, "unknown", seconds, message


def one_line(value):
    """Tabs and line breaks in a test name or message would split a TSV row, a table row or a log line, and a log
    line that starts with "::" is a workflow command."""
    return re.sub(r"[\t\r\n]+", " ", value)


def escape(value):
    """For the message of a workflow command."""
    return one_line(value).replace("%", "%25")


def table_cell(value):
    return one_line(value).replace("|", "\\|")


OUTCOME_TEXT = {
    "passed": "passed only on its retry",
    "skipped": "failed its first attempt and its retry was skipped",
    "unknown": "failed its first attempt and the report has no retry",
}


def report(report_dir, summary_path, out=sys.stdout):
    found = []
    for path in sorted(glob.glob(os.path.join(report_dir, "TEST-*.xml"))):
        try:
            found.extend(retried_tests(path))
        except ET.ParseError as e:
            print(f"Skipping {one_line(path)}: {e}", file=out)
    if not found:
        print("No retried tests.", file=out)
        return
    with open(os.path.join(report_dir, "retried-tests.tsv"), "w") as f:
        f.write("class\tmethod\tretry\tfirst_attempt_seconds\tfirst_attempt_failure\n")
        for cls, name, outcome, seconds, message in found:
            f.write(f"{one_line(cls)}\t{one_line(name)}\t{outcome}\t{seconds:.3f}\t{one_line(message)[:2000]}\n")
            if outcome in OUTCOME_TEXT:
                print(f"::warning title=Retried test::{escape(cls)}.{escape(name)} {OUTCOME_TEXT[outcome]}."
                      f" First attempt ({seconds:.1f} s): {escape(message[:500])}", file=out)
            else:
                print(f"Retried test {one_line(cls)}.{one_line(name)}: {outcome}. First attempt ({seconds:.1f} s):"
                      f" {one_line(message)[:500]}", file=out)
    if summary_path:
        with open(summary_path, "a") as f:
            f.write(f"### Retried tests ({len(found)})\n\n"
                    "| Test | Retry | First attempt | First attempt failure |\n|---|---|---|---|\n")
            for cls, name, outcome, seconds, message in found:
                test = table_cell(f"{cls.split('.')[-1]}.{name}")
                f.write(f"| {test} | {outcome} | {seconds:.1f} s | {table_cell(message[:300])} |\n")
            f.write("\n")


def main():
    report(sys.argv[1], os.environ.get("GITHUB_STEP_SUMMARY"))


if __name__ == "__main__":
    main()
