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

"""Tests of report_retried_tests.py, on reports in the form Gradle writes for TestNG.

Run: python3 -m unittest discover -s pulsar-build -p 'test_report_retried_tests.py'
"""

import io
import os
import tempfile
import unittest

import report_retried_tests

CLS = "org.apache.pulsar.broker.FooTest"


def failed(name, message="java.lang.AssertionError: expected [1] but found [2]", kind="java.lang.AssertionError",
           seconds="1.5", cls=CLS):
    """A retried attempt: skipped, with the attempt's failure."""
    return (f'<testcase name="{name}" classname="{cls}" time="{seconds}">'
            f'<skipped message="{message}" type="{kind}">{kind}</skipped></testcase>')


def passed(name, cls=CLS):
    return f'<testcase name="{name}" classname="{cls}" time="0.4"/>'


def failure(name, cls=CLS):
    return (f'<testcase name="{name}" classname="{cls}" time="0.5">'
            '<failure message="java.lang.AssertionError: again" type="java.lang.AssertionError">x</failure></testcase>')


def skip(name, message="org.testng.SkipException: not here", kind="org.testng.SkipException", cls=CLS):
    return (f'<testcase name="{name}" classname="{cls}" time="0.0">'
            f'<skipped message="{message}" type="{kind}">{kind}</skipped></testcase>')


FAIL_FAST_SKIP = dict(message="Skipped after failure since testFailFast system property is set.",
                      kind="org.apache.pulsar.tests.FailFastNotifier$FailFastSkipException")


class ReportRetriedTestsTest(unittest.TestCase):

    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.summary = os.path.join(self.dir.name, "summary.md")

    def run_report(self, *testcases, cls=CLS):
        with open(os.path.join(self.dir.name, f"TEST-{cls}.xml"), "w") as f:
            f.write(f'<?xml version="1.0" encoding="UTF-8"?>\n<testsuite name="{cls}" tests="{len(testcases)}">'
                    + "".join(testcases) + "<system-out><![CDATA[]]></system-out></testsuite>")
        out = io.StringIO()
        report_retried_tests.report(self.dir.name, self.summary, out)
        tsv = os.path.join(self.dir.name, "retried-tests.tsv")
        rows = []
        if os.path.exists(tsv):
            with open(tsv) as f:
                rows = [line.rstrip("\n").split("\t") for line in f][1:]
        return out.getvalue(), rows

    def test_retry_that_passes(self):
        out, rows = self.run_report(failed("testA"), passed("testA"))
        self.assertEqual(rows, [[CLS, "testA", "passed", "1.500",
                                 "java.lang.AssertionError: expected [1] but found [2]"]])
        self.assertIn(f"::warning title=Retried test::{CLS}.testA passed only on its retry.", out)

    def test_retry_that_fails_again(self):
        out, rows = self.run_report(failed("testA"), failure("testA"))
        self.assertEqual([row[2] for row in rows], ["failed again"])
        self.assertNotIn("::warning", out)
        self.assertIn(f"Retried test {CLS}.testA: failed again.", out)

    def test_data_provider_row_with_a_new_index(self):
        _, rows = self.run_report(passed("testRow[0](false)"), failed("testRow[1](true)"), passed("testRow[3](true)"),
                                  passed("testRow[4](false)"))
        self.assertEqual([row[1:3] for row in rows], [["testRow[1](true)", "passed"]])

    def test_retry_with_arguments_rendered_differently(self):
        # a mutable data provider argument is rendered again when the retry starts
        _, rows = self.run_report(failed("testWithTimer[0]({})"), failure("testWithTimer[1]({1=task})"))
        self.assertEqual([row[1:3] for row in rows], [["testWithTimer[0]({})", "failed again"]])

    def test_retry_that_is_skipped(self):
        out, rows = self.run_report(failed("testA"), skip("testA", **FAIL_FAST_SKIP))
        self.assertEqual([row[2] for row in rows], ["skipped"])
        self.assertIn("testA failed its first attempt and its retry was skipped.", out)

    def test_later_invocation_does_not_answer_for_a_skipped_retry(self):
        # invocationCount: the retry of the first invocation was skipped, the next invocation passed
        _, rows = self.run_report(failed("testA"), skip("testA"), passed("testA"))
        self.assertEqual([row[2] for row in rows], ["skipped"])

    def test_failed_attempt_without_a_retry_in_the_report(self):
        out, rows = self.run_report(passed("testB"), failed("testA"))
        self.assertEqual([row[1:3] for row in rows], [["testA", "unknown"]])
        self.assertIn("testA failed its first attempt and the report has no retry.", out)

    def test_retry_after_two_failed_attempts(self):
        # with testRetryCount=2
        _, rows = self.run_report(failed("testA"), failed("testA"), passed("testA"))
        self.assertEqual([row[2] for row in rows], ["passed", "passed"])

    def test_skips_that_are_not_retried(self):
        out, rows = self.run_report(skip("testA"), skip("testB", **FAIL_FAST_SKIP),
                                    f'<testcase name="testC" classname="{CLS}" time="0.0"><skipped/></testcase>',
                                    passed("testD"))
        self.assertEqual(rows, [])
        self.assertEqual(out, "No retried tests.\n")
        self.assertFalse(os.path.exists(os.path.join(self.dir.name, "retried-tests.tsv")))

    def test_retry_fixtures_are_left_out(self):
        cls = "org.apache.pulsar.tests.RetryAnalyzerTest"
        _, rows = self.run_report(failed("testMethodA", cls=cls), passed("testMethodA", cls=cls), cls=cls)
        self.assertEqual(rows, [])

    def test_other_buildtools_tests_are_reported(self):
        cls = "org.apache.pulsar.tests.ThreadDumpUtilTest"
        _, rows = self.run_report(failed("testHelp", cls=cls), passed("testHelp", cls=cls), cls=cls)
        self.assertEqual([row[1:3] for row in rows], [["testHelp", "passed"]])

    def test_line_breaks_and_tabs_in_names_and_messages(self):
        name = "m[1](a&#10;::error::injected&#9;b|c)"
        message = "java.lang.AssertionError: first&#10;::error::second&#9;column|pipe"
        out, rows = self.run_report(failed(name, message=message), failure("m[3](a&#10;::error::injected&#9;b|c)"),
                                    failed("n", message=message), passed("n"))
        self.assertFalse([line for line in out.splitlines() if line.startswith("::error")])
        self.assertEqual(len(out.splitlines()), 2)
        self.assertEqual([len(row) for row in rows], [5, 5])
        self.assertEqual(rows[0][1], "m[1](a ::error::injected b|c)")
        with open(self.summary) as f:
            table = [line for line in f if line.startswith("| FooTest.")]
        self.assertEqual(len(table), 2)
        self.assertIn(r"| FooTest.m[1](a ::error::injected b\|c) | failed again |", table[0])
        self.assertIn(r"first ::error::second column\|pipe", table[1])


if __name__ == "__main__":
    unittest.main()
