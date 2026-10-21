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

"""Check kernel-visible errno and an already-open journal FD before relying on fault tests."""
import errno
import os
import threading
import time
from pathlib import Path

CONTROL = Path("/storage/control")


def mode(value):
    (CONTROL / "mode.next").write_text(value + "\n")
    os.replace(CONTROL / "mode.next", CONTROL / "mode")


def expect_errno(operation, expected):
    try:
        operation()
    except OSError as error:
        assert error.errno == expected, (error, expected)
    else:
        raise AssertionError(f"Expected errno {expected}")


fd = os.open("/storage/journal/probe.txn", os.O_CREAT | os.O_RDWR, 0o600)
try:
    os.write(fd, b"before\n")
    os.fsync(fd)
    for value, expected in (("readonly", errno.EROFS), ("eio", errno.EIO)):
        mode(value)
        expect_errno(lambda: os.write(fd, b"fault\n"), expected)
        expect_errno(lambda: os.fsync(fd), expected)
    # EIO is journal-only; a disk-check-style probe must not exercise the fault.
    Path("/storage/journal/nonjournal-probe").write_text("healthy")
    mode("healthy")
    os.write(fd, b"recovered\n")
    os.fsync(fd)

    for operation in (lambda: os.write(fd, b"released\n"), lambda: os.fsync(fd)):
        previous = len((CONTROL / "events").read_text().splitlines())
        mode("stall")
        completed = threading.Event()
        failures = []

        def blocked():
            try:
                operation()
            except BaseException as error:
                failures.append(error)
            finally:
                completed.set()

        worker = threading.Thread(target=blocked, daemon=True)
        worker.start()
        try:
            deadline = time.monotonic() + 5
            while len((CONTROL / "events").read_text().splitlines()) == previous:
                assert time.monotonic() < deadline, "No intercepted journal operation"
                time.sleep(0.01)
            assert not completed.wait(0.2), "Journal operation did not stall"
        finally:
            mode("healthy")
            worker.join(5)
        assert completed.is_set(), "Healthy control did not release the journal"
        assert not failures, failures
    os.lseek(fd, 0, os.SEEK_SET)
    assert os.read(fd, 1024) == b"before\nrecovered\nreleased\n"
    assert "EXPIRED" not in (CONTROL / "events").read_text()
    print("PASS: open-FD EROFS/EIO, journal-only faults, bounded write/fsync stall and recovery")
finally:
    mode("healthy")
    os.close(fd)
