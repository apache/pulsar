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

"""Bounded allocation on exactly the test-owned ledger tmpfs, never the host disk."""
import errno
import os
from pathlib import Path

ROOT = Path("/storage/ledgers")
FILL = ROOT / "fill"
MIB = 1024 * 1024

mounts = [line.split() for line in Path("/proc/mounts").read_text().splitlines()]
assert any(fields[1:3] == [str(ROOT), "tmpfs"] for fields in mounts), "Ledger mount must be tmpfs"
assert ROOT.resolve() == ROOT and FILL.resolve() == FILL, "Refusing redirected storage path"
stat = os.statvfs(ROOT)
assert 0 < stat.f_frsize * stat.f_blocks <= 128 * MIB, "Refusing unbounded storage mount"
written = 0
fd = os.open(FILL, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
try:
    block = bytes(MIB)
    for _ in range(129):
        written += os.write(fd, block)
    raise AssertionError("Bounded ledger allocation did not encounter ENOSPC")
except OSError as error:
    assert error.errno == errno.ENOSPC, f"Unexpected allocation error: {error!r}"
    # Record the numeric errno, not libc's potentially localized strerror text.
    Path("/storage/control/full-error").write_text(f"ENOSPC errno={error.errno} bytes={written}\n")
finally:
    os.close(fd)
