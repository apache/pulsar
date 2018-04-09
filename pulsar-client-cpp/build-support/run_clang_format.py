#!/usr/bin/env python
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

# Original: https://github.com/apache/arrow/blob/4dbce607d50031a405af39d36e08cd03c5ffc764/cpp/build-support/run_clang_format.py
# ChangeLog:
#     2018-01-08: Accept multiple source directories (@Licht-T)

import fnmatch
import os
import subprocess
import sys

if len(sys.argv) < 5:
    sys.stderr.write("Usage: %s $CLANG_FORMAT $CHECK_FORMAT exclude_globs.txt "
                     "$source_dir1 $source_dir2\n" %
                     sys.argv[0])
    sys.exit(1)

CLANG_FORMAT = sys.argv[1]
CHECK_FORMAT = int(sys.argv[2]) == 1
EXCLUDE_GLOBS_FILENAME = sys.argv[3]
SOURCE_DIRS = sys.argv[4:]

exclude_globs = [line.strip() for line in open(EXCLUDE_GLOBS_FILENAME, "r")]

files_to_format = []
matches = []
for source_dir in SOURCE_DIRS:
    for directory, subdirs, files in os.walk(source_dir):
        for name in files:
            name = os.path.join(directory, name)
            if not (name.endswith('.h') or name.endswith('.cc')):
                continue

            excluded = False
            for g in exclude_globs:
                if fnmatch.fnmatch(name, g):
                    excluded = True
                    break
            if not excluded:
                files_to_format.append(name)

if CHECK_FORMAT:
    output = subprocess.check_output([CLANG_FORMAT, '-output-replacements-xml']
                                     + files_to_format,
                                     stderr=subprocess.STDOUT).decode('utf8')

    to_fix = []
    for line in output.split('\n'):
        if 'offset' in line:
            to_fix.append(line)

    if len(to_fix) > 0:
        print("clang-format checks failed, run 'make format' to fix")
        sys.exit(-1)
else:
    try:
        cmd = [CLANG_FORMAT, '-i'] + files_to_format
        subprocess.check_output(cmd, stderr=subprocess.STDOUT)
    except Exception as e:
        print(e)
        print(' '.join(cmd))
        raise