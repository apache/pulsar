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

# coding: utf-8
import pathlib
import subprocess
import sys

from junitparser import JUnitXml

class_names = []

for path in pathlib.Path(sys.argv[1]).rglob('TEST-*.xml'):
    try:
        for suite in JUnitXml.fromfile(str(path)):
            if suite.result is not None and suite.result._tag != 'skipped':
            	class_names.append(suite.classname)
    except Exception as e:
        print(f'{e}: {path}', file=sys.stderr)

class_names = set(class_names)
print(",".join(class_names))
