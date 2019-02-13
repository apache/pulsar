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

import xml.etree.ElementTree as ET
from os.path import dirname, realpath, join

# Derive the POM path from the current script location
TOP_LEVEL_PATH = dirname(dirname(realpath(__file__)))
POM_PATH = join(TOP_LEVEL_PATH, 'pom.xml')

root = ET.XML(open(POM_PATH).read())
print(root.find('{http://maven.apache.org/POM/4.0.0}version').text)
