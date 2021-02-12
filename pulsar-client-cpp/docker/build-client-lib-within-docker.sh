#!/usr/bin/env bash
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


set -e -x

cd /pulsar/pulsar-client-cpp

find . -name CMakeCache.txt | xargs -r rm
find . -name CMakeFiles | xargs -r rm -rf
rm -f lib/*.pb.*

cmake . -DBUILD_TESTS=OFF -DLINK_STATIC=ON \
        -DPYTHON_INCLUDE_DIR=/opt/python/$PYTHON_SPEC/include/python$PYTHON_VERSION \
        -DPYTHON_LIBRARY=/opt/python/$PYTHON_SPEC/lib \

make pulsarShared pulsarStatic -j4
