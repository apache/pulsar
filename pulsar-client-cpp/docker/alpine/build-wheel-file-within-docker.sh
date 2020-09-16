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

ROOT_DIR=$(git rev-parse --show-toplevel)
cd $ROOT_DIR/pulsar-client-cpp
PYTHON_INCLUDE_DIR=${PYTHON_INCLUDE_DIR:-/usr/include/python3.8}
PYTHON_LIBRARY=${PYTHON_LIBRARY:-/usr/lib/python3.8}

cmake .  -DBUILD_TESTS=OFF \
          -DBUILD_PYTHON_WRAPPER=ON \
          -DCMAKE_BUILD_TYPE=Release \
          -DLINK_STATIC=ON  \
          -DPYTHON_INCLUDE_DIR=${PYTHON_INCLUDE_DIR} \
          -DPYTHON_LIBRARY=${PYTHON_LIBRARY}

make -j2 _pulsar

cd python
python3 setup.py bdist_wheel
