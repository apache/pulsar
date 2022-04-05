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

set -ex

cd /pulsar/pulsar-client-cpp

find . -name CMakeCache.txt | xargs -r rm
find . -name CMakeFiles | xargs -r rm -rf

cmake . -DPYTHON_INCLUDE_DIR=/opt/python/$PYTHON_SPEC/include/python$PYTHON_VERSION \
        -DPYTHON_LIBRARY=/opt/python/$PYTHON_SPEC/lib \
        -DLINK_STATIC=ON \
        -DBUILD_TESTS=OFF \
        -DBUILD_WIRESHARK=OFF

make clean
make _pulsar -j3

cd python
python setup.py bdist_wheel

# Audit wheel is required to convert a wheel that is tagged as generic
# 'linux' into a 'multilinux' wheel.
# Only wheel files tagged as multilinux can be uploaded to PyPI
# Audit wheel will make sure no external dependencies are needed for
# the shared library and that only symbols supported by most linux
# distributions are used.
auditwheel repair dist/pulsar_client*-$PYTHON_SPEC-linux_${ARCH}.whl
