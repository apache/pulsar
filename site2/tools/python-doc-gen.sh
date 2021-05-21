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

set -xe

ROOT_DIR=$(git rev-parse --show-toplevel)
VERSION=`${ROOT_DIR}/src/get-project-version.py`

# Make sure the Python client lib is installed
# so that Pdoc can import the module
find $ROOT_DIR -name CMakeCache.txt | xargs rm -f
find $ROOT_DIR -name CMakeFiles | xargs rm -rf
find $ROOT_DIR -name PulsarApi.pb.h | xargs rm -rf
find $ROOT_DIR -name PulsarApi.pb.cc | xargs rm -rf
cd $ROOT_DIR/pulsar-client-cpp
export CMAKE_ARGS="-DCMAKE_BUILD_TYPE=Debug -DBUILD_DYNAMIC_LIB=OFF -DPYTHON_INCLUDE_DIR=/usr/include/python2.7 -DPYTHON_LIBRARY=/usr/lib/x86_64-linux-gnu/libpython2.7.so"
cmake . $CMAKE_ARGS
make -j8 _pulsar
pip install enum34
pip install six
pip install fastavro
pip install certifi
pip install pdoc

# Create documentation
DESTINATION=$ROOT_DIR/generated-site/api/python/${VERSION}
rm -fr $DESTINATION
mkdir -p $DESTINATION
PYTHONPATH=$ROOT_DIR/pulsar-client-cpp/python pdoc pulsar \
  --html \
  --html-dir $DESTINATION
mv -f $DESTINATION/pulsar/* $DESTINATION/
rmdir $DESTINATION/pulsar

# Update /api/python to the latest version
rm -fr $ROOT_DIR/generated-site/api/python/{index.html,functions,pulsar}
cp -r $DESTINATION/* $ROOT_DIR/generated-site/api/python/
