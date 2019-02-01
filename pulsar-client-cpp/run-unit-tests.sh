#!/bin/bash
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

set -e

ROOT_DIR=$(git rev-parse --show-toplevel)
cd $ROOT_DIR/pulsar-client-cpp

./pulsar-test-service-start.sh
pushd tests

if [ -f /gtest-parallel/gtest-parallel ]; then
    echo "---- Run unit tests in parallel"
    tests=""
    if [ $# -eq 1 ]; then
        tests="--gtest_filter=$1"
        echo "Running tests: $1"
    fi
    /gtest-parallel/gtest-parallel ./main $tests --workers=10
    RES=$?
else
    ./main
    RES=$?
fi

popd

if [ $RES -eq 0 ]; then
    pushd python
    echo "---- Build Python Wheel file"
    python setup.py bdist_wheel

    echo "---- Installing  Python Wheel file"
    pip install dist/pulsar_client-*-linux_x86_64.whl

    echo "---- Running Python unit tests"

    # Running tests from a different directory to avoid importing directly
    # from the current dir, but rather using the installed wheel file
    cp *_test.py /tmp
    pushd /tmp

    python pulsar_test.py
    RES=$?

    echo "---- Running Python Function Instance unit tests"
    bash /pulsar/pulsar-functions/instance/src/scripts/run_python_instance_tests.sh
    RES=$?

    popd
    popd
fi

./pulsar-test-service-stop.sh

exit $RES
