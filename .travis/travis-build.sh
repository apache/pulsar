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

if [ "X$1" = "Xjava8" ]; then
    echo "Running Java build"
    mvn -Dstyle.color=always license:check test package

    echo "Checking license on distribution files"
    src/check-binary-license ./distribution/server/target/apache-pulsar-*-bin.tar.gz

elif [ "X$1" = "Xintegration" ]; then
    tests/scripts/pre-integ-tests.sh

    echo "Creating docker images"
    mvn install -Pdocker -DskipTests > docker-build.log

    echo "Running integration tests"
    mvn -Dstyle.color=always -B test -DintegrationTests -DredirectTestOutputToFile=false

    tests/scripts/post-integ-tests.sh

elif [ "X$1" = "Xcpp" ]; then
    echo "Running C++ / Python build"

    mvn package -DskipTests > cpp-build.log

    export CMAKE_ARGS="-DCMAKE_BUILD_TYPE=Debug -DBUILD_DYNAMIC_LIB=OFF"
    pulsar-client-cpp/docker-build.sh
    pulsar-client-cpp/docker-tests.sh

else
    echo "Invalid test suite '$1'"
    exit 1
fi
