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

# Build Pulsar C++ client within a Docker container

# Fail script in case of errors
set -e

ROOT_DIR=$(git rev-parse --show-toplevel)
cd $ROOT_DIR/pulsar-client-cpp

BUILD_IMAGE_NAME="${BUILD_IMAGE_NAME:-apachepulsar/pulsar-build}"
BUILD_IMAGE_VERSION="${BUILD_IMAGE_VERSION:-ubuntu-16.04}"

IMAGE="$BUILD_IMAGE_NAME:$BUILD_IMAGE_VERSION"

echo "---- Build Pulsar C++ client using image $IMAGE (pass <skip-clean> for incremental build)"

docker pull $IMAGE

VOLUME_OPTION=${VOLUME_OPTION:-"-v $ROOT_DIR:/pulsar"}
PYTHON_INCLUDE_DIR="\$(python3 -c \"from distutils.sysconfig import get_python_inc; print(get_python_inc())\")"
PYTHON_LIBRARY="\$(python3 -c \"import distutils.sysconfig as sysconfig; print(sysconfig.get_config_var('LIBDIR'))\")"
CMAKE_ARGS="-DPYTHON_INCLUDE_DIR=\"$PYTHON_INCLUDE_DIR\" -DPYTHON_LIBRARY=\"$PYTHON_LIBRARY\" $CMAKE_ARGS"
COMMAND="cd /pulsar/pulsar-client-cpp \\
 && apt install -y python3-dev \\
 && cmake . $CMAKE_ARGS \\
 && make check-format \\
 && make -j8"

DOCKER_CMD="docker run -i ${VOLUME_OPTION} ${IMAGE}"

# Remove any cached CMake relate file from previous builds
if [ "$1" != "skip-clean" ]; then
	find . -name CMakeCache.txt | xargs rm -f
	find . -name CMakeFiles | xargs rm -rf
fi

$DOCKER_CMD bash -c "${COMMAND}"
