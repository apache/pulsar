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

# Build Pulsar C++ client in CentOS 7 container

set -e

ROOT_DIR=$(git rev-parse --show-toplevel)
cd $ROOT_DIR/pulsar-client-cpp

IMAGE="${BUILD_IMAGE_NAME:-apachepulsar/pulsar-cpp-build-centos7}"
cd ./docker/centos-7
docker build -t "${IMAGE}" .
cd -

VOLUME_OPTION=${VOLUME_OPTION:-"-v $ROOT_DIR:/pulsar"}
COMMAND="cd /pulsar/pulsar-client-cpp && mkdir -p _builds && cd _builds &&
 /opt/cmake/cmake-3.4.0-Linux-x86_64/bin/cmake .. -DBUILD_PYTHON_WRAPPER=OFF -DBUILD_TESTS=OFF && make"

DOCKER_CMD="docker run -i ${VOLUME_OPTION} ${IMAGE}"

rm -rf _builds
$DOCKER_CMD bash -c "${COMMAND}"
