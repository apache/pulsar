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


set -e

BUILD_IMAGE_NAME="${BUILD_IMAGE_NAME:-apachepulsar/pulsar-build}"

ROOT_DIR=$(git rev-parse --show-toplevel)
cd $ROOT_DIR

PYTHON_VERSIONS=(
   '3.6 cp36-cp36m'
)

for line in "${PYTHON_VERSIONS[@]}"; do
    read -r -a PY <<< "$line"
    PYTHON_VERSION=${PY[0]}
    PYTHON_SPEC=${PY[1]}
    echo "--------- Build Client library"

    IMAGE_NAME=$BUILD_IMAGE_NAME:manylinux-$PYTHON_SPEC

    echo "Using image: $IMAGE_NAME"

    VOLUME_OPTION=${VOLUME_OPTION:-"-v $ROOT_DIR:/pulsar"}
    COMMAND="/pulsar/pulsar-client-cpp/docker/build-client-lib-within-docker.sh"
    DOCKER_CMD="docker run -i ${VOLUME_OPTION} ${IMAGE_NAME}"

    $DOCKER_CMD bash -c "${COMMAND}"

done
