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

# Build Pulsar Python3.9 client

set -e

ROOT_DIR=$(git rev-parse --show-toplevel)
cd $ROOT_DIR/pulsar-client-cpp


# Use manylinux2014 build image
PYTHON_VERSION="3.9"
PYTHON_SPEC="cp39-cp39"
ARCH="x86_64"
IMAGE_NAME=apachepulsar/pulsar-build
IMAGE=$IMAGE_NAME:manylinux-$PYTHON_SPEC-$ARCH

VOLUME_OPTION=${VOLUME_OPTION:-"-v $ROOT_DIR:/pulsar"}
COMMAND="/pulsar/pulsar-client-cpp/docker/build-wheel-file-within-docker.sh"
DOCKER_CMD="docker run -i ${VOLUME_OPTION} -e USE_FULL_POM_NAME -e NAME_POSTFIX -e ARCH=${ARCH} ${IMAGE}"

$DOCKER_CMD bash -c "${COMMAND}"
