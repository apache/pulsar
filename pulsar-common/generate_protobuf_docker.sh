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

# Fail script in case of errors
set -e

ROOT_DIR=$(git rev-parse --show-toplevel)
COMMON_DIR=$ROOT_DIR/pulsar-common
cd $COMMON_DIR

BUILD_IMAGE_NAME="${BUILD_IMAGE_NAME:-apachepulsar/pulsar-build}"
BUILD_IMAGE_VERSION="${BUILD_IMAGE_VERSION:-ubuntu-16.04}"

IMAGE="$BUILD_IMAGE_NAME:$BUILD_IMAGE_VERSION"

echo $IMAGE

# Force to pull image in case it was updated
docker pull $IMAGE

WORKDIR=/workdir
docker run -i \
    -v ${COMMON_DIR}:${WORKDIR} $IMAGE \
    bash -c "cd ${WORKDIR}; PROTOC=/pulsar/protobuf/src/protoc ./generate_protobuf.sh"

