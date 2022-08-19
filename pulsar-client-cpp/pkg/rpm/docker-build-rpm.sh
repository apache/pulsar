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

ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/../../.. &> /dev/null && pwd )"
IMAGE_NAME=apachepulsar/pulsar-build:centos-7-2.11

if [[ -z $BUILD_IMAGE ]]; then
    # pull the image from DockerHub by default
    docker pull $IMAGE_NAME
else
    docker build --platform linux/amd64 -t $IMAGE_NAME $ROOT_DIR/pulsar-client-cpp/pkg/rpm
fi

docker run --platform linux/amd64 -v $ROOT_DIR:/pulsar $IMAGE_NAME \
        /pulsar/pulsar-client-cpp/pkg/rpm/build-rpm.sh
