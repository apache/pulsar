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

ROOT_DIR=$(git rev-parse --show-toplevel)
GOIMAGE=golang:1.11.4

docker pull $GOIMAGE

DOCKER_CMD="docker run -i -v $ROOT_DIR:/pulsar $GOIMAGE"

$DOCKER_CMD sh -c "cd /pulsar/tests/docker-images/latest-version-image/target/ && mkdir cpp-dep-build && cp -r /pulsar/pulsar-client-cpp/pkg/deb/BUILD/DEB/* /pulsar/tests/docker-images/latest-version-image/target/cpp-dep-build && cd /pulsar/tests/docker-images/latest-version-image/target/cpp-dep-build && apt install ./apache-pulsar-client.deb && apt install ./apache-pulsar-client-dev.deb && cd /pulsar/tests/docker-images/latest-version-image/go-test-schema && ./build_go_schema_test.sh && cp /go/bin/consumer-schema . && cp /go/bin/producer-schema ."
