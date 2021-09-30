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

# Run C++ unit tests within a Docker container

# Fail script in case of errors
set -e

if [ "$1" = "--help" ]; then
    echo "Usage:"
    echo "--tests=\"<test-regex>\" (eg: --test=\"BasicEndToEndTest.*\")"
    exit 0
fi


ROOT_DIR=$(git rev-parse --show-toplevel)
cd $ROOT_DIR/pulsar-client-cpp

BUILD_IMAGE_NAME="${BUILD_IMAGE_NAME:-apachepulsar/pulsar-build}"
BUILD_IMAGE_VERSION="${BUILD_IMAGE_VERSION:-ubuntu-16.04-pb3}"

IMAGE="$BUILD_IMAGE_NAME:$BUILD_IMAGE_VERSION"

echo "---- Testing Pulsar C++ client using image $IMAGE (type --help for more options)"

docker pull $IMAGE

CONTAINER_LABEL="pulsartests=$$"
export GTEST_COLOR=${GTEST_COLOR:-no}
DOCKER_CMD="docker run -e GTEST_COLOR -i -l $CONTAINER_LABEL -v $ROOT_DIR:/pulsar $IMAGE"


for args in "$@"
do
    arg=$(echo $args | cut -f1 -d=)
    val=$(echo $args | cut -f2 -d=)

    case "$arg" in
            --tests)   tests=${val} ;;
            *)
    esac
done

# Start 2 Pulsar standalone instances (one with TLS and one without)
# and execute the tests
set +e
DISABLE_COLOR_OUTPUT=""
if [ "$GTEST_COLOR" = "no" ]; then
  DISABLE_COLOR_OUTPUT="| cat"
fi
$DOCKER_CMD bash -c "set -o pipefail; cd /pulsar/pulsar-client-cpp && ./run-unit-tests.sh ${tests} $DISABLE_COLOR_OUTPUT"
RES=$?
if [ $RES -ne 0 ]; then
  (
  cd "$ROOT_DIR"
  mkdir -p test-logs
  cd test-logs
  container_id=$(docker ps -a -q --filter "label=$CONTAINER_LABEL")
  if [ -n "$container_id" ]; then
    # copy logs from the container that ran the tests
    docker commit $container_id pulsartests/$container_id
    docker run -i --rm pulsartests/$container_id \
      bash -c "cd /tmp; tar zcf - gtest-parallel-logs gtest_parallel_results.json pulsar-test-dist/logs" \
      | tar zxvf -
  fi
  )
fi
exit $RES
