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

# ROOT_DIR should be an absolute path so that Docker accepts it as a valid volumes path
ROOT_DIR=`cd $(dirname $0)/../..; pwd`
cd $ROOT_DIR

PYTHON_VERSIONS=(
   '2.7  cp27-cp27mu  x86_64'
   '2.7  cp27-cp27m   x86_64'
   '3.5  cp35-cp35m   x86_64'
   '3.6  cp36-cp36m   x86_64'
   '3.7  cp37-cp37m   x86_64'
   '3.8  cp38-cp38    x86_64'
   '3.9  cp39-cp39    x86_64'
   '3.10 cp310-cp310  x86_64'
   '3.7  cp37-cp37m   aarch64'
   '3.8  cp38-cp38    aarch64'
   '3.9  cp39-cp39    aarch64'
   '3.10 cp310-cp310  aarch64'
)

function contains_build_version {
    for line in "${PYTHON_VERSIONS[@]}"; do
        read -r -a v <<< "$line"
        value="${v[0]} ${v[1]} ${v[2]}"

        if [ "${build_version}" == "${value}" ]; then
            # found
            res=1
            return
        fi
    done

    # not found
    res=0
}


if [ $# -ge 1 ]; then
    build_version=$@
    contains_build_version
    if [ $res == 1 ]; then
        PYTHON_VERSIONS=(
            "${build_version}"
        )
    else
        echo "Unknown build version : ${build_version}"
        echo "Supported python build versions are :"
        echo ${PYTHON_VERSIONS[@]}
        exit 1
    fi
fi

for line in "${PYTHON_VERSIONS[@]}"; do
    read -r -a PY <<< "$line"
    PYTHON_VERSION=${PY[0]}
    PYTHON_SPEC=${PY[1]}
    ARCH=${PY[2]}
    echo "--------- Build Python wheel for $PYTHON_VERSION -- $PYTHON_SPEC -- $ARCH"

    IMAGE=$BUILD_IMAGE_NAME:manylinux-$PYTHON_SPEC-$ARCH

    echo "Using image: $IMAGE"

    VOLUME_OPTION=${VOLUME_OPTION:-"-v $ROOT_DIR:/pulsar"}
    COMMAND="/pulsar/pulsar-client-cpp/docker/build-wheel-file-within-docker.sh"
    DOCKER_CMD="docker run -i ${VOLUME_OPTION} -e USE_FULL_POM_NAME -e NAME_POSTFIX ${IMAGE}"

    $DOCKER_CMD bash -c "${COMMAND}"

done
