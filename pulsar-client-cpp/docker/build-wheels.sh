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

source ./pulsar-client-cpp/docker/python-versions.sh

function contains_build_version {
    for line in "${PYTHON_VERSIONS[@]}"; do
        read -r -a v <<< "$line"
        value="${v[0]} ${v[1]} ${v[2]} ${v[3]}"

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
    IMAGE=${PY[2]}
    ARCH=${PY[3]}
    echo "--------- Build Python wheel for $PYTHON_VERSION -- $IMAGE -- $PYTHON_SPEC -- $ARCH"

    IMAGE=$BUILD_IMAGE_NAME:${IMAGE}-$PYTHON_SPEC-$ARCH

    echo "Using image: $IMAGE"

    VOLUME_OPTION=${VOLUME_OPTION:-"-v $ROOT_DIR:/pulsar"}
    COMMAND="/pulsar/pulsar-client-cpp/docker/build-wheel-file-within-docker.sh"
    DOCKER_CMD="docker run -i ${VOLUME_OPTION} -e USE_FULL_POM_NAME -e NAME_POSTFIX ${IMAGE}"

    $DOCKER_CMD bash -c "ARCH=$ARCH ${COMMAND}"

done
