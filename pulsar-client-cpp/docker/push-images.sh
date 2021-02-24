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


# Create all the Docker images for variations of Python versions

set -e

DOCKER_ORG=apachepulsar

PYTHON_VERSIONS=(
   '2.7 cp27-cp27mu'
   '2.7 cp27-cp27m'
   '3.5 cp35-cp35m'
   '3.6 cp36-cp36m'
   '3.7 cp37-cp37m'
   '3.8 cp38-cp38'
   '3.9 cp39-cp39'
)

for line in "${PYTHON_VERSIONS[@]}"; do
    read -r -a PY <<< "$line"
    PYTHON_VERSION=${PY[0]}
    PYTHON_SPEC=${PY[1]}
    
    IMAGE_NAME=pulsar-build:manylinux-$PYTHON_SPEC
    FULL_NAME=$DOCKER_ORG/$IMAGE_NAME

    echo "IMAGE_NAME: $IMAGE_NAME"
    echo "FULL_NAME: $FULL_NAME"
    docker tag $IMAGE_NAME $FULL_NAME
    docker push $FULL_NAME

    echo "==== Successfully pushed image $FULL_NAME"
done
