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

ROOT_DIR=$(git rev-parse --show-toplevel)
cd $ROOT_DIR/docker

# We should only publish images that are made from official and approved releases
# and thus ignore all the release candidates that are just tagged during the release
# process
if [[ ${TRAVIS_TAG} == *"candidate"* || ${TRAVIS_TAG} == *"rc"* ]]; then
    echo "Skipping non-final release tag ${TRAVIS_TAG}"
    exit 0
fi

if [ -z "$DOCKER_USER" ]; then
    echo "Docker user in variable \$DOCKER_USER was not set. Skipping image publishing"
    exit 1
fi

if [ -z "$DOCKER_PASSWORD" ]; then
    echo "Docker password in variable \$DOCKER_PASSWORD was not set. Skipping image publishing"
    exit 1
fi

DOCKER_ORG="${DOCKER_ORG:-apachepulsar}"

docker login -u="$DOCKER_USER" -p="$DOCKER_PASSWORD"
if [ $? -ne 0 ]; then
    echo "Failed to loging to Docker Hub"
    exit 1
fi

MVN_VERSION=`./get-version.sh`
echo "Pulsar version: ${MVN_VERSION}"

set -x

# Fail if any of the subsequent commands fail
set -e

docker tag pulsar:latest $DOCKER_ORG/pulsar:latest
docker tag pulsar-grafana:latest $DOCKER_ORG/pulsar-grafana:latest
docker tag pulsar-dashboard:latest $DOCKER_ORG/pulsar-dashboard:latest

docker tag pulsar:latest $DOCKER_ORG/pulsar:$MVN_VERSION
docker tag pulsar-grafana:latest $DOCKER_ORG/pulsar-grafana:$MVN_VERSION
docker tag pulsar-dashboard:latest $DOCKER_ORG/pulsar-dashboard:$MVN_VERSION

# Push all images and tags
docker push $DOCKER_ORG/pulsar:latest
docker push $DOCKER_ORG/pulsar-grafana:latest
docker push $DOCKER_ORG/pulsar-dashboard:latest
docker push $DOCKER_ORG/pulsar:$MVN_VERSION
docker push $DOCKER_ORG/pulsar-grafana:$MVN_VERSION
docker push $DOCKER_ORG/pulsar-dashboard:$MVN_VERSION
