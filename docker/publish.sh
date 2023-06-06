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

ROOT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/.. >/dev/null 2>&1 && pwd )"
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

docker login ${DOCKER_REGISTRY} -u="$DOCKER_USER" -p="$DOCKER_PASSWORD"
if [ $? -ne 0 ]; then
    echo "Failed to loging to Docker Hub"
    exit 1
fi

MVN_VERSION=`./get-version.sh`
echo "Pulsar version: ${MVN_VERSION}"
GIT_COMMIT_ID_ABBREV=$(git rev-parse --short=7 HEAD 2>/dev/null || echo no-git)
GIT_BRANCH=$(git branch --show-current 2>/dev/null || echo no-git)
IMAGE_TAG="${MVN_VERSION}-${GIT_COMMIT_ID_ABBREV}"

if [[ -z ${DOCKER_REGISTRY} ]]; then
    docker_registry_org=${DOCKER_ORG}
else
    docker_registry_org=${DOCKER_REGISTRY}/${DOCKER_ORG}
    echo "Starting to push images to ${docker_registry_org}..."
fi

set -x

# Fail if any of the subsequent commands fail
set -e

if [[ "$GIT_BRANCH" == "master" ]]; then
  docker tag apachepulsar/pulsar:${IMAGE_TAG} ${docker_registry_org}/pulsar:latest
  docker tag apachepulsar/pulsar-all:${IMAGE_TAG} ${docker_registry_org}/pulsar-all:latest
fi

docker tag apachepulsar/pulsar:${IMAGE_TAG} ${docker_registry_org}/pulsar:$MVN_VERSION
docker tag apachepulsar/pulsar-all:${IMAGE_TAG} ${docker_registry_org}/pulsar-all:$MVN_VERSION

# Push all images and tags
if [[ "$GIT_BRANCH" == "master" ]]; then
  docker push ${docker_registry_org}/pulsar:latest
  docker push ${docker_registry_org}/pulsar-all:latest
fi

docker push ${docker_registry_org}/pulsar:$MVN_VERSION
docker push ${docker_registry_org}/pulsar-all:$MVN_VERSION

echo "Finished pushing images to ${docker_registry_org}"