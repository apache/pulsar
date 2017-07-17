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

MVN_VERSION=`./get-version.sh`

echo "Pulsar version: ${MVN_VERSION}"

PULSAR_TGZ=$(dirname $PWD)/all/target/pulsar-${MVN_VERSION}-bin.tar.gz

if [ ! -f $PULSAR_TGZ ]; then
    echo "Pulsar bin distribution not found at ${PULSAR_TGZ}"
    exit 1
fi

LINKED_PULSAR_TGZ=pulsar-${MVN_VERSION}-bin.tar.gz
ln -f ${PULSAR_TGZ} $LINKED_PULSAR_TGZ

echo "Using Pulsar binary package at ${PULSAR_TGZ}"

# Build base image, reused by all other components
docker build --build-arg VERSION=${MVN_VERSION} \
             -t pulsar:latest .

if [ $? != 0 ]; then
    echo "Error: Failed to create Docker image for pulsar"
    exit 1
fi

rm pulsar-${MVN_VERSION}-bin.tar.gz


# Build pulsar-grafana image
docker build -t pulsar-grafana grafana
if [ $? != 0 ]; then
    echo "Error: Failed to create Docker image for pulsar-grafana"
    exit 1
fi

# Build dashboard docker image
docker build -t pulsar-dashboard ../dashboard
if [ $? != 0 ]; then
    echo "Error: Failed to create Docker image for pulsar-dashboard"
    exit 1
fi
