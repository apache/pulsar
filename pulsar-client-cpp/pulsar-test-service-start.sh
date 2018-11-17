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

set -e

ROOT_DIR=$(git rev-parse --show-toplevel)
cd $ROOT_DIR

DATA_DIR=/tmp/pulsar-test-data
rm -rf $DATA_DIR
mkdir -p $DATA_DIR

export PULSAR_STANDALONE_CONF=$PWD/pulsar-client-cpp/test-conf/standalone-ssl.conf
bin/pulsar-daemon start standalone \
        --no-functions-worker --no-stream-storage \
        --zookeeper-dir $DATA_DIR/zookeeper \
        --bookkeeper-dir $DATA_DIR/bookkeeper

echo "-- Wait for Pulsar service to be ready"
until curl http://localhost:8080/metrics > /dev/null 2>&1 ; do sleep 1; done

echo "-- Pulsar service is ready -- Configure permissions"

export PULSAR_CLIENT_CONF=$PWD/pulsar-client-cpp/test-conf/client-ssl.conf

# Create "standalone" cluster
bin/pulsar-admin clusters create \
        standalone \
        --url http://localhost:8080/ \
        --url-secure https://localhost:8443/ \
        --broker-url pulsar://localhost:6650/ \
        --broker-url-secure pulsar+ssl://localhost:6651/

# Create "public" tenant
bin/pulsar-admin tenants create public -r "anonymous" -c "standalone"

# Create "public/default" with no auth required
bin/pulsar-admin namespaces create public/default --clusters standalone
bin/pulsar-admin namespaces grant-permission public/default --actions produce,consume --role "anonymous"

# Create "public/default-2" with no auth required
bin/pulsar-admin namespaces create public/default-2 --clusters standalone
bin/pulsar-admin namespaces grant-permission public/default-2 --actions produce,consume --role "anonymous"

# Create "public/default-3" with no auth required
bin/pulsar-admin namespaces create public/default-3 --clusters standalone
bin/pulsar-admin namespaces grant-permission public/default-3 --actions produce,consume --role "anonymous"

# Create "private" tenant
bin/pulsar-admin tenants create private -r "" -c "standalone"

# Create "private/auth" with required authentication
bin/pulsar-admin namespaces create private/auth --clusters standalone

echo "-- Ready to start tests"
