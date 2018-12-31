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

SRC_DIR=$(git rev-parse --show-toplevel)
cd $SRC_DIR

if [ -f /.dockerenv ]; then
    # When running tests inside docker. Unpack the pulsar tgz
    # because otherwise the classpath might not be correct
    # in picking up the jars from local maven repo
    export PULSAR_DIR=/tmp/pulsar-test-dist
    rm -rf $PULSAR_DIR
    mkdir $PULSAR_DIR
    TGZ=$(ls -1 $SRC_DIR/distribution/server/target/apache-pulsar*bin.tar.gz | head -1)
    tar xfz $TGZ -C $PULSAR_DIR --strip-components 1
else
    export PULSAR_DIR=$SRC_DIR
fi

DATA_DIR=/tmp/pulsar-test-data
rm -rf $DATA_DIR
mkdir -p $DATA_DIR

# Copy TLS test certificates
mkdir -p $DATA_DIR/certs
cp $SRC_DIR/pulsar-broker/src/test/resources/authentication/tls/*.pem $DATA_DIR/certs

# Generate secret key and token
mkdir -p $DATA_DIR/tokens
$PULSAR_DIR/bin/pulsar tokens create-secret-key --output $DATA_DIR/tokens/secret.key

$PULSAR_DIR/bin/pulsar tokens create \
            --subject token-principal \
            --secret-key file:///$DATA_DIR/tokens/secret.key \
            > $DATA_DIR/tokens/token.txt

export PULSAR_STANDALONE_CONF=$SRC_DIR/pulsar-client-cpp/test-conf/standalone-ssl.conf
$PULSAR_DIR/bin/pulsar-daemon start standalone \
        --no-functions-worker --no-stream-storage \
        --zookeeper-dir $DATA_DIR/zookeeper \
        --bookkeeper-dir $DATA_DIR/bookkeeper

echo "-- Wait for Pulsar service to be ready"
until curl http://localhost:8080/metrics > /dev/null 2>&1 ; do sleep 1; done

echo "-- Pulsar service is ready -- Configure permissions"

export PULSAR_CLIENT_CONF=$SRC_DIR/pulsar-client-cpp/test-conf/client-ssl.conf

# Create "standalone" cluster
$PULSAR_DIR/bin/pulsar-admin clusters create \
        standalone \
        --url http://localhost:8080/ \
        --url-secure https://localhost:8443/ \
        --broker-url pulsar://localhost:6650/ \
        --broker-url-secure pulsar+ssl://localhost:6651/

# Create "public" tenant
$PULSAR_DIR/bin/pulsar-admin tenants create public -r "anonymous" -c "standalone"

# Create "public/default" with no auth required
$PULSAR_DIR/bin/pulsar-admin namespaces create public/default \
                        --clusters standalone
$PULSAR_DIR/bin/pulsar-admin namespaces grant-permission public/default \
                        --actions produce,consume \
                        --role "anonymous"

# Create "public/default-2" with no auth required
$PULSAR_DIR/bin/pulsar-admin namespaces create public/default-2 \
                        --clusters standalone
$PULSAR_DIR/bin/pulsar-admin namespaces grant-permission public/default-2 \
                        --actions produce,consume \
                        --role "anonymous"

# Create "public/default-3" with no auth required
$PULSAR_DIR/bin/pulsar-admin namespaces create public/default-3 \
                        --clusters standalone
$PULSAR_DIR/bin/pulsar-admin namespaces grant-permission public/default-3 \
                        --actions produce,consume \
                        --role "anonymous"

# Create "private" tenant
$PULSAR_DIR/bin/pulsar-admin tenants create private -r "" -c "standalone"

# Create "private/auth" with required authentication
$PULSAR_DIR/bin/pulsar-admin namespaces create private/auth --clusters standalone

$PULSAR_DIR/bin/pulsar-admin namespaces grant-permission private/auth \
                        --actions produce,consume \
                        --role "token-principal"

echo "-- Ready to start tests"
