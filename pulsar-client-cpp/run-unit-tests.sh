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

# Start 2 Pulsar standalone instances (one with TLS and one without)
# and execute the unit tests

rm -rf ./pulsar-dist
mkdir pulsar-dist
tar xfz ../all/target/apache-pulsar*bin.tar.gz  -C pulsar-dist --strip-components 1

PULSAR_STANDALONE_CONF=$PWD/test-conf/standalone.conf pulsar-dist/bin/pulsar standalone > broker.log &
standalone_pid=$!;

PULSAR_STANDALONE_CONF=$PWD/test-conf/authentication.conf pulsar-dist/bin/pulsar standalone \
              --zookeeper-port 2191 --bookkeeper-port 3191 \
              --zookeeper-dir data2/standalone/zookeeper --bookkeeper-dir \
              data2/standalone/bookkeeper > broker-tls.log &
auth_pid=$!;
sleep 10

PULSAR_CLIENT_CONF=$PWD/test-conf/client.conf pulsar-dist/bin/pulsar-admin clusters create \
        --url http://localhost:9765/ --url-secure https://localhost:9766/ \
        --broker-url pulsar://localhost:9885/ --broker-url-secure pulsar+ssl://localhost:9886/ \
        cluster
sleep 5

cd tests

if [ -f /gtest-parallel/gtest-parallel ]; then
    echo "---- Run unit tests in parallel"
    /gtest-parallel/gtest-parallel ./main --workers=10
    RES=$?
else
    ./main
    RES=$?
fi

kill -9 $standalone_pid $auth_pid

rm -rf pulsar-dist

exit $RES
