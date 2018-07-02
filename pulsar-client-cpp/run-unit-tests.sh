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
tar xfz ../distribution/server/target/apache-pulsar*bin.tar.gz  -C pulsar-dist --strip-components 1

PULSAR_STANDALONE_CONF=$PWD/test-conf/standalone.conf pulsar-dist/bin/pulsar standalone --no-functions-worker --no-stream-storage > broker.log &
standalone_pid=$!;

PULSAR_STANDALONE_CONF=$PWD/test-conf/standalone-ssl.conf pulsar-dist/bin/pulsar standalone \
              --no-functions-worker \
              --no-stream-storage \
              --zookeeper-port 2191 --bookkeeper-port 3191 \
              --zookeeper-dir data2/standalone/zookeeper --bookkeeper-dir \
              data2/standalone/bookkeeper > broker-tls.log &
auth_pid=$!;

echo "Wait for non-tls standalone up"
until grep "Created tenant public" broker.log; do sleep 5; done

# create property for test
PULSAR_CLIENT_CONF=$PWD/test-conf/client.conf pulsar-dist/bin/pulsar-admin tenants create prop -r "" -c "unit"
echo "Created tenant 'prop' - $?"

PULSAR_CLIENT_CONF=$PWD/test-conf/client.conf pulsar-dist/bin/pulsar-admin tenants create property -r "" -c "cluster"
echo "Created tenant 'property' - $?"

PULSAR_CLIENT_CONF=$PWD/test-conf/client-ssl.conf pulsar-dist/bin/pulsar-admin clusters create \
        --url http://localhost:9765/ --url-secure https://localhost:9766/ \
        --broker-url pulsar://localhost:9885/ --broker-url-secure pulsar+ssl://localhost:9886/ \
        cluster
PULSAR_CLIENT_CONF=$PWD/test-conf/client-ssl.conf pulsar-dist/bin/pulsar-admin clusters create \
        --url http://localhost:9765/ --url-secure https://localhost:9766/ \
        --broker-url pulsar://localhost:9885/ --broker-url-secure pulsar+ssl://localhost:9886/ \
        unit

sleep 5

pushd tests

if [ -f /gtest-parallel/gtest-parallel ]; then
    echo "---- Run unit tests in parallel"
    /gtest-parallel/gtest-parallel ./main --workers=10
    RES=$?
else
    ./main
    RES=$?
fi

popd

if [ $RES -eq 0 ]; then
    pushd python
    echo "---- Build Python Wheel file"
    python setup.py bdist_wheel

    echo "---- Installing  Python Wheel file"
    pip install dist/pulsar_client-*-linux_x86_64.whl

    echo "---- Running Python unit tests"

    # Running tests from a different directory to avoid importing directly
    # from the current dir, but rather using the installed wheel file
    cp pulsar_test.py /tmp
    pushd /tmp

    python pulsar_test.py
    RES=$?

    popd
    popd

fi

kill -9 $standalone_pid $auth_pid

rm -rf pulsar-dist

exit $RES
