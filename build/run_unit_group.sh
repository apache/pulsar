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
set -x
set -o pipefail
set -o errexit

MVN_TEST_COMMAND='build/retry.sh mvn -B -ntp test'

# Test Groups  -- start --
function broker_group_1() {
  $MVN_TEST_COMMAND -pl pulsar-broker -Dinclude="**/AdminApiOffloadTest.java" \
                                      -DtestForkCount=1 \
                                      -DtestReuseFork=true

  $MVN_TEST_COMMAND -pl pulsar-broker -Dinclude="org/apache/pulsar/broker/**/*.java" \
                                      -Dexclude='**/*$*,org/apache/pulsar/broker/zookeeper/**/*.java,
                                                 org/apache/pulsar/broker/loadbalance/**/*.java,
                                                 org/apache/pulsar/broker/service/**/*.java,
                                                 **/AdminApiOffloadTest.java'

}

function broker_group_2() {
  $MVN_TEST_COMMAND -pl pulsar-broker -Dinclude="**/MessagePublishBufferThrottleTest.java" \
                                      -DtestForkCount=1 \
                                      -DtestReuseFork=true

  $MVN_TEST_COMMAND -pl pulsar-broker -Dinclude="**/ReplicatorTest.java" \
                                      -DtestForkCount=1 \
                                      -DtestReuseFork=true

  $MVN_TEST_COMMAND -pl pulsar-broker -Dinclude="**/TopicOwnerTest.java" \
                                      -DtestForkCount=1 \
                                      -DtestReuseFork=true

  $MVN_TEST_COMMAND -pl pulsar-broker -Dinclude="**/AntiAffinityNamespaceGroupTest.java" \
                                      -DtestForkCount=1 \
                                      -DtestReuseFork=true

  $MVN_TEST_COMMAND -pl pulsar-broker -Dinclude="**/*StreamingDispatcher*Test.java" \
                                      -DtestForkCount=1 \
                                      -DtestReuseFork=true

  $MVN_TEST_COMMAND -pl pulsar-broker -Dinclude="org/apache/pulsar/broker/zookeeper/**/*.java,
                                                 org/apache/pulsar/broker/loadbalance/**/*.java,
                                                 org/apache/pulsar/broker/service/**/*.java" \
                                      -Dexclude='**/*$*,**/ReplicatorTest.java,
                                                 **/MessagePublishBufferThrottleTest.java,
                                                 **/TopicOwnerTest.java,
                                                 **/*StreamingDispatcher*Test.java,
                                                 **/AntiAffinityNamespaceGroupTest.java'
}

function broker_client_api() {
  $MVN_TEST_COMMAND -pl pulsar-broker -Dinclude="**/DispatcherBlockConsumerTest.java" \
                                      -DtestForkCount=1 \
                                      -DtestReuseFork=true

  $MVN_TEST_COMMAND -pl pulsar-broker -Dinclude="**/SimpleProducerConsumerTest.java" \
                                      -DtestForkCount=1 \
                                      -DtestReuseFork=true

  $MVN_TEST_COMMAND -pl pulsar-broker -Dinclude="org/apache/pulsar/client/api/**/*.java" \
                                      -Dexclude='**/*$*,**/DispatcherBlockConsumerTest.java,
                                                 **/SimpleProducerConsumerTest.java'
}

function broker_client_impl() {
  $MVN_TEST_COMMAND -pl pulsar-broker -Dinclude="org/apache/pulsar/client/impl/**/*.java"
}

function broker_client_other() {
  $MVN_TEST_COMMAND -pl pulsar-broker -Dexclude='**/*$*,org/apache/pulsar/broker/**/*.java,
                                                 org/apache/pulsar/client/**/*.java'
}

function proxy() {
  $MVN_TEST_COMMAND -pl pulsar-proxy -DtestForkCount=1 \
                                     -DtestReuseFork=true \
                                     -Dinclude="**/ProxyRolesEnforcementTest.java" \
                                     -DtestForkCount=1 \
                                     -DtestReuseFork=true

  $MVN_TEST_COMMAND -pl pulsar-proxy -DtestForkCount=1 \
                                     -DtestReuseFork=true \
                                     -Dinclude="**/ProxyAuthenticationTest.java" \
                                     -DtestForkCount=1 \
                                     -DtestReuseFork=true

  $MVN_TEST_COMMAND -pl pulsar-proxy -DtestForkCount=1 \
                                     -DtestReuseFork=true \
                                     -Dinclude="**/ProxyTest.java" \
                                     -DtestForkCount=1 \
                                     -DtestReuseFork=true

  $MVN_TEST_COMMAND -pl pulsar-proxy -DtestForkCount=1 \
                                     -DtestReuseFork=true \
                                     -Dinclude="**/MessagePublishBufferThrottleTest.java" \
                                     -DtestForkCount=1 \
                                     -DtestReuseFork=true

  $MVN_TEST_COMMAND -pl pulsar-proxy -DtestForkCount=1 \
                                     -Dexclude='**/*$*,**/ProxyRolesEnforcementTest.java,
                                                **/ProxyAuthenticationTest.java,
                                                **/ProxyTest.java,
                                                **/MessagePublishBufferThrottleTest.java' \
                                     -DtestReuseFork=true
}

function other() {
  build/retry.sh mvn -B -ntp install -PbrokerSkipTest \
                                     -Dexclude='**/*$*,org/apache/pulsar/proxy/**/*.java,
                                                **/ManagedLedgerTest.java,
                                                **/TestPulsarKeyValueSchemaHandler.java,
                                                **/PrimitiveSchemaTest.java,
                                                BlobStoreManagedLedgerOffloaderTest.java'

  $MVN_TEST_COMMAND -pl managed-ledger -Dinclude="**/ManagedLedgerTest.java" \
                                       -DtestForkCount=1 \
                                       -DtestReuseFork=true

  $MVN_TEST_COMMAND -pl pulsar-sql/presto-pulsar-plugin -Dinclude="**/TestPulsarKeyValueSchemaHandler.java" \
                                                        -DtestForkCount=1 \
                                                        -DtestReuseFork=true

  $MVN_TEST_COMMAND -pl pulsar-client -Dinclude="**/PrimitiveSchemaTest.java" \
                                      -DtestForkCount=1 \
                                      -DtestReuseFork=true

  $MVN_TEST_COMMAND -pl tiered-storage/jcloud -Dinclude="**/BlobStoreManagedLedgerOffloaderTest.java" \
                                              -DtestForkCount=1 \
                                              -DtestReuseFork=true
}

# Test Groups  -- end --

TEST_GROUP=$1

echo -n "Test Group : $TEST_GROUP"

case $TEST_GROUP in

  BROKER_GROUP_1)
    broker_group_1
    ;;

  BROKER_GROUP_2)
    broker_group_2
    ;;

  BROKER_CLIENT_API)
    broker_client_api
    ;;

  BROKER_CLIENT_IMPL)
    broker_client_impl
    ;;

  BROKER_CLIENT_OTHER)
    broker_client_other
    ;;

  PROXY)
    proxy
    ;;

  OTHER)
    other
    ;;

  *)
    echo -n "INVALID TEST GROUP"
    exit 1
    ;;
esac

