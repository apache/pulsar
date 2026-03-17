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

# Adapted for Gradle build system

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

set -e
set -o pipefail
set -o errexit

# Base Gradle test options - skip non-test tasks
GRADLE_TEST_OPTIONS="./gradlew --no-daemon --continue"

function gradle_test() {
  (
    local coverage_arg=""
    if [[ "${COLLECT_COVERAGE}" != "false" ]]; then
      coverage_arg="jacocoTestReport"
    fi
    local use_fail_fast=1
    if [[ "$GITHUB_ACTIONS" == "true" && "$GITHUB_EVENT_NAME" != "pull_request" ]]; then
      use_fail_fast=0
    fi
    if [[ "$1" == "--no-fail-fast" ]]; then
      use_fail_fast=0
      shift;
    fi
    local failfast_args=""
    if [ $use_fail_fast -eq 1 ]; then
      failfast_args="-PtestFailFast=true --fail-fast"
    else
      failfast_args="-PtestFailFast=false"
    fi
    echo "::group::Run tests for " "$@"
    $GRADLE_TEST_OPTIONS $failfast_args "$@" $coverage_arg "${COMMANDLINE_ARGS[@]}"
    echo "::endgroup::"
    set +x
    "$SCRIPT_DIR/pulsar_ci_tool.sh" move_test_reports
  )
}

# solution for printing output in "set -x" trace mode without tracing the echo calls
shopt -s expand_aliases
echo_and_restore_trace() {
  builtin echo "$@"
  [ $trace_enabled -eq 1 ] && set -x || true
}
alias echo='{ [[ $- =~ .*x.* ]] && trace_enabled=1 || trace_enabled=0; set +x; } 2> /dev/null; echo_and_restore_trace'

# Test Groups  -- start --
function test_group_broker_group_1() {
  gradle_test :pulsar-broker:test -PtestGroups=broker -PtestReuseFork=true
  # run tests in broker-isolated group individually
  gradle_test :pulsar-broker:test --tests "org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsGeneratorWithNoUnsafeTest" -PtestForkCount=1 -PtestReuseFork=false
}

function test_group_broker_group_2() {
  gradle_test :pulsar-broker:test -PtestGroups='schema,utils,functions-worker,broker-io,broker-discovery,broker-compaction,broker-naming,websocket,other'
}

function test_group_broker_group_3() {
  gradle_test :pulsar-broker:test -PtestGroups=broker-admin
  # run AdminApiTransactionMultiBrokerTest independently with a larger heap size
  gradle_test :pulsar-broker:test --tests "org.apache.pulsar.broker.admin.v3.AdminApiTransactionMultiBrokerTest" -PtestMaxHeapSize=1500M -PtestForkCount=1 -PtestReuseFork=false
}

function test_group_broker_group_4() {
  gradle_test :pulsar-broker:test -PtestGroups=cluster-migration
}

function test_group_broker_group_5() {
  gradle_test :pulsar-broker:test -PtestGroups=broker-replication
}

function test_group_broker_client_api() {
  gradle_test :pulsar-broker:test -PtestGroups=broker-api
}

function test_group_broker_client_impl() {
  gradle_test :pulsar-broker:test -PtestGroups=broker-impl
}

function test_group_client() {
  gradle_test :pulsar-client:test
}

function test_group_metadata() {
  gradle_test :pulsar-metadata:test -PtestReuseFork=false
}

function test_group_protobufv4() {
  gradle_test :pulsar-client:test :pulsar-functions:instance:test \
    -Pprotobuf3.version=4.31.1 -Pprotoc3.version=4.31.1 \
    --tests "org.apache.pulsar.client.api.ProtobufSchemaApiSignatureTest" \
    --tests "org.apache.pulsar.client.impl.schema.ProtobufSchemaTest" \
    --tests "org.apache.pulsar.client.impl.schema.ProtobufNativeSchemaTest" \
    --tests "org.apache.pulsar.functions.source.TopicSchemaTest" \
    --tests "org.apache.pulsar.functions.instance.JavaInstanceRunnableTest"
}

# prints summaries of failed tests to console
# uses the Gradle test result XML files
function print_testng_failures() {
  (
    { set +x; } 2>/dev/null
    local testng_failed_file="$1"
    local report_prefix="${2:-Test failure in}"
    local group_title="${3:-Detailed test failures}"
    if [ -f "$testng_failed_file" ]; then
      local testng_report_dir=$(dirname "$testng_failed_file")
      local failed_count=0
      for failed_test_class in $(cat "$testng_failed_file" | grep 'class name=' | perl -p -e 's/.*\"(.*?)\".*/$1/'); do
        ((failed_count += 1))
        if [ $failed_count -eq 1 ]; then
          echo "::endgroup::"
          echo "::group::${group_title}"
        fi
        local test_report_file="${testng_report_dir}/${failed_test_class}.txt"
        if [ -f "${test_report_file}" ]; then
          local test_report="$(cat "${test_report_file}" | grep -E "^Tests run: " | perl -p -se 's/^(Tests run: .*) <<< FAILURE! - in (.*)$/::warning::$report_prefix $2 - $1/' -- -report_prefix="${report_prefix}")"
          echo "$test_report"
          cat "${test_report_file}"
        fi
      done
    fi
  )
}

function test_group_broker_flaky() {
  echo "::endgroup::"
  echo "::group::Running quarantined tests"
  gradle_test --no-fail-fast :pulsar-broker:test -PtestGroups=quarantine -PexcludedTestGroups=flaky -PfailIfNoTests=false \
    -PtestForkCount=2 ||
    print_testng_failures pulsar-broker/build/test-results/test/testng-failed.xml "Quarantined test failure in" "Quarantined test failures"
  echo "::endgroup::"
  echo "::group::Running flaky tests"
  gradle_test --no-fail-fast :pulsar-broker:test -PtestGroups=flaky -PexcludedTestGroups=quarantine -PtestForkCount=2
  echo "::endgroup::"
  local modules_with_flaky_tests=$(git grep -l '@Test.*"flaky"' | grep '/src/test/java/' | \
    awk -F '/src/test/java/' '{ print $1 }' | grep -v -E 'pulsar-broker' | sort | uniq | \
    sed 's|/|:|g; s/^/:/; s/:$//' | tr '\n' ' ')
  if [ -n "${modules_with_flaky_tests}" ]; then
    echo "::group::Running flaky tests in modules '${modules_with_flaky_tests}'"
    local gradle_tasks=""
    for mod in $modules_with_flaky_tests; do
      gradle_tasks="$gradle_tasks ${mod}:test"
    done
    gradle_test --no-fail-fast $gradle_tasks -PtestGroups=flaky -PexcludedTestGroups=quarantine -PfailIfNoTests=false
    echo "::endgroup::"
  fi
}

function test_group_proxy() {
    echo "::group::Running pulsar-proxy tests"
    gradle_test :pulsar-proxy:test --tests "org.apache.pulsar.proxy.server.ProxyServiceTlsStarterTest"
    gradle_test :pulsar-proxy:test --tests "org.apache.pulsar.proxy.server.ProxyServiceStarterTest"
    gradle_test :pulsar-proxy:test \
      -PexcludeTests='org.apache.pulsar.proxy.server.ProxyServiceTlsStarterTest,org.apache.pulsar.proxy.server.ProxyServiceStarterTest'
    echo "::endgroup::"
}

function test_group_other() {
  # Run all subproject tests except distribution, docker, broker, proxy, client, metadata, and IO
  gradle_test \
    :managed-ledger:test \
    :pulsar-common:test \
    :pulsar-broker-common:test \
    :pulsar-client-api:test \
    :pulsar-client-admin-api:test \
    :pulsar-client-admin:test \
    :pulsar-config-validation:test \
    :pulsar-functions:utils:test \
    :pulsar-functions:instance:test \
    :pulsar-functions:runtime:test \
    :pulsar-functions:worker:test \
    :pulsar-functions:secrets:test \
    :pulsar-transaction:common:test \
    :pulsar-transaction:coordinator:test \
    :pulsar-websocket:test \
    :pulsar-package-management:core:test \
    :pulsar-package-management:bookkeeper-storage:test \
    :pulsar-package-management:filesystem-storage:test \
    :pulsar-opentelemetry:test \
    :structured-event-log:test \
    :pulsar-broker-auth-athenz:test \
    :pulsar-broker-auth-oidc:test \
    :pulsar-broker-auth-sasl:test \
    :pulsar-client-auth-athenz:test \
    :pulsar-client-auth-sasl:test \
    :pulsar-client-messagecrypto-bc:test \
    :pulsar-cli-utils:test \
    :pulsar-docs-tools:test \
    :pulsar-testclient:test \
    :tiered-storage:jcloud:test \
    :tiered-storage:file-system:test \
    -PexcludeTests='**/ManagedLedgerTest.java,**/OffloadersCacheTest.java,**/OffsetsCacheTest.java,**/PrimitiveSchemaTest.java,**/BlobStoreManagedLedgerOffloaderTest.java,**/BlobStoreManagedLedgerOffloaderStreamingTest.java,**/DnsResolverTest.java'

  gradle_test :managed-ledger:test \
    --tests "*.ManagedLedgerTest" \
    --tests "*.OffloadersCacheTest"
  # DnsResolverTest needs to be run separately since it relies on static field values
  gradle_test :pulsar-common:test --tests "*.DnsResolverTest"

  gradle_test :tiered-storage:jcloud:test --tests "*.BlobStoreManagedLedgerOffloaderTest"
  gradle_test :tiered-storage:jcloud:test --tests "*.BlobStoreManagedLedgerOffloaderStreamingTest"
  gradle_test :tiered-storage:jcloud:test --tests "*.OffsetsCacheTest"

  echo "::endgroup::"
  local modules_with_quarantined_tests=$(git grep -l '@Test.*"quarantine"' | grep '/src/test/java/' | \
    awk -F '/src/test/java/' '{ print $1 }' | grep -v -E 'pulsar-broker|pulsar-proxy|pulsar-io|pulsar-client' | sort | uniq | \
    sed 's|/|:|g; s/^/:/; s/:$//' | tr '\n' ' ')
  if [ -n "${modules_with_quarantined_tests}" ]; then
    echo "::group::Running quarantined tests outside of pulsar-broker & pulsar-proxy (if any)"
    local gradle_tasks=""
    for mod in $modules_with_quarantined_tests; do
      gradle_tasks="$gradle_tasks ${mod}:test"
    done
    gradle_test --no-fail-fast $gradle_tasks -PtestGroups=quarantine -PexcludedTestGroups=flaky \
      -PfailIfNoTests=false || \
        echo "::warning::There were test failures in the 'quarantine' test group."
    echo "::endgroup::"
  fi
}

function test_group_pulsar_io() {
    echo "::group::Running pulsar-io tests"
    gradle_test \
      :pulsar-io:common:test \
      :pulsar-io:kafka:test \
      :pulsar-io:rabbitmq:test \
      :pulsar-io:cassandra:test \
      :pulsar-io:aerospike:test \
      :pulsar-io:kinesis:test \
      :pulsar-io:hdfs3:test \
      :pulsar-io:jdbc-core:test \
      :pulsar-io:jdbc-clickhouse:test \
      :pulsar-io:jdbc-mariadb:test \
      :pulsar-io:jdbc-postgres:test \
      :pulsar-io:jdbc-sqlite:test \
      :pulsar-io:data-generator:test \
      :pulsar-io:batch-data-generator:test \
      :pulsar-io:canal:test \
      :pulsar-io:file:test \
      :pulsar-io:hbase:test \
      :pulsar-io:http:test \
      :pulsar-io:influxdb:test \
      :pulsar-io:mongo:test \
      :pulsar-io:netty:test \
      :pulsar-io:redis:test \
      :pulsar-io:solr:test \
      :pulsar-io:dynamodb:test \
      :pulsar-io:nsq:test \
      :pulsar-io:alluxio:test \
      :pulsar-io:debezium-core:test \
      :pulsar-io:debezium-mongodb:test \
      :pulsar-io:debezium-mssql:test \
      :pulsar-io:debezium-mysql:test \
      :pulsar-io:debezium-oracle:test \
      :pulsar-io:debezium-postgres:test
    echo "::endgroup::"
}

function test_group_pulsar_io_elastic() {
    echo "::group::Running elastic-search tests"
    gradle_test :pulsar-io:elastic-search:test
    echo "::endgroup::"
}

function test_group_pulsar_io_kafka_connect() {
    echo "::group::Running Pulsar IO Kafka connect adaptor tests"
    gradle_test :pulsar-io:kafka-connect-adaptor:test
    echo "::endgroup::"
}

function list_test_groups() {
  declare -F | awk '{print $NF}' | sort | grep -E '^test_group_' | sed 's/^test_group_//g' | tr '[:lower:]' '[:upper:]'
}

# Test Groups  -- end --

if [[ "$1" == "--list" ]]; then
  list_test_groups
  exit 0
fi

TEST_GROUP=$1
if [ -z "$TEST_GROUP" ]; then
  echo "usage: $0 [test_group]"
  echo "Available test groups:"
  list_test_groups
  exit 1
fi
shift
COMMANDLINE_ARGS=("$@")
echo "Test Group : $TEST_GROUP"
test_group_function_name="test_group_$(echo "$TEST_GROUP" | tr '[:upper:]' '[:lower:]')"
if [[ "$(LC_ALL=C type -t "${test_group_function_name}")" == "function" ]]; then
  set -x
  eval "$test_group_function_name"
else
  echo "INVALID TEST GROUP"
  echo "Available test groups:"
  list_test_groups
  exit 1
fi
