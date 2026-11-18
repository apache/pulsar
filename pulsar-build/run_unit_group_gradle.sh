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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

set -e
set -o pipefail
set -o errexit

function gradle_test() {
  (
    local use_fail_fast=1
    if [[ "$GITHUB_ACTIONS" == "true" && "$GITHUB_EVENT_NAME" != "pull_request" ]]; then
      use_fail_fast=0
    fi
    if [[ "$1" == "--no-fail-fast" ]]; then
      use_fail_fast=0
      shift
    fi
    local configuration_cache_arg="--no-configuration-cache"
    local test_durations_file=""
    if [[ "$1" == "--test-shards" ]]; then
      # see gradle_test_shards
      configuration_cache_arg="--configuration-cache"
      test_durations_file="$2"
      shift 2
    fi
    local failfast_args=""
    local continue_args=""
    if [ $use_fail_fast -eq 1 ]; then
      failfast_args="-PtestFailFast=true"
    else
      failfast_args="-PtestFailFast=false"
      # When fail-fast is off, use --continue so Gradle runs all test tasks
      # even if one module's tests fail (similar to Maven's --fail-at-end).
      continue_args="--continue"
    fi
    echo "::group::Run tests for " "$@"
    ./gradlew $configuration_cache_arg $continue_args "$@" $failfast_args "${COMMANDLINE_ARGS[@]}"
    echo "::endgroup::"
    set +x
    if [ -n "$test_durations_file" ]; then
      "$SCRIPT_DIR/pulsar_ci_tool.sh" record_test_durations "$test_durations_file"
    fi
    "$SCRIPT_DIR/pulsar_ci_tool.sh" move_test_reports
  )
}

# Runs a module's tests as `testShards`, four test tasks with one fork each, instead of `test`, whose four
# forks get the test classes round-robin in name order (see pulsar.java-conventions.gradle.kts). The shards
# get the classes packed by the durations that the group's latest successful CI run on master recorded (see
# download_test_durations in pulsar_ci_tool.sh), and a successful run records them. Without recorded
# durations, `testShards` runs `test`. Gradle runs the tasks of one project in parallel only with the
# configuration cache.
function gradle_test_shards() {
  local project="$1"
  shift
  local durations_file="${TEST_DURATIONS_DIR:-$PWD/build/test-durations}/${TEST_GROUP}.txt"
  gradle_test --test-shards "$durations_file" "${project}:testShards" -PtestShards=4 \
    -PtestShardDurations="$durations_file" "$@"
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
  gradle_test_shards :pulsar-broker -PtestGroups='broker'
  # run broker-isolated test individually
  gradle_test :pulsar-broker:test -PexcludedTestGroups='' \
    --tests 'org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsGeneratorWithNoUnsafeTest'
}

function test_group_broker_group_2() {
  gradle_test_shards :pulsar-broker \
    -PtestGroups='schema,utils,functions-worker,broker-io,broker-discovery,broker-compaction,broker-naming,websocket,other,builtin,stats'
}

function test_group_broker_group_3() {
  gradle_test_shards :pulsar-broker -PtestGroups='broker-admin'
  # run AdminApiTransactionMultiBrokerTest independently with a larger heap size
  gradle_test :pulsar-broker:test -PexcludedTestGroups='' \
    --tests 'org.apache.pulsar.broker.admin.v3.AdminApiTransactionMultiBrokerTest' \
    -DmaxHeapSize=1500m
}

function test_group_broker_group_4() {
  gradle_test :pulsar-broker:test -PtestGroups='cluster-migration'
}

function test_group_broker_group_5() {
  gradle_test_shards :pulsar-broker -PtestGroups='broker-replication'
}

function test_group_broker_client_api() {
  gradle_test_shards :pulsar-broker -PtestGroups='broker-api'
}

function test_group_broker_client_impl() {
  gradle_test_shards :pulsar-broker -PtestGroups='broker-impl'
}

function test_group_client() {
  gradle_test_shards :pulsar-client-original
}

function test_group_metadata() {
  gradle_test_shards :pulsar-metadata
}

function test_group_proxy() {
  echo "::group::Running pulsar-proxy tests"
  gradle_test_shards :pulsar-proxy
  echo "::endgroup::"
}

function test_group_broker_flaky() {
  echo "::endgroup::"
  echo "::group::Running quarantined tests"
  gradle_test --no-fail-fast :pulsar-broker:test \
    -PtestGroups='quarantine' -PexcludedTestGroups='flaky' || true
  echo "::endgroup::"
  echo "::group::Running flaky tests"
  gradle_test --no-fail-fast :pulsar-broker:test \
    -PtestGroups='flaky' -PexcludedTestGroups='quarantine'
  echo "::endgroup::"
}

# The OTHER group runs tests in all modules except broker, proxy, client, metadata, and IO modules
function test_group_other() {
  # Run tests in all modules except the ones that have dedicated CI groups.
  # Uses Gradle's -x flag to exclude tasks by path.
  # Gradle starts the test tasks in the order they are requested, and `test` requests them by project path.
  # tiered-storage-jcloud's tests are requested first: they take 2-4 minutes, mostly in one test class on one
  # fork, and when they started last they ran alone at the end of the build.
  gradle_test \
    --exclude-task :pulsar-broker:test \
    --exclude-task :pulsar-proxy:test \
    --exclude-task :pulsar-client-original:test \
    --exclude-task :pulsar-metadata:test \
    -x :pulsar-io:pulsar-io-core:test \
    -x :pulsar-io:pulsar-io-common:test \
    -x :pulsar-io:pulsar-io-data-generator:test \
    -x :pulsar-io:pulsar-io-batch-data-generator:test \
    -x :pulsar-io:pulsar-io-batch-discovery-triggerers:test \
    -x :tests:pulsar-client-admin-shade-test:test \
    -x :tests:pulsar-client-admin-v5-test:test \
    -x :tests:pulsar-client-all-shade-test:test \
    -x :tests:pulsar-client-v5-shade-test:test \
    -x :tests:pulsar-client-v5-all-test:test \
    -x :tests:pulsar-client-shade-test:test \
    -x :tests:pulsar-client-only-shade-test:test \
    -x :tests:pulsar-client-native-image:test \
    -x :tests:pulsar-client-java-compatibility:test \
    :tiered-storage:tiered-storage-jcloud:test \
    test

  # Run DnsResolverTest separately since it relies on static field values
  gradle_test :pulsar-common:test \
    -PexcludedTestGroups='' \
    --tests 'org.apache.pulsar.common.util.netty.DnsResolverTest'
}

function test_group_pulsar_io() {
  # Run pulsar-io module tests (connectors moved to pulsar-connectors repo)
  gradle_test :pulsar-io:pulsar-io-core:test \
    :pulsar-io:pulsar-io-common:test \
    :pulsar-io:pulsar-io-data-generator:test \
    :pulsar-io:pulsar-io-batch-data-generator:test \
    :pulsar-io:pulsar-io-batch-discovery-triggerers:test
}

function test_group_protobufv3() {
  # Rebuild and test with protobuf v3 (overriding the default v4 version)
  gradle_test \
    -PprotobufVersion=3.25.5 \
    :pulsar-client-original:test \
    --tests "org.apache.pulsar.client.api.ProtobufSchemaApiSignatureTest" \
    --tests "org.apache.pulsar.client.impl.schema.ProtobufSchemaTest" \
    --tests "org.apache.pulsar.client.impl.schema.ProtobufNativeSchemaTest" \
    --tests "org.apache.pulsar.client.impl.schema.ProtobufNativeSchemaUtilsTest"
  gradle_test \
    -PprotobufVersion=3.25.5 \
    :pulsar-functions:pulsar-functions-instance:test \
    --tests "org.apache.pulsar.functions.source.TopicSchemaTest" \
    --tests "org.apache.pulsar.functions.instance.JavaInstanceRunnableTest"
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
