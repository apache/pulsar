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
    # --no-configuration-cache: required because the Shadow plugin's filesMatching { filter { } }
    # lambdas in shaded JAR builds capture Gradle script references that can't be serialized.
    # This is a known Shadow plugin limitation tracked upstream.
    ./gradlew --no-configuration-cache $continue_args "$@" $failfast_args "${COMMANDLINE_ARGS[@]}"
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
  gradle_test :pulsar-broker:test -PtestGroups='broker'
  # run broker-isolated test individually
  gradle_test :pulsar-broker:test -PexcludedTestGroups='' \
    --tests 'org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsGeneratorWithNoUnsafeTest'
}

function test_group_broker_group_2() {
  gradle_test :pulsar-broker:test \
    -PtestGroups='schema,utils,functions-worker,broker-io,broker-discovery,broker-compaction,broker-naming,websocket,other,builtin,stats'
}

function test_group_broker_group_3() {
  gradle_test :pulsar-broker:test -PtestGroups='broker-admin'
  # run AdminApiTransactionMultiBrokerTest independently with a larger heap size
  gradle_test :pulsar-broker:test -PexcludedTestGroups='' \
    --tests 'org.apache.pulsar.broker.admin.v3.AdminApiTransactionMultiBrokerTest' \
    -DmaxHeapSize=1500m
}

function test_group_broker_group_4() {
  gradle_test :pulsar-broker:test -PtestGroups='cluster-migration'
}

function test_group_broker_group_5() {
  gradle_test :pulsar-broker:test -PtestGroups='broker-replication'
}

function test_group_broker_client_api() {
  gradle_test :pulsar-broker:test -PtestGroups='broker-api'
}

function test_group_broker_client_impl() {
  gradle_test :pulsar-broker:test -PtestGroups='broker-impl'
}

function test_group_client() {
  gradle_test :pulsar-client-original:test
}

function test_group_metadata() {
  gradle_test :pulsar-metadata:test
}

function test_group_proxy() {
  echo "::group::Running pulsar-proxy tests"
  gradle_test :pulsar-proxy:test
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
    -x :tests:pulsar-client-all-shade-test:test \
    -x :tests:pulsar-client-shade-test:test \
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

function test_group_protobufv4() {
  # Rebuild and test with protobuf v4 (overriding the default v3 version)
  gradle_test \
    -PprotobufVersion=4.31.1 \
    :pulsar-client-original:test \
    --tests "org.apache.pulsar.client.api.ProtobufSchemaApiSignatureTest" \
    --tests "org.apache.pulsar.client.impl.schema.ProtobufSchemaTest" \
    --tests "org.apache.pulsar.client.impl.schema.ProtobufNativeSchemaTest"
  gradle_test \
    -PprotobufVersion=4.31.1 \
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
