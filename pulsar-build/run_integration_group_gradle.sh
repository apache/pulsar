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

function gradle_integration_test() {
  local use_fail_fast=1
  if [[ "$GITHUB_ACTIONS" == "true" && "$GITHUB_EVENT_NAME" != "pull_request" ]]; then
    use_fail_fast=0
  fi
  if [[ "$1" == "--no-fail-fast" ]]; then
    use_fail_fast=0
    shift
  fi
  local failfast_args=""
  if [ $use_fail_fast -eq 1 ]; then
    failfast_args="-PtestFailFast=true"
  else
    failfast_args="-PtestFailFast=false"
  fi

  local coverage_args=""
  if [[ "$1" == "--coverage" ]]; then
    coverage_args="-Pcoverage"
    shift
  fi

  echo "::group::Run integration tests for " "$@"
  set -x
  ./gradlew --no-configuration-cache :tests:integration:integrationTest "$@" $failfast_args $coverage_args
  set +x
  echo "::endgroup::"
  "$SCRIPT_DIR/pulsar_ci_tool.sh" move_test_reports
}

test_group_backwards_compat() {
  gradle_integration_test -PintegrationTestSuiteFile=pulsar-backwards-compatibility.xml "$@"
}

test_group_cli() {
  gradle_integration_test -PintegrationTestSuiteFile=pulsar-cli.xml "$@"
  gradle_integration_test -PintegrationTestSuiteFile=pulsar-auth.xml "$@"
}

test_group_messaging() {
  gradle_integration_test -PintegrationTestSuiteFile=pulsar-messaging.xml "$@"
  gradle_integration_test -PintegrationTestSuiteFile=pulsar-proxy.xml "$@"
  gradle_integration_test -PintegrationTestSuiteFile=pulsar-websocket.xml "$@"
  gradle_integration_test -PintegrationTestSuiteFile=pulsar-tls.xml "$@"
}

test_group_loadbalance() {
  gradle_integration_test -PintegrationTestSuiteFile=pulsar-loadbalance.xml "$@"
}

test_group_standalone() {
  gradle_integration_test -PintegrationTestSuiteFile=pulsar-standalone.xml "$@"
}

test_group_transaction() {
  gradle_integration_test -PintegrationTestSuiteFile=pulsar-transaction.xml "$@"
}

test_group_metrics() {
  gradle_integration_test -PintegrationTestSuiteFile=pulsar-metrics.xml "$@"
}

test_group_upgrade() {
  gradle_integration_test -PintegrationTestSuiteFile=pulsar-upgrade.xml "$@"
}

test_group_pulsar_k8s() {
  gradle_integration_test -PintegrationTestSuiteFile=pulsar-k8s.xml "$@"
}

test_group_function() {
  gradle_integration_test -PintegrationTestSuiteFile=pulsar-function.xml "$@"
}

test_group_schema() {
  gradle_integration_test -PintegrationTestSuiteFile=pulsar-schema.xml "$@"
}

test_group_tiered_filesystem() {
  gradle_integration_test -PintegrationTestSuiteFile=tiered-filesystem-storage.xml "$@"
}

test_group_tiered_jcloud() {
  gradle_integration_test -PintegrationTestSuiteFile=tiered-jcloud-storage.xml "$@"
}

test_group_pulsar_connectors_thread() {
  gradle_integration_test -PintegrationTestSuiteFile=pulsar-thread.xml -PtestGroups=function "$@"
  gradle_integration_test -PintegrationTestSuiteFile=pulsar-thread.xml -PtestGroups=source "$@"
  gradle_integration_test -PintegrationTestSuiteFile=pulsar-thread.xml -PtestGroups=sink "$@"
}

test_group_pulsar_connectors_process() {
  gradle_integration_test -PintegrationTestSuiteFile=pulsar-process.xml -PtestGroups=function "$@"
  gradle_integration_test -PintegrationTestSuiteFile=pulsar-process.xml -PtestGroups=source "$@"
  gradle_integration_test -PintegrationTestSuiteFile=pulsar-process.xml -PtestGroups=sink "$@"
}

test_group_pulsar_io() {
  gradle_integration_test -PintegrationTestSuiteFile=pulsar-io-sources.xml -PtestGroups=source "$@"
  gradle_integration_test -PintegrationTestSuiteFile=pulsar-io-sinks.xml -PtestGroups=sink "$@"
}

test_group_plugin() {
  gradle_integration_test -PintegrationTestSuiteFile=pulsar-plugin.xml "$@"
}

test_group_pulsar_io_ora() {
  gradle_integration_test -PintegrationTestSuiteFile=pulsar-io-ora-source.xml -PtestGroups=source -PtestRetryCount=0 "$@"
}

test_group_shade_run() {
  echo "::group::Run shade tests"
  ./gradlew --no-configuration-cache \
    :tests:pulsar-client-shade-test:test \
    :tests:pulsar-client-admin-shade-test:test \
    :tests:pulsar-client-all-shade-test:test \
    "$@"
  echo "::endgroup::"
  "$SCRIPT_DIR/pulsar_ci_tool.sh" move_test_reports
}

list_test_groups() {
  declare -F | awk '{print $NF}' | sort | grep -E '^test_group_' | sed 's/^test_group_//g' | tr '[:lower:]' '[:upper:]'
}

TEST_GROUP=$1
if [ -z "$TEST_GROUP" ]; then
  echo "usage: $0 [test_group]"
  echo "Available test groups:"
  list_test_groups
  exit 1
fi
shift

echo "Test Group : $TEST_GROUP"
test_group_function_name="test_group_$(echo "$TEST_GROUP" | tr '[:upper:]' '[:lower:]')"
if [[ "$(LC_ALL=C type -t "${test_group_function_name}")" == "function" ]]; then
  eval "$test_group_function_name" "$@"
else
  echo "INVALID TEST GROUP"
  echo "Available test groups:"
  list_test_groups
  exit 1
fi
