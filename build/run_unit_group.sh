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

MVN_TEST_OPTIONS='mvn -B -ntp -DskipSourceReleaseAssembly=true -DskipBuildDistribution=true -Dspotbugs.skip=true -Dlicense.skip=true -Dcheckstyle.skip=true -Drat.skip=true'

function mvn_test() {
  (
    local clean_arg=""
    if [[ "$1" == "--clean" ]]; then
        clean_arg="clean"
        shift
    fi
    local coverage_arg="-Pcoverage"
    if [[ "${COLLECT_COVERAGE}" == "false" ]]; then
      coverage_arg=""
    fi
    local target="verify"
    if [[ "$1" == "--install" ]]; then
      target="install"
      shift
    fi
    local use_fail_fast=1
    if [[ "$GITHUB_ACTIONS" == "true" && "$GITHUB_EVENT_NAME" != "pull_request" ]]; then
      use_fail_fast=0
    fi
    if [[ "$1" == "--no-fail-fast" ]]; then
      use_fail_fast=0
      shift;
    fi
    local failfast_args
    if [ $use_fail_fast -eq 1 ]; then
      failfast_args="-DtestFailFast=true -DtestFailFastFile=/tmp/test_fail_fast_killswitch.$$.$RANDOM.$(date +%s) --fail-fast"
    else
      failfast_args="-DtestFailFast=false --fail-at-end"
    fi
    echo "::group::Run tests for " "$@"
    # use "verify" instead of "test" to workaround MDEP-187 issue in pulsar-functions-worker and pulsar-broker projects with the maven-dependency-plugin's copy goal
    # Error message was "Artifact has not been packaged yet. When used on reactor artifact, copy should be executed after packaging: see MDEP-187"
    $MVN_TEST_OPTIONS $failfast_args $clean_arg $target $coverage_arg "$@" "${COMMANDLINE_ARGS[@]}"
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
  mvn_test -pl pulsar-broker -Dgroups='broker' -DtestReuseFork=true
  # run tests in broker-isolated group individually (instead of with -Dgroups=broker-isolated) to avoid scanning all test classes
  mvn_test -pl pulsar-broker -Dtest=org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsGeneratorWithNoUnsafeTest -DtestForkCount=1 -DtestReuseFork=false
}

function test_group_broker_group_2() {
  mvn_test -pl pulsar-broker -Dgroups='schema,utils,functions-worker,broker-io,broker-discovery,broker-compaction,broker-naming,websocket,other'
}

function test_group_broker_group_3() {
  mvn_test -pl pulsar-broker -Dgroups='broker-admin'
}

function test_group_broker_group_4() {
  mvn_test -pl pulsar-broker -Dgroups='cluster-migration'
}

function test_group_broker_client_api() {
  mvn_test -pl pulsar-broker -Dgroups='broker-api'
}

function test_group_broker_client_impl() {
  mvn_test -pl pulsar-broker -Dgroups='broker-impl'
}

function test_group_client() {
  mvn_test -pl pulsar-client
}

# prints summaries of failed tests to console
# by using the targer/surefire-reports files
# works only when testForkCount > 1 since that is when surefire will create reports for individual test classes
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
  mvn_test --no-fail-fast -pl pulsar-broker -Dgroups='quarantine' -DexcludedGroups='flaky' -DfailIfNoTests=false \
    -DtestForkCount=2 ||
    print_testng_failures pulsar-broker/target/surefire-reports/testng-failed.xml "Quarantined test failure in" "Quarantined test failures"
  echo "::endgroup::"
  echo "::group::Running flaky tests"
  mvn_test --no-fail-fast -pl pulsar-broker -Dgroups='flaky' -DexcludedGroups='quarantine' -DtestForkCount=2
  echo "::endgroup::"
  local modules_with_flaky_tests=$(git grep -l '@Test.*"flaky"' | grep '/src/test/java/' | \
    awk -F '/src/test/java/' '{ print $1 }' | grep -v -E 'pulsar-broker' | sort | uniq | \
    perl -0777 -p -e 's/\n(\S)/,$1/g')
  if [ -n "${modules_with_flaky_tests}" ]; then
    echo "::group::Running flaky tests in modules '${modules_with_flaky_tests}'"
    mvn_test --no-fail-fast -pl "${modules_with_flaky_tests}" -Dgroups='flaky' -DexcludedGroups='quarantine' -DfailIfNoTests=false
    echo "::endgroup::"
  fi
}

function test_group_proxy() {
    echo "::group::Running pulsar-proxy tests"
    mvn_test -pl pulsar-proxy -Dtest="org.apache.pulsar.proxy.server.ProxyServiceTlsStarterTest"
    mvn_test -pl pulsar-proxy -Dtest="org.apache.pulsar.proxy.server.ProxyServiceStarterTest"
    mvn_test -pl pulsar-proxy -Dexclude='org.apache.pulsar.proxy.server.ProxyServiceTlsStarterTest,
                                                  org.apache.pulsar.proxy.server.ProxyServiceStarterTest'
    echo "::endgroup::"
}

function test_group_other() {
  mvn_test --clean --install \
           -pl '!org.apache.pulsar:distribution,!org.apache.pulsar:pulsar-offloader-distribution,!org.apache.pulsar:pulsar-server-distribution,!org.apache.pulsar:pulsar-io-distribution,!org.apache.pulsar:pulsar-all-docker-image' \
           -PskipTestsForUnitGroupOther -DdisableIoMainProfile=true -DdisableSqlMainProfile=true -DskipIntegrationTests \
           -Dexclude='**/ManagedLedgerTest.java,
                   **/OffloadersCacheTest.java,
                   **/OffsetsCacheTest.java,
                  **/PrimitiveSchemaTest.java,
                  **/BlobStoreManagedLedgerOffloaderTest.java,
                  **/BlobStoreManagedLedgerOffloaderStreamingTest.java'

  mvn_test -pl managed-ledger -Dinclude='**/ManagedLedgerTest.java,
                                                  **/OffloadersCacheTest.java'

  mvn_test -pl tiered-storage/jcloud -Dinclude='**/BlobStoreManagedLedgerOffloaderTest.java'
  mvn_test -pl tiered-storage/jcloud -Dinclude='**/BlobStoreManagedLedgerOffloaderStreamingTest.java'
  mvn_test -pl tiered-storage/jcloud -Dinclude='**/OffsetsCacheTest.java'

  echo "::endgroup::"
  local modules_with_quarantined_tests=$(git grep -l '@Test.*"quarantine"' | grep '/src/test/java/' | \
    awk -F '/src/test/java/' '{ print $1 }' | grep -v -E 'pulsar-broker|pulsar-proxy|pulsar-io|pulsar-sql|pulsar-client' | sort | uniq | \
    perl -0777 -p -e 's/\n(\S)/,$1/g')
  if [ -n "${modules_with_quarantined_tests}" ]; then
    echo "::group::Running quarantined tests outside of pulsar-broker & pulsar-proxy (if any)"
    mvn_test --no-fail-fast -pl "${modules_with_quarantined_tests}" test -Dgroups='quarantine' -DexcludedGroups='flaky' \
      -DfailIfNoTests=false || \
        echo "::warning::There were test failures in the 'quarantine' test group."
    echo "::endgroup::"
  fi
}

function test_group_pulsar_io() {
    echo "::group::Running pulsar-io tests"
    mvn_test --install -Ppulsar-io-tests,-main
    echo "::endgroup::"

    echo "::group::Running pulsar-sql tests"
    mvn_test --install -Ppulsar-sql-tests,-main -DtestForkCount=1
    echo "::endgroup::"
}

function test_group_pulsar_io_elastic() {
    echo "::group::Running elastic-search tests"
    mvn_test --install -Ppulsar-io-elastic-tests,-main
    echo "::endgroup::"
}

function test_group_pulsar_io_kafka_connect() {
    echo "::group::Running Pulsar IO Kafka connect adaptor tests"
    mvn_test --install -Ppulsar-io-kafka-connect-tests,-main
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
