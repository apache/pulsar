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

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

set -e
set -o pipefail
set -o errexit

JAVA_MAJOR_VERSION="$(java -version 2>&1 |grep " version " | awk -F\" '{ print $2 }' | awk -F. '{ if ($1=="1") { print $2 } else { print $1 } }')"

# runs integration tests via Gradle
# 1. builds required dependencies with ./gradlew :tests:assemble (and dependents)
# 2. runs tests with ./gradlew :tests:test with the given suite file and parameters
gradle_run_integration_test() {
  set +x
  # skip test run if next parameter is "--build-only"
  local build_only=0
  if [[ "$1" == "--build-only" ]]; then
      build_only=1
      shift
  fi
  local skip_build_deps=0
  while [[ "$1" == "--skip-build-deps" ]]; do
    skip_build_deps=1
    shift
  done
  local clean_arg=""
  if [[ "$1" == "--clean" ]]; then
      clean_arg="clean"
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
  local failfast_args=""
  if [ $use_fail_fast -eq 1 ]; then
    failfast_args="-PtestFailFast=true --fail-fast"
  else
    failfast_args="-PtestFailFast=false"
  fi
  local coverage_args=""
  if [[ "$1" == "--coverage" ]]; then
      if [ ! -d /tmp/jacocoDir ]; then
          mkdir /tmp/jacocoDir
          sudo chmod 0777 /tmp/jacocoDir || chmod 0777 /tmp/jacocoDir
      fi
      coverage_args="-Pcoverage -Pintegrationtest.coverage.enabled=true -Pintegrationtest.coverage.dir=/tmp/jacocoDir"
      shift
  fi

  # Collect remaining args, separating Gradle properties from test suite config
  local gradle_extra_args=()
  local suite_file=""
  local test_groups=""
  local test_fork_count=""
  local test_reuse_fork=""
  local test_retry_count=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -DintegrationTestSuiteFile=*)
        suite_file="${1#-DintegrationTestSuiteFile=}"
        ;;
      -DintegrationTests)
        # This was a Maven profile activation flag; in Gradle we use -PintegrationTests
        gradle_extra_args+=("-PintegrationTests")
        ;;
      -DShadeTests)
        gradle_extra_args+=("-PShadeTests")
        ;;
      -DBackwardsCompatTests)
        gradle_extra_args+=("-PBackwardsCompatTests")
        ;;
      -Dgroups=*)
        test_groups="${1#-Dgroups=}"
        ;;
      -DtestForkCount=*)
        test_fork_count="${1#-DtestForkCount=}"
        ;;
      -DtestReuseFork=*)
        test_reuse_fork="${1#-DtestReuseFork=}"
        ;;
      -DtestRetryCount=*)
        test_retry_count="${1#-DtestRetryCount=}"
        ;;
      -DskipDocker)
        # skip docker in Gradle is default behavior unless -Pdocker is passed
        ;;
      -D*)
        # convert remaining -D properties to -P for Gradle
        local prop="${1#-D}"
        gradle_extra_args+=("-P${prop}")
        ;;
      *)
        gradle_extra_args+=("$1")
        ;;
    esac
    shift
  done

  # Build Gradle property args
  local gradle_props=""
  if [[ -n "$suite_file" ]]; then
    gradle_props="$gradle_props -PintegrationTestSuiteFile=$suite_file"
  fi
  if [[ -n "$test_groups" ]]; then
    gradle_props="$gradle_props -PtestGroups=$test_groups"
  fi
  if [[ -n "$test_fork_count" ]]; then
    gradle_props="$gradle_props -PtestForkCount=$test_fork_count"
  fi
  if [[ -n "$test_reuse_fork" ]]; then
    gradle_props="$gradle_props -PtestReuseFork=$test_reuse_fork"
  fi
  if [[ -n "$test_retry_count" ]]; then
    gradle_props="$gradle_props -PtestRetryCount=$test_retry_count"
  fi

  (
  cd "$PROJECT_DIR"
  set -x
  if [ $skip_build_deps -ne 1 ]; then
    echo "::group::Build dependencies for integration tests"
    ./gradlew --no-daemon :tests:assemble $clean_arg ${gradle_extra_args[@]+"${gradle_extra_args[@]}"} $gradle_props
    echo "::endgroup::"
  fi
  if [[ $build_only -ne 1 ]]; then
    echo "::group::Run integration tests"
    ./gradlew --no-daemon :tests:test $failfast_args $coverage_args $clean_arg \
      ${gradle_extra_args[@]+"${gradle_extra_args[@]}"} $gradle_props
    echo "::endgroup::"
    set +x
    "$SCRIPT_DIR/pulsar_ci_tool.sh" move_test_reports || true
  fi
  )
}

test_group_shade() {
  gradle_run_integration_test "$@" -DShadeTests -DtestForkCount=1 -DtestReuseFork=false
}

test_group_shade_build() {
  gradle_run_integration_test --build-only "$@" -DShadeTests -DtestForkCount=1 -DtestReuseFork=false
}

test_group_shade_run() {
  gradle_run_integration_test --skip-build-deps --clean "$@" -DShadeTests -DtestForkCount=1 -DtestReuseFork=false
}

test_group_backwards_compat() {
  gradle_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-backwards-compatibility.xml -DintegrationTests
  gradle_run_integration_test "$@" -DBackwardsCompatTests -DtestForkCount=1 -DtestReuseFork=false
}

test_group_cli() {
  # run pulsar cli integration tests
  gradle_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-cli.xml -DintegrationTests

  # run pulsar auth integration tests
  gradle_run_integration_test --skip-build-deps "$@" -DintegrationTestSuiteFile=pulsar-auth.xml -DintegrationTests
}

test_group_function() {
  gradle_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-function.xml -DintegrationTests
}

test_group_messaging() {
  # run integration messaging tests
  gradle_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-messaging.xml -DintegrationTests
  # run integration proxy tests
  gradle_run_integration_test --skip-build-deps "$@" -DintegrationTestSuiteFile=pulsar-proxy.xml -DintegrationTests
  # run integration WebSocket tests
  gradle_run_integration_test --skip-build-deps "$@" -DintegrationTestSuiteFile=pulsar-websocket.xml -DintegrationTests
  # run integration TLS tests
  gradle_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-tls.xml -DintegrationTests
}

test_group_loadbalance() {
   gradle_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-loadbalance.xml -DintegrationTests
}

test_group_plugin() {
  gradle_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-plugin.xml -DintegrationTests
}

test_group_schema() {
  gradle_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-schema.xml -DintegrationTests
}

test_group_standalone() {
  gradle_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-standalone.xml -DintegrationTests
}

test_group_upgrade() {
 gradle_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-upgrade.xml -DintegrationTests
}

test_group_transaction() {
  gradle_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-transaction.xml -DintegrationTests
}

test_group_metrics() {
   gradle_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-metrics.xml -DintegrationTests
}

test_group_tiered_filesystem() {
  gradle_run_integration_test "$@" -DintegrationTestSuiteFile=tiered-filesystem-storage.xml -DintegrationTests
}

test_group_tiered_jcloud() {
  gradle_run_integration_test "$@" -DintegrationTestSuiteFile=tiered-jcloud-storage.xml -DintegrationTests
}

test_group_pulsar_connectors_thread() {
  # run integration function
  gradle_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-thread.xml -DintegrationTests -Dgroups=function

  # run integration source
  gradle_run_integration_test --skip-build-deps "$@" -DintegrationTestSuiteFile=pulsar-thread.xml -DintegrationTests -Dgroups=source

  # run integration sink
  gradle_run_integration_test --skip-build-deps "$@" -DintegrationTestSuiteFile=pulsar-thread.xml -DintegrationTests -Dgroups=sink
}

test_group_pulsar_connectors_process() {
  # run integration function
  gradle_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-process.xml -DintegrationTests -Dgroups=function

  # run integration source
  gradle_run_integration_test --skip-build-deps "$@" -DintegrationTestSuiteFile=pulsar-process.xml -DintegrationTests -Dgroups=source

  # run integration sink
  gradle_run_integration_test --skip-build-deps "$@" -DintegrationTestSuiteFile=pulsar-process.xml -DintegrationTests -Dgroups=sink
}

test_group_sql() {
  gradle_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-sql.xml -DintegrationTests -DtestForkCount=1 -DtestReuseFork=false
}

test_group_pulsar_k8s() {
  gradle_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-k8s.xml -DintegrationTests -DtestRetryCount=0
}

test_group_pulsar_io() {
  gradle_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-io-sources.xml -DintegrationTests -Dgroups=source
  gradle_run_integration_test --skip-build-deps "$@" -DintegrationTestSuiteFile=pulsar-io-sinks.xml -DintegrationTests -Dgroups=sink
}

test_group_pulsar_io_ora() {
  gradle_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-io-ora-source.xml -DintegrationTests -Dgroups=source -DtestRetryCount=0
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
