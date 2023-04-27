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

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

set -e
set -o pipefail
set -o errexit

JAVA_MAJOR_VERSION="$(java -version 2>&1 |grep " version " | awk -F\" '{ print $2 }' | awk -F. '{ if ($1=="1") { print $2 } else { print $1 } }')"
# Used to shade run test on Java 8, because the latest TestNG requires Java 11 or higher.
TESTNG_VERSION="7.3.0"

# lists all active maven modules with given parameters
# parses the modules from the "mvn initialize" output
# returns a CSV value
mvn_list_modules() {
  (
    mvn -B -ntp -Dscan=false "$@" initialize \
      | grep -- "-< .* >-" \
      | sed -E 's/.*-< (.*) >-.*/\1/' \
      | tr '\n' ',' | sed 's/,$/\n/'
  )
}

# runs integration tests
# 1. cds to "tests" directory and lists the active modules to be used as value
#    for "-pl" parameter of later mvn commands
# 2. runs "mvn -pl [active_modules] -am install [given_params]" to build and install required dependencies
# 3. finally runs tests with "mvn -pl [active_modules] test [given_params]"
mvn_run_integration_test() {
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
  local failfast_args
  if [ $use_fail_fast -eq 1 ]; then
    failfast_args="-DtestFailFast=true -DtestFailFastFile=/tmp/test_fail_fast_killswitch.$$.$RANDOM.$(date +%s) --fail-fast"
  else
    failfast_args="-DtestFailFast=false --fail-at-end"
  fi
  local coverage_args=""
  if [[ "$1" == "--coverage" ]]; then
      if [ ! -d /tmp/jacocoDir ]; then
          mkdir /tmp/jacocoDir
          sudo chmod 0777 /tmp/jacocoDir || chmod 0777 /tmp/jacocoDir
      fi
      coverage_args="-Pcoverage -Dintegrationtest.coverage.enabled=true -Dintegrationtest.coverage.dir=/tmp/jacocoDir"
      shift
  fi
  (
  cd "$SCRIPT_DIR"/../tests
  local modules=$(mvn_list_modules -DskipDocker "$@")
  cd ..
  set -x
  if [ $skip_build_deps -ne 1 ]; then
    echo "::group::Build dependencies for $modules"
    mvn -B -T 1C -ntp -pl "$modules" -DskipDocker -DskipSourceReleaseAssembly=true -Dspotbugs.skip=true -Dlicense.skip=true -Dmaven.test.skip=true -Dcheckstyle.skip=true -Drat.skip=true -am $clean_arg install "$@"
    echo "::endgroup::"
  fi
  if [[ $build_only -ne 1 ]]; then
    echo "::group::Run tests for " "$@"
    # use "verify" instead of "test"
    mvn -B -ntp -pl "$modules" $failfast_args $coverage_args -DskipDocker -DskipSourceReleaseAssembly=true -Dspotbugs.skip=true -Dlicense.skip=true -Dcheckstyle.skip=true -Drat.skip=true -DredirectTestOutputToFile=false $clean_arg verify "$@"
    echo "::endgroup::"
    set +x
    "$SCRIPT_DIR/pulsar_ci_tool.sh" move_test_reports || true
  fi
  )
}

test_group_shade() {
  mvn_run_integration_test "$@" -DShadeTests -DtestForkCount=1 -DtestReuseFork=false
}

test_group_shade_build() {
  mvn_run_integration_test --build-only "$@" -DShadeTests -DtestForkCount=1 -DtestReuseFork=false
}

test_group_shade_run() {
  local additional_args
  if [[ $JAVA_MAJOR_VERSION -gt 8 && $JAVA_MAJOR_VERSION -lt 17 ]]; then
    additional_args="-Dmaven.compiler.source=$JAVA_MAJOR_VERSION -Dmaven.compiler.target=$JAVA_MAJOR_VERSION"
  fi

  if [[ $JAVA_MAJOR_VERSION -ge 8 && $JAVA_MAJOR_VERSION -lt 11 ]]; then
      additional_args="$additional_args -Dtestng.version=$TESTNG_VERSION"
  fi

  mvn_run_integration_test --skip-build-deps --clean "$@" -Denforcer.skip=true -DShadeTests -DtestForkCount=1 -DtestReuseFork=false $additional_args
}

test_group_backwards_compat() {
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-backwards-compatibility.xml -DintegrationTests
  mvn_run_integration_test "$@" -DBackwardsCompatTests -DtestForkCount=1 -DtestReuseFork=false
}

test_group_cli() {
  # run pulsar cli integration tests
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-cli.xml -DintegrationTests

  # run pulsar auth integration tests
  mvn_run_integration_test --skip-build-deps "$@" -DintegrationTestSuiteFile=pulsar-auth.xml -DintegrationTests
}

test_group_function() {
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-function.xml -DintegrationTests
}

test_group_messaging() {
  # run integration messaging tests
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-messaging.xml -DintegrationTests
  # run integration proxy tests
  mvn_run_integration_test --skip-build-deps "$@" -DintegrationTestSuiteFile=pulsar-proxy.xml -DintegrationTests
  # run integration WebSocket tests
  mvn_run_integration_test --skip-build-deps "$@" -DintegrationTestSuiteFile=pulsar-websocket.xml -DintegrationTests
  # run integration TLS tests
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-tls.xml -DintegrationTests
}

test_group_plugin() {
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-plugin.xml -DintegrationTests
}

test_group_schema() {
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-schema.xml -DintegrationTests
}

test_group_standalone() {
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-standalone.xml -DintegrationTests
}

test_group_transaction() {
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-transaction.xml -DintegrationTests
}

test_group_tiered_filesystem() {
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=tiered-filesystem-storage.xml -DintegrationTests
}

test_group_tiered_jcloud() {
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=tiered-jcloud-storage.xml -DintegrationTests
}

test_group_pulsar_connectors_thread() {
  # run integration function
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-thread.xml -DintegrationTests -Dgroups=function

  # run integration source
  mvn_run_integration_test --skip-build-deps "$@" -DintegrationTestSuiteFile=pulsar-thread.xml -DintegrationTests -Dgroups=source

  # run integration sink
  mvn_run_integration_test --skip-build-deps "$@" -DintegrationTestSuiteFile=pulsar-thread.xml -DintegrationTests -Dgroups=sink
}

test_group_pulsar_connectors_process() {
  # run integration function
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-process.xml -DintegrationTests -Dgroups=function

  # run integration source
  mvn_run_integration_test --skip-build-deps "$@" -DintegrationTestSuiteFile=pulsar-process.xml -DintegrationTests -Dgroups=source

  # run integration sink
  mvn_run_integration_test --skip-build-deps "$@" -DintegrationTestSuiteFile=pulsar-process.xml -DintegrationTests -Dgroups=sink
}

test_group_sql() {
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-sql.xml -DintegrationTests -DtestForkCount=1 -DtestReuseFork=false
}

test_group_pulsar_io() {
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-io-sources.xml -DintegrationTests -Dgroups=source
  mvn_run_integration_test --skip-build-deps "$@" -DintegrationTestSuiteFile=pulsar-io-sinks.xml -DintegrationTests -Dgroups=sink
}

test_group_pulsar_io_ora() {
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-io-ora-source.xml -DintegrationTests -Dgroups=source -DtestRetryCount=0
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
