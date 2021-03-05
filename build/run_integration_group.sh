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
source "$SCRIPT_DIR/ci_build_functions.sh"

set -e
set -o pipefail
set -o errexit

# lists all active maven modules with given parameters
# parses the modules from the "mvn initialize" output
# returns a CSV value
mvn_list_modules() {
  (
    mvn -B -ntp "$@" initialize \
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
  (
  set +x
  skip_build_deps=0
  while [[ "$1" == "--skip-build-deps" ]]; do
    skip_build_deps=1
    shift
  done
  cd "$SCRIPT_DIR"/../tests
  modules=$(mvn_list_modules -DskipDocker "$@")
  cd ..
  set -x
  if [ $skip_build_deps -ne 1 ]; then
    echo "::group::Build dependencies for $modules"
    mvn -B -T 1C -ntp -pl "$modules" -DskipDocker -DskipSourceReleaseAssembly=true -Dspotbugs.skip=true -Dlicense.skip=true -Dmaven.test.skip=true -am install "$@"
    echo "::endgroup::"
  fi
  echo "::group::Run tests for " "$@"
  mvn -B -ntp -pl "$modules" -DskipDocker -DskipSourceReleaseAssembly=true -Dspotbugs.skip=true -Dlicense.skip=true -DredirectTestOutputToFile=false test "$@"
  echo "::endgroup::"
  set +x
  ci_move_test_reports
  )
}

test_group_shade() {
  mvn_run_integration_test "$@" -DShadeTests
}

test_group_backwards_compat() {
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-backwards-compatibility.xml -DBackwardsCompatTests
}

test_group_cli() {
  # run pulsar cli integration tests
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-cli.xml -DintegrationTests
  # run pulsar auth integration tests
  mvn_run_integration_test --skip-build-deps "$@" -DintegrationTestSuiteFile=pulsar-auth.xml -DintegrationTests
}

test_group_function_state() {
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-function-state.xml -DintegrationTests
}

test_group_messaging() {
  # run integration messaging tests
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-messaging.xml -DintegrationTests
  # run integration proxy tests
  mvn_run_integration_test --skip-build-deps "$@" -DintegrationTestSuiteFile=pulsar-proxy.xml -DintegrationTests
  # run integration proxy with WebSocket tests
  mvn_run_integration_test --skip-build-deps "$@" -DintegrationTestSuiteFile=pulsar-proxy-websocket.xml -DintegrationTests
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
  # run integraion sink
  mvn_run_integration_test --skip-build-deps "$@" -DintegrationTestSuiteFile=pulsar-process.xml -DintegrationTests -Dgroups=sink
}

test_group_sql() {
  mvn_run_integration_test "$@" -DintegrationTestSuiteFile=pulsar-sql.xml -DintegrationTests -DtestForkCount=1
}

list_test_groups() {
  declare -F | awk '{print $NF}' | sort | egrep '^test_group_' | sed 's/^test_group_//g' | tr '[:lower:]' '[:upper:]'
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
if [[ "$(LC_ALL=C type -t $test_group_function_name)" == "function" ]]; then
  eval "$test_group_function_name" "$@"
else
  echo "INVALID TEST GROUP"
  echo "Available test groups:"
  list_test_groups
  exit 1
fi