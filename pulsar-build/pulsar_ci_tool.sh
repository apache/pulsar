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

# shell function library for Pulsar CI builds

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

set -e
set -o pipefail

ARTIFACT_RETENTION_DAYS="${ARTIFACT_RETENTION_DAYS:-3}"

# lists all available functions in this tool
function ci_list_functions() {
  declare -F | awk '{print $NF}' | sort | grep -E '^ci_' | sed 's/^ci_//'
}

# prints thread dumps for all running JVMs
# used in CI when a job gets cancelled because of a job timeout
function ci_print_thread_dumps() {
  local dump_dir=""
  if [[ "$GITHUB_ACTIONS" == "true" ]]; then
    dump_dir="${GITHUB_WORKSPACE}/build/threaddumps"
    mkdir -p "$dump_dir"
  fi
  for java_pid in $(jps -q -J-XX:+PerfDisableSharedMem); do
    echo "----------------------- pid $java_pid -----------------------"
    cat /proc/$java_pid/cmdline | xargs -0 echo
    jcmd $java_pid Thread.print -l
    jcmd $java_pid GC.heap_info
    if [ -n "$dump_dir" ]; then
      cat /proc/$java_pid/cmdline | xargs -0 echo > "$dump_dir/cmdline_$java_pid.txt"
      jcmd $java_pid Thread.print -l > "$dump_dir/threaddump_$java_pid.txt"
      jcmd $java_pid GC.heap_info > "$dump_dir/heapinfo_$java_pid.txt"
    fi
  done
  return 0
}

# installs a tool executable if it's not found on the PATH
function ci_install_tool() {
  local tool_executable=$1
  local tool_package=${2:-$1}
  if ! command -v $tool_executable &>/dev/null; then
    if [[ "$GITHUB_ACTIONS" == "true" ]]; then
      echo "::group::Installing ${tool_package}"
      sudo apt-get -y install ${tool_package} >/dev/null || {
        echo "Installing the package failed. Switching the ubuntu mirror and retrying..."
        # retry after picking the ubuntu mirror
        sudo apt-get -y install ${tool_package}
      }
      echo '::endgroup::'
    else
      fail "$tool_executable wasn't found on PATH. You should first install $tool_package with your package manager."
    fi
  fi
}

# outputs the given message to stderr and exits the shell script
function fail() {
  echo "$*" >&2
  exit 1
}

# copies test reports into test-reports and test-results directories
# subsequent runs of tests might overwrite previous reports. This ensures that all test runs get reported.
function ci_move_test_reports() {
  (
    if [ -n "${GITHUB_WORKSPACE}" ]; then
      cd "${GITHUB_WORKSPACE}"
      mkdir -p test-reports
      mkdir -p test-results
    fi
    # aggregate all junit xml reports in a single directory
    if [ -d test-reports ]; then
      # copy test reports to single directory, rename duplicates
      # Gradle test reports
      find . -path '*/build/test-results/*/TEST-*.xml' -print0 | xargs -0 -r -n 1 mv -t test-reports --backup=numbered
      # rename possible duplicates to have ".xml" extension
      (
        for f in test-reports/*~; do
          mv -- "$f" "${f}.xml"
        done 2>/dev/null
      ) || true
    fi
    # aggregate all test-results in a single directory
    if [ -d test-results ]; then
      (
        # Gradle test-results directories
        find . -type d -path '*/build/test-results/test' -not -path './test-results/*' |
          while IFS=$'\n' read -r directory; do
            echo "Copying Gradle reports from $directory"
            target_dir="test-results/${directory}"
            if [ -d "$target_dir" ]; then
              ( command ls -vr1d "${target_dir}~"* 2> /dev/null | awk '{print "mv "$0" "substr($0,0,length-1)substr($0,length,1)+1}' | sh ) || true
              mv "$target_dir" "${target_dir}~1"
            fi
            cp -R --parents "$directory" test-results
            rm -rf "$directory"
          done
      )
    fi
  )
}

# generates a top-level index.html that links to each module's HTML test report
function ci_generate_test_report_index() {
  (
    if [ -n "${GITHUB_WORKSPACE}" ]; then
      cd "${GITHUB_WORKSPACE}"
    fi
    local index_file="test-reports/index.html"
    mkdir -p "$(dirname "$index_file")"
    local found_reports=()
    while IFS= read -r -d '' report; do
      found_reports+=("$report")
    done < <(find . -path '*/build/reports/tests/test/index.html' -not -path './build/*' -print0 | sort -z)
    if [ ${#found_reports[@]} -eq 0 ]; then
      echo "No HTML test reports found."
      return 0
    fi
    cat > "$index_file" <<'HEADER'
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8"/>
<title>Test Reports Index</title>
<style>
body { font-family: sans-serif; margin: 2em; }
a { text-decoration: none; color: #0366d6; }
a:hover { text-decoration: underline; }
li { margin: 0.3em 0; }
</style>
</head>
<body>
<h1>Test Reports</h1>
<ul>
HEADER
    for report in "${found_reports[@]}"; do
      # strip leading ./
      local rel_path="${report#./}"
      # extract module name (everything before /build/reports/tests/test/index.html)
      local module_name="${rel_path%%/build/reports/tests/test/index.html}"
      echo "  <li><a href=\"../${rel_path}\">${module_name}</a></li>" >> "$index_file"
    done
    cat >> "$index_file" <<'FOOTER'
</ul>
</body>
</html>
FOOTER
    echo "Generated test report index at $index_file with ${#found_reports[@]} module(s)."
  )
}

ci_report_netty_leaks() {
  if [ -z "$NETTY_LEAK_DUMP_DIR" ]; then
    echo "NETTY_LEAK_DUMP_DIR isn't set"
    return 0
  fi
  local temp_file=$(mktemp -t netty_leak.XXXX)

  # concat all netty_leak_*.txt files in the dump directory to a temp file
  if [ -d "$NETTY_LEAK_DUMP_DIR" ]; then
    find "$NETTY_LEAK_DUMP_DIR" -maxdepth 1 -type f -name "netty_leak_*.txt" -exec cat {} \; >> $temp_file
  fi

  # check if there are any netty_leak_*.txt files in the container logs
  local container_logs_dir="tests/integration/target/container-logs"
  if [ -d "$container_logs_dir" ]; then
    local container_netty_leak_dump_dir="$NETTY_LEAK_DUMP_DIR/container-logs"
    mkdir -p "$container_netty_leak_dump_dir"
    while read -r file; do
      # example file name "tests/integration/target/container-logs/ltnizrzm-standalone/var-log-pulsar.tar.gz"
      # take ltnizrzm-standalone part
      container_name=$(basename "$(dirname "$file")")
      target_dir="$container_netty_leak_dump_dir/$container_name"
      mkdir -p "$target_dir"
      tar -C "$target_dir" -zxf "$file" --strip-components=1 --wildcards --wildcards-match-slash '*/netty_leak_*.txt' >/dev/null 2>&1 || true
    done < <(find "$container_logs_dir" -type f -name "*.tar.gz")
    # remove all empty directories
    find "$container_netty_leak_dump_dir" -type d -empty -delete
    # print all netty_leak_*.txt files in the container logs dump directory to the temp file
    if [ -d "$container_netty_leak_dump_dir" ]; then
      find "$container_netty_leak_dump_dir" -type f -name "netty_leak_*.txt" -exec cat {} \; >> $temp_file
    fi
  fi

  if [ -s $temp_file ]; then
    local leak_found_log_message
    if [[ "$NETTY_LEAK_DETECTION" == "fail_on_leak" ]]; then
      leak_found_log_message="::error::Netty leaks found. Failing the build since Netty leak detection is set to 'fail_on_leak'."
    else
      leak_found_log_message="::warning::Netty leaks found."
    fi
    {
      echo "${leak_found_log_message}"
      local test_file_locations=$(grep -h -i test $temp_file | grep org.apache | sed 's/^[[:space:]]*//;s/[[:space:]]*$//;s/^Hint: //' | sort -u || true)
      if [[ -n "$test_file_locations" ]]; then
        echo "Test file locations in stack traces:"
        echo
        echo "$test_file_locations"
      fi
      echo "Details:"
      cat $temp_file
    } | tee $NETTY_LEAK_DUMP_DIR/leak_report.txt
    touch pulsar-build/netty_leaks_found
    if [[ "$NETTY_LEAK_DETECTION" == "fail_on_leak" ]]; then
      exit 1
    fi
  else
    echo "No netty leaks found."
    touch pulsar-build/netty_leaks_not_found
  fi
  rm $temp_file
}

if [ -z "$1" ]; then
  echo "usage: $0 [ci_tool_function_name]"
  echo "Available ci tool functions:"
  ci_list_functions
  exit 1
fi
ci_function_name="ci_$1"
shift

if [[ "$(LC_ALL=C type -t "${ci_function_name}")" == "function" ]]; then
  eval "$ci_function_name" "$@"
else
  echo "Invalid ci tool function"
  echo "Available ci tool functions:"
  ci_list_functions
  exit 1
fi
