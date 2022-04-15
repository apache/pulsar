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
  for java_pid in $(jps -q -J-XX:+PerfDisableSharedMem); do
    echo "----------------------- pid $java_pid -----------------------"
    cat /proc/$java_pid/cmdline | xargs -0 echo
    jcmd $java_pid Thread.print -l
    jcmd $java_pid GC.heap_info
  done
  return 0
}

# runs maven
function _ci_mvn() {
  mvn -B -ntp "$@"
}

# runs OWASP Dependency Check for all projects
function ci_dependency_check() {
  _ci_mvn -Pmain,skip-all,skipDocker,owasp-dependency-check initialize verify -pl '!pulsar-client-tools-test' "$@"
}

# installs a tool executable if it's not found on the PATH
function ci_install_tool() {
  local tool_executable=$1
  local tool_package=${2:-$1}
  if ! command -v $tool_executable &>/dev/null; then
    echo "::group::Installing ${tool_package}"
    sudo apt-get -y install ${tool_package} >/dev/null
    echo '::endgroup::'
  fi
}

# outputs the given message to stderr and exits the shell script
function fail() {
  echo "$*" >&2
  exit 1
}

# function to retry a given commend 3 times with a backoff of 10 seconds in between
function ci_retry() {
  local n=1
  local max=3
  local delay=10
  while true; do
    "$@" && break || {
      if [[ $n -lt $max ]]; then
        ((n++))
        echo "::warning::Command failed. Attempt $n/$max:"
        sleep $delay
      else
        fail "::error::The command has failed after $n attempts."
      fi
    }
  done
}

# saves a given image (1st parameter) to the GitHub Actions Artifacts with the given name (2nd parameter)
function ci_docker_save_image_to_github_actions_artifacts() {
  local image=$1
  local artifactname="${2}.zst"
  ci_install_tool pv
  echo "::group::Saving docker image ${image} with name ${artifactname} in GitHub Actions Artifacts"
  # delete possible previous artifact that might exist when re-running
  gh-actions-artifact-client.js delete "${artifactname}" &>/dev/null || true
  docker save ${image} | zstd | pv -ft -i 5 | pv -Wbaf -i 5 | gh-actions-artifact-client.js upload --retentionDays=$ARTIFACT_RETENTION_DAYS "${artifactname}"
  echo "::endgroup::"
}

# loads a docker image from the GitHub Actions Artifacts with the given name (1st parameter)
function ci_docker_load_image_from_github_actions_artifacts() {
  local artifactname="${1}.zst"
  ci_install_tool pv
  echo "::group::Loading docker image from name ${artifactname} in GitHub Actions Artifacts"
  gh-actions-artifact-client.js download "${artifactname}" | pv -batf -i 5 | unzstd | docker load
  echo "::endgroup::"
}

# loads and extracts a zstd (.tar.zst) compressed tar file from the GitHub Actions Artifacts with the given name (1st parameter)
function ci_restore_tar_from_github_actions_artifacts() {
  local artifactname="${1}.tar.zst"
  ci_install_tool pv
  echo "::group::Restoring tar from name ${artifactname} in GitHub Actions Artifacts to $PWD"
  gh-actions-artifact-client.js download "${artifactname}" | pv -batf -i 5 | tar -I zstd -xf -
  echo "::endgroup::"
}

# stores a given command (with full arguments, specified after 1st parameter) output to GitHub Actions Artifacts with the given name (1st parameter)
function ci_store_tar_to_github_actions_artifacts() {
  local artifactname="${1}.tar.zst"
  shift
  ci_install_tool pv
  echo "::group::Storing $1 tar command output to name ${artifactname} in GitHub Actions Artifacts"
  # delete possible previous artifact that might exist when re-running
  gh-actions-artifact-client.js delete "${artifactname}" &>/dev/null || true
  "$@" | pv -ft -i 5 | pv -Wbaf -i 5 | gh-actions-artifact-client.js upload --retentionDays=$ARTIFACT_RETENTION_DAYS "${artifactname}"
  echo "::endgroup::"
}

# copies test reports into test-reports and surefire-reports directory
# subsequent runs of tests might overwrite previous reports. This ensures that all test runs get reported.
function ci_move_test_reports() {
  (
    if [ -n "${GITHUB_WORKSPACE}" ]; then
      cd "${GITHUB_WORKSPACE}"
      mkdir -p test-reports
      mkdir -p surefire-reports
    fi
    # aggregate all junit xml reports in a single directory
    if [ -d test-reports ]; then
      # copy test reports to single directory, rename duplicates
      find . -path '*/target/surefire-reports/junitreports/TEST-*.xml' -print0 | xargs -0 -r -n 1 mv -t test-reports --backup=numbered
      # rename possible duplicates to have ".xml" extension
      (
        for f in test-reports/*~; do
          mv -- "$f" "${f}.xml"
        done 2>/dev/null
      ) || true
    fi
    # aggregate all surefire-reports in a single directory
    if [ -d surefire-reports ]; then
      (
        find . -type d -path '*/target/surefire-reports' -not -path './surefire-reports/*' |
          while IFS=$'\n' read -r directory; do
            echo "Copying reports from $directory"
            target_dir="surefire-reports/${directory}"
            if [ -d "$target_dir" ]; then
              # rotate backup directory names *~3 -> *~2, *~2 -> *~3, *~1 -> *~2, ...
              ( command ls -vr1d "${target_dir}~"* 2> /dev/null | awk '{print "mv "$0" "substr($0,0,length-1)substr($0,length,1)+1}' | sh ) || true
              # backup existing target directory, these are the results of the previous test run
              mv "$target_dir" "${target_dir}~1"
            fi
            # copy files
            cp -R --parents "$directory" surefire-reports
            # remove the original directory
            rm -rf "$directory"
          done
      )
    fi
  )
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
