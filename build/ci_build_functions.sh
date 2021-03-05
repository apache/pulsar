#!/usr/bin/env bash
# shell function library for CI builds
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

function ci_install_tool() {
  local tool_executable=$1
  local tool_package=${2:-$1}
  if ! command -v $tool_executable &>/dev/null; then
    echo "::group::Installing ${tool_package}"
    sudo apt-get -y install ${tool_package} >/dev/null
    echo '::endgroup::'
  fi
}

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
        echo "::error::The command has failed after $n attempts." >&2
        return 1
      fi
    }
  done
}

function ci_docker_save_image_to_github_actions_cache() {
  local image=$1
  local cachekey=$2
  ci_install_tool pv
  echo "::group::Saving docker image ${image} with key ${cachekey} in GitHub Actions Cache"
  ci_retry bash -c "docker save ${image} | zstd | pv -ft -i 5 | pv -Wbaf -i 5 | curl -s -H 'Content-Type: application/octet-stream' -X PUT --data-binary @- http://localhost:12321/${cachekey}"
  echo "::endgroup::"
}

function ci_docker_load_image_from_github_actions_cache() {
  local cachekey=$1
  ci_install_tool pv
  echo "::group::Loading docker image from key ${cachekey} in GitHub Actions Cache"
  ci_retry bash -c "curl -s http://localhost:12321/${cachekey} | pv -batf -i 5 | unzstd | docker load"
  echo "::endgroup::"
}

function ci_restore_tar_from_github_actions_cache() {
  local cachekey=$1
  ci_install_tool pv
  echo "::group::Restoring tar from key ${cachekey} to $PWD"
  ci_retry bash -c "curl -s http://localhost:12321/${cachekey} | pv -batf -i 5 | tar -I zstd -xf -"
  echo "::endgroup::"
}

function ci_store_to_github_actions_cache() {
  local cachekey=$1
  shift
  ci_install_tool pv
  echo "::group::Storing $1 command output to key ${cachekey}"
  ci_retry bash -c '"$@" | pv -ft -i 5 | pv -Wbaf -i 5 | curl -s -H "Content-Type: application/octet-stream" \
            -X PUT --data-binary @- \
            http://localhost:12321/'${cachekey} bash "$@"
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
      find -path '*/target/surefire-reports/junitreports/TEST-*.xml' | xargs -r -n 1 mv -t test-reports --backup=numbered
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

function ci_build_java_test_image() {
  ci_mvn_docker_build -am -pl tests/docker-images/java-test-image install -Pcore-modules,docker \
     -DskipSourceReleaseAssembly=true "$@"
}

function ci_build_pulsar_test_image() {
  # include building of Pulsar SQL, Connectors, Offloaders and server distros
  ci_mvn_docker_build -am \
  -pl pulsar-sql/presto-distribution,distribution/io,distribution/offloaders,distribution/server,tests/docker-images/latest-version-image \
  install -Pmain,docker "$@"
}

function ci_mvn_docker_build() {
  (
  local additional_params=()
  if [ "$CI" = "true" ]; then
    additional_params+=("-Ddockerfile.build.noCache=true")
  fi
  # check if docker experimental mode is enabled which is required for
  # using "docker build --squash" for squashing all intermediate layers of the build to a single layer
  if [[ "$(docker version -f '{{.Server.Experimental}}' 2>/dev/null)" == "true" ]]; then
    additional_params+=("-Ddockerfile.build.squash=true")
  else
    echo "Docker server doesn't have experimental features enabled (https://docs.docker.com/engine/reference/commandline/dockerd/#description). Images won't be optimized with '--squash'." >&2
  fi
  set -x
  ci_mvn "${additional_params[@]}" -Dmaven.test.skip=true -Dspotbugs.skip=true -Dlicense.skip=true "$@"
  )
}


function ci_replace_maven_wagon_http_library() {
  (
    # patches installed maven version to get fix for https://issues.apache.org/jira/browse/HTTPCORE-634
    MAVEN_HOME=$(mvn -v |grep 'Maven home:' | awk '{ print $3 }')
    if [ -d "$MAVEN_HOME" ]; then
      cd "$MAVEN_HOME/lib"
      rm wagon-http-*-shaded.jar
      curl -O https://repo1.maven.org/maven2/org/apache/maven/wagon/wagon-http/3.4.3/wagon-http-3.4.3-shaded.jar
    fi
  )
}

function ci_mvn() {
  (
  local additional_params=()
  if [ "$CI" = "true" ]; then
    additional_params+=(-B -ntp)
  fi
  set -x
  mvn "${additional_params[@]}" "$@"
  )
}

function ci_check_license_headers() {
  (
    ci_mvn -T 8 initialize license:check
  )
}

function ci_build_core_modules() {
  (
    ci_mvn -T 1C -Pcore-modules clean install -DskipTests -Dspotbugs.skip=true -Dlicense.skip=true
  )
}

function ci_list_functions() {
  declare -F | awk '{print $NF}' | sort | egrep '^ci_'
}

(
  # check if this file has been sourced
  sourced=0
  if [ -n "$ZSH_EVAL_CONTEXT" ]; then
    case $ZSH_EVAL_CONTEXT in *:file) sourced=1;; esac
  elif [ -n "$KSH_VERSION" ]; then
    [ "$(cd $(dirname -- $0) && pwd -P)/$(basename -- $0)" != "$(cd $(dirname -- ${.sh.file}) && pwd -P)/$(basename -- ${.sh.file})" ] && sourced=1
  elif [ -n "$BASH_VERSION" ]; then
    (return 0 2>/dev/null) && sourced=1
  else # All other shells: examine $0 for known shell binary filenames
    # Detects `sh` and `dash`; add additional shell filenames as needed.
    case ${0##*/} in sh|dash) sourced=1;; esac
  fi

  if [ $sourced -eq 0 ]; then
    ci_function_name=$1
    if [ -z "$ci_function_name" ]; then
      echo "usage: $0 [ci_function_name]"
      echo "Available ci functions:"
      ci_list_functions
      exit 1
    fi
    shift

    if [[ "$(LC_ALL=C type -t $ci_function_name)" == "function" ]]; then
      eval "$ci_function_name" "$@"
    else
      echo "Invalid ci function"
      echo "Available ci functions:"
      ci_list_functions
      exit 1
    fi
  fi
)
