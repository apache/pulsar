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
  mvn -B -ntp -DUBUNTU_MIRROR="${UBUNTU_MIRROR}" -DUBUNTU_SECURITY_MIRROR="${UBUNTU_SECURITY_MIRROR}" \
        "$@"
}

# runs OWASP Dependency Check for all projects
function ci_dependency_check() {
  _ci_mvn -Pmain,skip-all,skipDocker,owasp-dependency-check initialize verify -pl '!pulsar-client-tools-test' "$@"
}

function ci_pick_ubuntu_mirror() {
  echo "Choosing fastest up-to-date ubuntu mirror based on download speed..."
  UBUNTU_MIRROR=$({
    # choose mirrors that are up-to-date by checking the Last-Modified header for
    {
      # randomly choose up to 10 mirrors using http:// protocol
      # (https isn't supported in docker containers that don't have ca-certificates installed)
      curl -s http://mirrors.ubuntu.com/mirrors.txt | grep '^http://' | shuf -n 10
      # also consider Azure's Ubuntu mirror
      echo http://azure.archive.ubuntu.com/ubuntu/
    } | xargs -I {} sh -c 'echo "$(curl -m 5 -sI {}dists/$(lsb_release -c | cut -f2)-security/Contents-$(dpkg --print-architecture).gz|sed s/\\r\$//|grep Last-Modified|awk -F": " "{ print \$2 }" | LANG=C date -f- -u +%s)" "{}"' | sort -rg | awk '{ if (NR==1) TS=$1; if ($1 == TS) print $2 }'
  } | xargs -I {} sh -c 'echo `curl -r 0-102400 -m 5 -s -w %{speed_download} -o /dev/null {}ls-lR.gz` {}' \
    |sort -g -r |head -1| awk '{ print $2  }')
  if [ -z "$UBUNTU_MIRROR" ]; then
      # fallback to full mirrors list
      UBUNTU_MIRROR="mirror://mirrors.ubuntu.com/mirrors.txt"
  fi
  OLD_MIRROR=$(cat /etc/apt/sources.list | grep '^deb ' | head -1 | awk '{ print $2 }')
  echo "Picked '$UBUNTU_MIRROR'. Current mirror is '$OLD_MIRROR'."
  if [[ "$OLD_MIRROR" != "$UBUNTU_MIRROR" ]]; then
    sudo sed -i "s|$OLD_MIRROR|$UBUNTU_MIRROR|g" /etc/apt/sources.list
    sudo apt-get update
  fi
  # set the chosen mirror also in the UBUNTU_MIRROR and UBUNTU_SECURITY_MIRROR environment variables
  # that can be used by docker builds
  export UBUNTU_MIRROR
  export UBUNTU_SECURITY_MIRROR=$UBUNTU_MIRROR
  # make environment variables available for later GitHub Actions steps
  if [ -n "$GITHUB_ENV" ]; then
    echo "UBUNTU_MIRROR=$UBUNTU_MIRROR" >> $GITHUB_ENV
    echo "UBUNTU_SECURITY_MIRROR=$UBUNTU_SECURITY_MIRROR" >> $GITHUB_ENV
  fi
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
        ci_pick_ubuntu_mirror
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

# saves a given image (1st parameter) to the GitHub Actions Artifacts with the given name (2nd parameter)
function ci_docker_save_image_to_github_actions_artifacts() {
  local image=$1
  local artifactname="${2}.zst"
  ci_install_tool pv
  echo "::group::Saving docker image ${image} with name ${artifactname} in GitHub Actions Artifacts"
  # delete possible previous artifact that might exist when re-running
  timeout 1m gh-actions-artifact-client.js delete "${artifactname}" &>/dev/null || true
  docker save ${image} | zstd | pv -ft -i 5 | pv -Wbaf -i 5 | timeout 20m gh-actions-artifact-client.js upload --retentionDays=$ARTIFACT_RETENTION_DAYS "${artifactname}"
  echo "::endgroup::"
}

# loads a docker image from the GitHub Actions Artifacts with the given name (1st parameter)
function ci_docker_load_image_from_github_actions_artifacts() {
  local artifactname="${1}.zst"
  ci_install_tool pv
  echo "::group::Loading docker image from name ${artifactname} in GitHub Actions Artifacts"
  timeout 20m gh-actions-artifact-client.js download "${artifactname}" | pv -batf -i 5 | unzstd | docker load
  echo "::endgroup::"
}

# loads and extracts a zstd (.tar.zst) compressed tar file from the GitHub Actions Artifacts with the given name (1st parameter)
function ci_restore_tar_from_github_actions_artifacts() {
  local artifactname="${1}.tar.zst"
  ci_install_tool pv
  echo "::group::Restoring tar from name ${artifactname} in GitHub Actions Artifacts to $PWD"
  timeout 5m gh-actions-artifact-client.js download "${artifactname}" | pv -batf -i 5 | tar -I zstd -xf -
  echo "::endgroup::"
}

# stores a given command (with full arguments, specified after 1st parameter) output to GitHub Actions Artifacts with the given name (1st parameter)
function ci_store_tar_to_github_actions_artifacts() {
  local artifactname="${1}.tar.zst"
  shift
  if [[ "$GITHUB_ACTIONS" == "true" ]]; then
    ci_install_tool pv
    echo "::group::Storing $1 tar command output to name ${artifactname} in GitHub Actions Artifacts"
    # delete possible previous artifact that might exist when re-running
    timeout 1m gh-actions-artifact-client.js delete "${artifactname}" &>/dev/null || true
    "$@" | pv -ft -i 5 | pv -Wbaf -i 5 | timeout 10m gh-actions-artifact-client.js upload --retentionDays=$ARTIFACT_RETENTION_DAYS "${artifactname}"
    echo "::endgroup::"
  else
    local artifactfile="$(mktemp -t artifact.XXXX)"
    echo "Storing output for debugging in $artifactfile"
    "$@" | pv -ft -i 5 | pv -Wbaf -i 5 > $artifactfile
  fi
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

function ci_check_ready_to_test() {
  if [[ -z "$GITHUB_EVENT_PATH" ]]; then
    >&2 echo "GITHUB_EVENT_PATH isn't set"
    return 1
  fi

  PR_JSON_URL=$(jq -r '.pull_request.url' "${GITHUB_EVENT_PATH}")
  echo "Refreshing $PR_JSON_URL..."
  PR_JSON=$(curl -s -H "Authorization: Bearer $GITHUB_TOKEN" "${PR_JSON_URL}")

  if printf "%s" "${PR_JSON}" | jq -e '.draft | select(. == true)' &> /dev/null; then
    echo "PR is draft."
  elif ! ( printf "%s" "${PR_JSON}" | jq -e '.mergeable | select(. == true)' &> /dev/null ); then
    echo "PR isn't mergeable."
  else
    # check ready-to-test label
    if printf "%s" "${PR_JSON}" | jq -e '.labels[] | .name | select(. == "ready-to-test")' &> /dev/null; then
      echo "Found ready-to-test label."
      return 0
    else
      echo "There is no ready-to-test label on the PR."
    fi

    # check if the PR has been approved
    PR_NUM=$(jq -r '.pull_request.number' "${GITHUB_EVENT_PATH}")
    REPO_FULL_NAME=$(jq -r '.repository.full_name' "${GITHUB_EVENT_PATH}")
    REPO_NAME=$(basename "${REPO_FULL_NAME}")
    REPO_OWNER=$(dirname "${REPO_FULL_NAME}")
    # use graphql query to find out reviewDecision
    PR_REVIEW_DECISION=$(curl -s -H "Authorization: Bearer $GITHUB_TOKEN" -X POST -d '{"query": "query { repository(name: \"'${REPO_NAME}'\", owner: \"'${REPO_OWNER}'\") { pullRequest(number: '${PR_NUM}') { reviewDecision } } }"}' https://api.github.com/graphql |jq -r '.data.repository.pullRequest.reviewDecision')
    echo "Review decision for PR #${PR_NUM} in repository ${REPO_OWNER}/${REPO_NAME} is ${PR_REVIEW_DECISION}"
    if [[ "$PR_REVIEW_DECISION" == "APPROVED" ]]; then
      return 0
    fi
  fi

  FORK_REPO_URL=$(jq -r '.pull_request.head.repo.html_url' "$GITHUB_EVENT_PATH")
  PR_BRANCH_LABEL=$(jq -r '.pull_request.head.label' "$GITHUB_EVENT_PATH")
  PR_BASE_BRANCH=$(jq -r '.pull_request.base.ref' "$GITHUB_EVENT_PATH")
  PR_URL=$(jq -r '.pull_request.html_url' "$GITHUB_EVENT_PATH")
  FORK_PR_TITLE_URL_ENCODED=$(printf "%s" "${PR_JSON}" | jq -r '"[run-tests] " + .title | @uri')
  FORK_PR_BODY_URL_ENCODED=$(jq -n -r "\"This PR is for running tests for upstream PR ${PR_URL}.\n\n<!-- Before creating this PR, please ensure that the fork $FORK_REPO_URL is up to date with https://github.com/apache/pulsar -->\" | @uri")
  if [[ "$PR_BASE_BRANCH" != "master" ]]; then
    sync_non_master_fork_docs=$(cat <<EOF
 \\$('\n')
   If ${FORK_REPO_URL}/tree/${PR_BASE_BRANCH} is missing, you must sync the branch ${PR_BASE_BRANCH} on the command line.
   \`\`\`
   git fetch https://github.com/apache/pulsar ${PR_BASE_BRANCH}
   git push ${FORK_REPO_URL} FETCH_HEAD:refs/heads/${PR_BASE_BRANCH}
   \`\`\`
EOF
)
  else
    sync_non_master_fork_docs=""
  fi

  >&2 tee -a "$GITHUB_STEP_SUMMARY" <<EOF

# Instructions for proceeding with the pull request:

apache/pulsar pull requests should be first tested in your own fork since the apache/pulsar CI based on
GitHub Actions has constrained resources and quota. GitHub Actions provides separate quota for
pull requests that are executed in a forked repository.

1. Go to ${FORK_REPO_URL}/tree/${PR_BASE_BRANCH} and ensure that your ${PR_BASE_BRANCH} branch is up to date
   with https://github.com/apache/pulsar \\
   [Sync your fork if it's behind.](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/working-with-forks/syncing-a-fork)${sync_non_master_fork_docs}
2. Open a pull request to your own fork. You can use this link to create the pull request in
   your own fork:
   [Create PR in fork for running tests](${FORK_REPO_URL}/compare/${PR_BASE_BRANCH}...${PR_BRANCH_LABEL}?expand=1&title=${FORK_PR_TITLE_URL_ENCODED}&body=${FORK_PR_BODY_URL_ENCODED})
3. Edit the description of the pull request ${PR_URL} and add the link to the PR that you opened to your own fork
   so that the reviewer can verify that tests pass in your own fork.
4. Ensure that tests pass in your own fork. Your own fork will be used to run the tests during the PR review
   and any changes made during the review. You as a PR author are responsible for following up on test failures.
   Please report any flaky tests as new issues at https://github.com/apache/pulsar/issues
   after checking that the flaky test isn't already reported.
5. When the PR is approved, it will be possible to restart the Pulsar CI workflow within apache/pulsar
   repository by adding a comment "/pulsarbot rerun-failure-checks" to the PR.
   An alternative for the PR approval is to add a ready-to-test label to the PR. This can be done
   by Apache Pulsar committers.
6. When tests pass on the apache/pulsar side, the PR can be merged by a Apache Pulsar Committer.

If you have any trouble you can get support in multiple ways:
* by sending email to the [dev mailing list](mailto:dev@pulsar.apache.org) ([subscribe](mailto:dev-subscribe@pulsar.apache.org))
* on the [#contributors channel on Pulsar Slack](https://apache-pulsar.slack.com/channels/contributors) ([join](https://pulsar.apache.org/community#section-discussions))
* in apache/pulsar [GitHub discussions Q&A](https://github.com/apache/pulsar/discussions/categories/q-a)

EOF
  return 1
}

ci_snapshot_pulsar_maven_artifacts() {
  (
  if [ -n "$GITHUB_WORKSPACE" ]; then
    cd "$GITHUB_WORKSPACE"
  else
    fail "This script can only be run in GitHub Actions"
  fi
  mkdir -p target
  find $HOME/.m2/repository/org/apache/pulsar -name "*.jar" > /tmp/provided_pulsar_maven_artifacts
  )
}

ci_upload_unittest_coverage_files() {
  _ci_upload_coverage_files unittest "$1"
}

ci_upload_inttest_coverage_files() {
  _ci_upload_coverage_files_inttest inttest "$1" integration-tests
}

ci_upload_systest_coverage_files() {
  _ci_upload_coverage_files_inttest systest "$1" system-tests
}

_ci_upload_coverage_files_inttest() {
  local testtype="$1"
  local testgroup="$2"
  local job_name="$3"
  local store_deps=0
  local firsttestgroup="$(_ci_list_testgroups_with_coverage "${job_name}" | head -1)"
  if [[ "${firsttestgroup}" == "${testgroup}" ]]; then
    store_deps=1
  fi
  _ci_upload_coverage_files "${testtype}" "${testgroup}" $store_deps
}

_ci_upload_coverage_files() {
  (
  testtype="$1"
  testgroup="$2"
  store_deps="${3:-1}"
  echo "::group::Uploading $testtype coverage files"
  if [ -n "$GITHUB_WORKSPACE" ]; then
    cd "$GITHUB_WORKSPACE"
  else
    fail "This script can only be run in GitHub Actions"
  fi

  if [ ! -f /tmp/provided_pulsar_maven_artifacts ]; then
    fail "It is necessary to run '$0 snapshot_pulsar_maven_artifacts' before running any tests."
  fi

  set -x

  local classpathFile="target/classpath_${testtype}_${testgroup}"

  local execFiles=$(find . -path "*/target/jacoco*.exec" -printf "%P\n")
  if [[ -n "$execFiles" ]]; then
    # create temp file
    local completeClasspathFile=$(mktemp -t tmp.classpath.XXXX)

    ci_install_tool xmlstarlet

    local projects=$({
      for execFile in $execFiles; do
        echo $(dirname "$(dirname "$execFile")")
      done
    } | sort | uniq)

    # iterate the projects
    for project in $projects; do
      local artifactId=$(xmlstarlet sel -t -m _:project -v _:artifactId -n $project/pom.xml)
      # find the test scope classpath for the project
      mvn -f $project/pom.xml -DincludeScope=test -Dscan=false dependency:build-classpath  -B | { grep 'Dependencies classpath:' -A1 || true; } | tail -1 \
                                    | sed 's/:/\n/g' | { grep 'org/apache/pulsar' || true; } \
                                    | { tee -a $completeClasspathFile || true; } > target/classpath_$artifactId || true
    done

    cat $completeClasspathFile | sort | uniq > $classpathFile
    # delete temp file
    rm $completeClasspathFile

    # upload target/jacoco*.exec, target/classes and any dependent jar files that were built during the unit test execution
    # transform jacoco exec filenames by appending "_${testtype}_${testgroup}" to the filename part to make the files unique
    # so that they don't get overridden when all files are extracted to the same working directory before merging
    (
      cd /
      ci_store_tar_to_github_actions_artifacts coverage_and_deps_${testtype}_${testgroup} \
                tar -I zstd -cPf - \
                  --transform="flags=r;s|\\(/jacoco.*\\).exec$|\\1_${testtype}_${testgroup}.exec|" \
                  --transform="flags=r;s|\\(/tmp/jacocoDir/.*\\).exec$|\\1_${testtype}_${testgroup}.exec|" \
                  --exclude="*/META-INF/bundled-dependencies/*" \
                  --exclude="*/META-INF/versions/*" \
                  $GITHUB_WORKSPACE/target/classpath_* \
                  $(find "$GITHUB_WORKSPACE" -path "*/target/jacoco*.exec" -printf "%p\n%h/classes\n" | sort | uniq) \
                  $([ -d /tmp/jacocoDir ] && echo "/tmp/jacocoDir" ) \
                  $([[ $store_deps -eq 1 ]] && { cat $GITHUB_WORKSPACE/$classpathFile | sort | uniq | { grep -v -Fx -f /tmp/provided_pulsar_maven_artifacts || true; }; } || true)
    )
  fi
  echo "::endgroup::"
  )
}

ci_restore_unittest_coverage_files() {
  _ci_restore_coverage_files unittest unit-tests
}

ci_restore_inttest_coverage_files() {
  _ci_restore_coverage_files inttest integration-tests
}

ci_restore_systest_coverage_files() {
  _ci_restore_coverage_files systest system-tests
}

_ci_restore_coverage_files() {
  (
  test_type="$1"
  job_name="$2"
  cd /
  for testgroup in $(_ci_list_testgroups_with_coverage "${job_name}"); do
    ci_restore_tar_from_github_actions_artifacts coverage_and_deps_${test_type}_${testgroup} || true
  done
  )
}

_ci_list_testgroups_with_coverage() {
  local job_name="$1"
  yq e ".jobs.${job_name}.strategy.matrix.include.[] | select(.no_coverage != true) | .group" "$GITHUB_WORKSPACE/.github/workflows/pulsar-ci.yaml"
}

ci_delete_unittest_coverage_files() {
  _ci_delete_coverage_files unittest unit-tests
}

ci_delete_inttest_coverage_files() {
  _ci_delete_coverage_files inttest integration-tests
}

ci_delete_systest_coverage_files() {
  _ci_delete_coverage_files systest system-tests
}

_ci_delete_coverage_files() {
  (
  test_type="$1"
  job_name="$2"
  for testgroup in $(yq e ".jobs.${job_name}.strategy.matrix.include.[] | select(.no_coverage != true) | .group" "$GITHUB_WORKSPACE/.github/workflows/pulsar-ci.yaml"); do
    timeout 1m gh-actions-artifact-client.js delete coverage_and_deps_${test_type}_${testgroup}.tar.zst || true
  done
  )
}

# creates an aggregated jacoco xml report for all projects that contain a target/jacoco*.exec file
#
# the default maven jacoco report has multiple problems:
# - by default, jacoco:report goal will only report coverage for the current project. it is not suitable for Pulsar's
#   unit tests that test production code that resides in multiple modules.
#    - there's jacoco:report-aggregate that is supposed to resolve this. It has 2 issues:
#       - 0.8.8 version doesn't yet support the required "includeCurrentProject" feature
#       - the dependent projects must be built as part of the same mvn execution and belong to the same maven "reactor"
#          - this isn't compatible with the way how Pulsar CI builds in "Build and License check" job and reuses
#            the build results to run unit tests.
#
# This solution resolves the problem by using the Jacoco command line tool to generate the report.
# It assumes that all projects that contain a target/jacoco*.exec file will also contain compiled classfiles.
ci_create_test_coverage_report() {
  echo "::group::Create test coverage report"
  if [ -n "$GITHUB_WORKSPACE" ]; then
    cd "$GITHUB_WORKSPACE"
  else
    cd "$SCRIPT_DIR/.."
  fi

  local execFiles=$(find . -path "*/target/jacoco*.exec" -size +10c -printf "%P\n" )
  if [[ -n "$execFiles" ]]; then
    mkdir -p /tmp/jacocoDir
    if [ ! -f /tmp/jacocoDir/jacococli.jar ]; then
      local jacoco_version=$(mvn help:evaluate -Dscan=false -Dexpression=jacoco-maven-plugin.version -q -DforceStdout)
      curl -sL -o /tmp/jacocoDir/jacococli.jar "https://repo1.maven.org/maven2/org/jacoco/org.jacoco.cli/${jacoco_version}/org.jacoco.cli-${jacoco_version}-nodeps.jar"
    fi

    ci_install_tool xmlstarlet
    # create mapping from project directory to project artifactId
    local projectToArtifactIdMapping=$(find -name pom.xml -printf "%P\n" |xargs -I{} bash -c 'echo -n "$(dirname $1) "; xmlstarlet sel -t -m _:project -v _:artifactId -n $1' -- {})

    # create temp files
    local completeClasspathFile=$(mktemp -t tmp.classpath.XXXX)
    local filterArtifactsFile=$(mktemp -t tmp.artifacts.XXXX)
    local classesDir=$(mktemp -d -t tmp.classes.XXXX)
    local sourcefilesFile=$(mktemp -t tmp.sources.XXXX)

    local projects=$({
      for execFile in $execFiles; do
        echo $(dirname "$(dirname "$execFile")")
      done
    } | sort | uniq)

    # projects that aren't considered as production code and their own src/main/java source code shouldn't be analysed
    local excludeProjectsPattern="testmocks|testclient|buildtools"

    # iterate projects
    for project in $projects; do
      local artifactId="$(printf "%s" "$projectToArtifactIdMapping" | grep -F "$project " | cut -d' ' -f2)"
      if [[ -d "$project/target/classes" && -d "$project/src/main/java" ]]; then
        mkdir -p "$classesDir/$project"
        cp -Rl "$project/target/classes" "$classesDir/$project"
        echo "$project/src/main/java" >> $sourcefilesFile
      fi
      echo "/$artifactId/" >> $filterArtifactsFile
      if [[ -n "$(echo "$project" | grep -v -E "$excludeProjectsPattern")" ]]; then
        if [ -f "target/classpath_$artifactId" ]; then
          echo "Found cached classpath for $artifactId."
          cat "target/classpath_$artifactId" >> $completeClasspathFile
        else
          echo "Resolving classpath for $project..."
          # find the test scope classpath for the project
          mvn -f $project/pom.xml -DincludeScope=test -Dscan=false dependency:build-classpath  -B | { grep 'Dependencies classpath:' -A1 || true; } | tail -1 \
                                        | sed 's/:/\n/g' | { grep 'org/apache/pulsar' || true; } \
                                        >> $completeClasspathFile || true
        fi
      else
        echo "Skipping analysing of $project"
      fi
    done

    # delete any possible embedded jar files in the classes directory
    find "$classesDir" -name "*.jar" -print -delete

    local excludeJarsPattern="bouncy-castle-bc|tests|$excludeProjectsPattern"

    local classfilesArgs="--classfiles $({
      {
        for classpathEntry in $(cat $completeClasspathFile | { grep -v -f $filterArtifactsFile || true; } | sort | uniq | { grep -v -E "$excludeJarsPattern" || true; }); do
            if [[ -f $classpathEntry && -n "$(unzip -Z1C $classpathEntry 'META-INF/bundled-dependencies/*' 'META-INF/versions/*' 2>/dev/null)" ]]; then
              # file must be processed by removing META-INF/bundled-dependencies and META-INF/versions
              local jartempfile=$(mktemp -t jarfile.XXXX --suffix=.jar)
              cp $classpathEntry $jartempfile
              zip -q -d $jartempfile 'META-INF/bundled-dependencies/*' 'META-INF/versions/*' &> /dev/null
              echo $jartempfile
            else
              echo $classpathEntry
            fi
        done
      }
      echo $classesDir
    } | tr '\n' ':' | sed -e 's/:$//' -e 's/:/ --classfiles /g')"

    local sourcefilesArgs="--sourcefiles $({
      # find the source file folders for the pulsar .jar files that are on the classpath
      for artifactId in $(cat $completeClasspathFile  | sort | uniq | { grep -v -E "$excludeJarsPattern" || true; } | perl -p -e 's|.*/org/apache/pulsar/([^/]*)/.*|$1|'); do
        local project="$(printf "%s" "$projectToArtifactIdMapping" | { grep $artifactId || true; } | cut -d' ' -f1)"
        if [[ -n "$project" && -d "$project/src/main/java" ]]; then
          echo "$project/src/main/java"
        fi
      done
      cat $sourcefilesFile
    } | tr '\n' ':' | sed -e 's/:$//' -e 's/:/ --sourcefiles /g')"

    rm $completeClasspathFile $filterArtifactsFile $sourcefilesFile

    set -x
    mkdir -p target/jacoco_test_coverage_report/html
    java -jar /tmp/jacocoDir/jacococli.jar report $execFiles \
          $classfilesArgs \
          --encoding UTF-8 --name "Apache Pulsar test coverage" \
          $sourcefilesArgs \
          --xml target/jacoco_test_coverage_report/jacoco.xml \
          --html target/jacoco_test_coverage_report/html \
          --csv target/jacoco_test_coverage_report/jacoco.csv
    set +x

    rm -rf "$classesDir"
  fi
  echo "::endgroup::"
}

# creates a jacoco xml report of the jacoco exec files produced in docker containers which have /tmp/jacocoDir mounted as /jacocoDir
# this is used to calculate test coverage for the apache/pulsar code that is run inside the containers in integration tests
# and system tests
ci_create_inttest_coverage_report() {
  echo "::group::Create int test coverage in containers report"
  if [[ -n "$(find /tmp/jacocoDir -name "*.exec" -print -quit)" ]]; then
    cd "$GITHUB_WORKSPACE"
    echo "Creating coverage report to target/jacoco_inttest_coverage_report"
    set -x
    mkdir -p target/jacoco_inttest_coverage_report
    # install jacococli.jar command line tool
    if [ ! -f /tmp/jacocoDir/jacococli.jar ]; then
      local jacoco_version=$(mvn help:evaluate -Dexpression=jacoco-maven-plugin.version -Dscan=false -q -DforceStdout)
      curl -sL -o /tmp/jacocoDir/jacococli.jar "https://repo1.maven.org/maven2/org/jacoco/org.jacoco.cli/${jacoco_version}/org.jacoco.cli-${jacoco_version}-nodeps.jar"
    fi
    # extract the Pulsar jar files from the docker image that was used to run the tests in the docker containers
    # the class files used to produce the jacoco exec files are needed in the xml report generation
    if [ ! -d /tmp/jacocoDir/pulsar_lib ]; then
      mkdir /tmp/jacocoDir/pulsar_lib
      docker run --rm -u "$UID:${GID:-"$(id -g)"}" -v /tmp/jacocoDir/pulsar_lib:/pulsar_lib:rw ${PULSAR_TEST_IMAGE_NAME:-apachepulsar/java-test-image:latest} bash -c "cp -p /pulsar/lib/org.apache.pulsar-* /pulsar_lib; [ -d /pulsar/connectors ] && cp -R /pulsar/connectors /pulsar_lib || true"
      # remove jar file that causes duplicate classes issue
      rm /tmp/jacocoDir/pulsar_lib/org.apache.pulsar-bouncy-castle* || true
      # remove any bundled dependencies as part of .jar/.nar files
      find /tmp/jacocoDir/pulsar_lib '(' -name "*.jar" -or -name "*.nar" ')' -exec echo "Processing {}" \; -exec zip -q -d {} 'META-INF/bundled-dependencies/*' 'META-INF/versions/*' \; |grep -E -v "Nothing to do|^$" || true
    fi
    # projects that aren't considered as production code and their own src/main/java source code shouldn't be analysed
    local excludeProjectsPattern="testmocks|testclient|buildtools"
    # produce jacoco XML coverage report from the exec files and using the extracted jar files
    java -jar /tmp/jacocoDir/jacococli.jar report /tmp/jacocoDir/*.exec \
      --classfiles /tmp/jacocoDir/pulsar_lib --encoding UTF-8 --name "Pulsar Integration Tests - coverage in containers" \
      $(find -path "*/src/main/java" -printf "--sourcefiles %P " | grep -v -E "$excludeProjectsPattern") \
      --xml target/jacoco_inttest_coverage_report/jacoco.xml \
      --html target/jacoco_inttest_coverage_report/html \
      --csv target/jacoco_inttest_coverage_report/jacoco.csv
    set +x
  else
    echo "No /tmp/jacocoDir/*.exec files to process"
  fi
  echo "::endgroup::"
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
