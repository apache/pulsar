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

shopt -s nullglob

function fail {
  echo $1 >&2
  exit 1
}

function retry {
  local n=1
  local max=3
  local delay=10
  while true; do
    "$@" && break || {
      if [[ $n -lt $max ]]; then
        if [[ -n "$CHANGED_TESTS" ]]; then
          if grep -q -F -f <(printf "$CHANGED_TESTS" | sed 's/,/\n/g' | sed -E 's|.*src/test/java/(.*)\.java$|\1|' | sed 's|/|.|g') */*/*/target/surefire-reports/testng-failed.xml */*/target/surefire-reports/testng-failed.xml */target/surefire-reports/testng-failed.xml; then
            fail "Not retrying since one of the changed tests failed."
          fi
        fi
        ((n++))
        echo "Command failed. Attempt $n/$max:"
        sleep $delay;
      else
        fail "The command has failed after $n attempts."
      fi
    }
  done
}

retry "$@"
