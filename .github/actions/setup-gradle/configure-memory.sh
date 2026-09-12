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

set -euo pipefail

profile="${1:-auto}"
case "$profile" in
  auto)
    profile=standard
    if [[ -r /proc/meminfo ]]; then
      memory_kib=$(awk '/^MemTotal:/ { print $2 }' /proc/meminfo)
      if [[ "$memory_kib" =~ ^[0-9]+$ ]] && (( memory_kib > 0 && memory_kib <= 8 * 1024 * 1024 )); then
        profile=low-memory
      fi
    fi
    ;;
  low-memory|standard) ;;
  *) echo "Unknown Gradle memory profile: $profile" >&2; exit 1 ;;
esac

echo "Gradle memory profile: $profile"
if [[ "$profile" == low-memory ]]; then
  gradle_dir="${GRADLE_USER_HOME:-$HOME/.gradle}"
  mkdir -p "$gradle_dir"
  # Bound workers across projects as well as forks within each test task. Leave
  # room for native JVM memory and containers, and recycle accumulated test state.
  printf '\n' >> "$gradle_dir/gradle.properties"
  cat >> "$gradle_dir/gradle.properties" <<'EOF'
org.gradle.jvmargs=-Xmx2g -Xss2m -XX:+UseG1GC -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp
org.gradle.workers.max=2
testMaxParallelForks=2
testForkEvery=50
EOF
fi
