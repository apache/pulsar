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

#
# This script cleans up old transaction logs and snapshots
#

#
# If this scripted is run out of /usr/bin or some other system bin directory
# it should be linked to and not copied. Things like java jar files are found
# relative to the canonical path of this script.
#
if [[ -z $JAVA_HOME ]]; then
    JAVA=$(which java)
    if [ $? != 0 ]; then
        echo "Error: JAVA_HOME not set, and no java executable found in $PATH." 1>&2
        exit 1
    fi
else
    JAVA=$JAVA_HOME/bin/java
fi

# use POSIX interface, symlink is followed automatically
ZOOBIN="${BASH_SOURCE-$0}"
ZOOBIN="$(dirname "$ZOOBIN")"
ZOOBINDIR="$(cd "$ZOOBIN" && pwd)"

CLASSPATH=""

for jar in "$ZOOBINDIR"/../lib/*.jar; do
  CLASSPATH="$CLASSPATH:$jar"
done

ZOO_LOG_DIR="$ZOOBINDIR"/../logs
mkdir -p "$ZOO_LOG_DIR"
ZOO_LOG_FILE=zookeeper-$USER-cli-$HOSTNAME.log

# shellcheck disable=SC2206
clientflags=($CLIENT_JVMFLAGS)
# shellcheck disable=SC2206
"$JAVA" "-Dzookeeper.log.dir=$ZOO_LOG_DIR" "-Dzookeeper.log.file=$ZOO_LOG_FILE" \
  "${clientflags[@]}" \
  -cp "$CLASSPATH" \
  org.apache.zookeeper.ZooKeeperMain "$@"