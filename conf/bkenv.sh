#!/bin/sh
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

# NOTE: this script is intentionally not executable. It is only meant to be sourced for environment variables.

# Set JAVA_HOME here to override the environment setting
# JAVA_HOME=

# default settings for starting bookkeeper

# Configuration file of settings used in bookie server
BOOKIE_CONF=${BOOKIE_CONF:-"$BK_HOME/conf/bookkeeper.conf"}

# Log4j configuration file
# BOOKIE_LOG_CONF=

# Logs location
BOOKIE_LOG_DIR=${BOOKIE_LOG_DIR:-"${PULSAR_LOG_DIR}"}

# Memory size options
BOOKIE_MEM=${BOOKIE_MEM:-${PULSAR_MEM:-"-Xms2g -Xmx2g -XX:MaxDirectMemorySize=2g"}}

# Garbage collection options
BOOKIE_GC=${BOOKIE_GC:-${PULSAR_GC:-"-XX:+UseZGC -XX:+PerfDisableSharedMem -XX:+AlwaysPreTouch"}}

if [ -z "$JAVA_HOME" ]; then
  JAVA_BIN=java
else
  JAVA_BIN="$JAVA_HOME/bin/java"
fi
for token in $("$JAVA_BIN" -version 2>&1 | grep 'version "'); do
    if [[ $token =~ \"([[:digit:]]+)\.([[:digit:]]+)(.*)\" ]]; then
        if [[ ${BASH_REMATCH[1]} == "1" ]]; then
          JAVA_MAJOR_VERSION=${BASH_REMATCH[2]}
        else
          JAVA_MAJOR_VERSION=${BASH_REMATCH[1]}
        fi
        break
    elif [[ $token =~ \"([[:digit:]]+)(.*)\" ]]; then
        # Process the java versions without dots, such as `17-internal`.
        JAVA_MAJOR_VERSION=${BASH_REMATCH[1]}
        break
    fi
done

if [[ -z "$BOOKIE_GC_LOG" ]]; then
  # fallback to PULSAR_GC_LOG if it is set
  BOOKIE_GC_LOG="$PULSAR_GC_LOG"
fi

BOOKIE_GC_LOG_DIR=${BOOKIE_GC_LOG_DIR:-"${PULSAR_GC_LOG_DIR:-"${BOOKIE_LOG_DIR}"}"}

if [[ -z "$BOOKIE_GC_LOG" ]]; then
  if [[ $JAVA_MAJOR_VERSION -gt 8 ]]; then
    BOOKIE_GC_LOG="-Xlog:gc*,safepoint:${BOOKIE_GC_LOG_DIR}/pulsar_bookie_gc_%p.log:time,uptime,tags:filecount=10,filesize=20M"
    if [[ $JAVA_MAJOR_VERSION -ge 17 ]]; then
      # Use async logging on Java 17+ https://bugs.openjdk.java.net/browse/JDK-8264323
      BOOKIE_GC_LOG="-Xlog:async ${BOOKIE_GC_LOG}"
    fi
  else
    # Java 8 gc log options
    BOOKIE_GC_LOG="-Xloggc:${BOOKIE_GC_LOG_DIR}/pulsar_bookie_gc_%p.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=20M"
  fi
fi

# Extra options to be passed to the jvm
BOOKIE_EXTRA_OPTS="${BOOKIE_EXTRA_OPTS:-"-Dio.netty.leakDetectionLevel=disabled ${PULSAR_EXTRA_OPTS:-"-Dio.netty.recycler.maxCapacityPerThread=4096"}"}"

# Add extra paths to the bookkeeper classpath
# BOOKIE_EXTRA_CLASSPATH=

#Folder where the Bookie server PID file should be stored
#BOOKIE_PID_DIR=

#Wait time before forcefully kill the Bookie server instance, if the stop is not successful
#BOOKIE_STOP_TIMEOUT=

#Entry formatter class to format entries.
#ENTRY_FORMATTER_CLASS=
