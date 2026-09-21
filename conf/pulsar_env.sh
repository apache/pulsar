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

# Set JAVA_HOME here to override the environment setting
# JAVA_HOME=

# default settings for starting pulsar broker

# Log4j configuration file
# PULSAR_LOG_CONF=

# Logs location
# PULSAR_LOG_DIR=

# Log format: "text" (default) or "json" (flat OpenTelemetry JSON, useful for log aggregators)
# PULSAR_LOG_FORMAT=json

# Configuration file of settings used in broker server
# PULSAR_BROKER_CONF=

# Configuration file of settings used in bookie server
# PULSAR_BOOKKEEPER_CONF=

# Configuration file of settings used in zookeeper server
# PULSAR_ZK_CONF=

# Configuration file of settings used in global zookeeper server
# PULSAR_GLOBAL_ZK_CONF=

# Select the ByteBuf allocator by adding -Dpulsar.allocator.type=<value> to PULSAR_EXTRA_OPTS.
# Supported values (case-insensitive):
#   pooled   - Netty PooledByteBufAllocator; prefers direct buffers (default).
#   unpooled - Netty UnpooledByteBufAllocator; prefers heap buffers.
#   adaptive - Netty AdaptiveByteBufAllocator; auto-tunes pooling and prefers direct buffers.
# Named allocators override each setting independently with pulsar.allocator.<id>.<setting>:
#   pulsar.allocator.default.type  - allocator used by general Pulsar operations
#   pulsar.allocator.ml-cache.type - separate allocator used for managed-ledger cache copies (default: adaptive)
# Supported settings: type, exit_on_oom (false), out_of_memory_policy (FallbackToHeap or ThrowException).
# Named settings fall back to the unqualified pulsar.allocator.<setting>, then the built-in default.
# Explicit global type and legacy pooled settings also apply to ml-cache unless overridden by name.
# Batch reads copy entries into this cache even when managedLedgerCacheCopyEntries=false. Adaptive
# reuses small size-class slots to limit fragmentation; retained chunks and size rounding still cost memory.
# To retain the previous cache allocator: -Dpulsar.allocator.ml-cache.type=pooled
# Settings are read when an allocator is first created. default overrides do not apply to other IDs.
# Leak detection is global: use -Dio.netty.leakDetection.level=disabled|simple|advanced|paranoid.
# pulsar.allocator.leak_detection and per-allocator leak_detection settings are not supported.
# -Dpulsar.allocator.pooled=true is deprecated; use -Dpulsar.allocator.type=pooled instead.
# pulsar.allocator.type takes precedence over the legacy pulsar.allocator.pooled property.
# If pulsar.allocator.type is unset, pulsar.allocator.pooled=true (or unset) selects pooled;
# other values of pulsar.allocator.pooled select unpooled.

# Extra options to be passed to the jvm
PULSAR_MEM=${PULSAR_MEM:-"-Xms2g -Xmx2g -XX:MaxDirectMemorySize=4g"}

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

# Garbage collection options
if [ -z "$PULSAR_GC" ]; then
  PULSAR_GC="-XX:+PerfDisableSharedMem -XX:+AlwaysPreTouch"
  if [[ $JAVA_MAJOR_VERSION -eq 21 || $JAVA_MAJOR_VERSION -eq 22 ]]; then
    PULSAR_GC="-XX:+UseZGC -XX:+ZGenerational ${PULSAR_GC}"
  else
    PULSAR_GC="-XX:+UseZGC ${PULSAR_GC}"
  fi
fi

PULSAR_GC_LOG_DIR=${PULSAR_GC_LOG_DIR:-"${PULSAR_LOG_DIR}"}

if [[ -z "$PULSAR_GC_LOG" ]]; then
  if [[ $JAVA_MAJOR_VERSION -gt 8 ]]; then
    PULSAR_GC_LOG="-Xlog:gc*,safepoint:${PULSAR_GC_LOG_DIR}/pulsar_gc_%p.log:time,uptime,tags:filecount=10,filesize=20M"
    if [[ $JAVA_MAJOR_VERSION -ge 17 ]]; then
      # Use async logging on Java 17+ https://bugs.openjdk.java.net/browse/JDK-8264323
      PULSAR_GC_LOG="-Xlog:async ${PULSAR_GC_LOG}"
    fi
  else
    # Java 8 gc log options
    PULSAR_GC_LOG="-Xloggc:${PULSAR_GC_LOG_DIR}/pulsar_gc_%p.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCApplicationStoppedTime -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=20M"
  fi
fi

# Add extra paths to the bookkeeper classpath
# PULSAR_EXTRA_CLASSPATH=

#Folder where the Bookie server PID file should be stored
#PULSAR_PID_DIR=

#Wait time before forcefully kill the pulsar server instance, if the stop is not successful
#PULSAR_STOP_TIMEOUT=

# Enable semantically stable telemetry for JVM metrics, unless otherwise overridden by the user.
if [ -z "$OTEL_SEMCONV_STABILITY_OPT_IN" ]; then
  export OTEL_SEMCONV_STABILITY_OPT_IN=jvm
fi
