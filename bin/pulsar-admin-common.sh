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

DEFAULT_CLIENT_CONF=$PULSAR_HOME/conf/client.conf
DEFAULT_LOG_CONF=$PULSAR_HOME/conf/log4j2.yaml

if [ -f "$PULSAR_HOME/conf/pulsar_tools_env.sh" ]
then
    . "$PULSAR_HOME/conf/pulsar_tools_env.sh"
fi

# Check for the java to use
if [[ -z $JAVA_HOME ]]; then
    JAVA=$(which java)
    if [ $? != 0 ]; then
        echo "Error: JAVA_HOME not set, and no java executable found in $PATH." 1>&2
        exit 1
    fi
else
    JAVA=$JAVA_HOME/bin/java
fi

for token in $("$JAVA" -version 2>&1 | grep 'version "'); do
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

# exclude tests jar
RELEASE_JAR=`ls $PULSAR_HOME/pulsar-*.jar 2> /dev/null | grep -v tests | tail -1`
if [ $? == 0 ]; then
    PULSAR_JAR=$RELEASE_JAR
fi

# exclude tests jar
BUILT_JAR=`ls $PULSAR_HOME/pulsar-client-tools/build/libs/pulsar-client-tools-*.jar 2> /dev/null | grep -v tests | tail -1`
if [ $? != 0 ] && [ ! -e "$PULSAR_JAR" ]; then
    echo "\nCouldn't find pulsar jar.";
    echo "Make sure you've run './gradlew assemble'\n";
    exit 1;
elif [ -e "$BUILT_JAR" ]; then
    PULSAR_JAR=$BUILT_JAR
fi

add_gradle_deps_to_classpath() {
    f="${PULSAR_HOME}/distribution/shell/build/classpath.txt"
    if [ ! -f "${f}" ]
    then
    (
      cd "${PULSAR_HOME}"
      ./gradlew :distribution:pulsar-shell-distribution:exportClasspath --no-configuration-cache -q 2> /dev/null
    )
    fi
    PULSAR_CLASSPATH=${CLASSPATH}:`cat "${f}"`
}

if [ -d "$PULSAR_HOME/lib" ]; then
    PULSAR_CLASSPATH="$PULSAR_CLASSPATH:$PULSAR_HOME/lib/*"
else
    add_gradle_deps_to_classpath
fi

if [ -z "$PULSAR_CLIENT_CONF" ]; then
    PULSAR_CLIENT_CONF=$DEFAULT_CLIENT_CONF
fi
if [ -z "$PULSAR_LOG_CONF" ]; then
    PULSAR_LOG_CONF=$DEFAULT_LOG_CONF
fi

PULSAR_CLASSPATH="$PULSAR_JAR:$PULSAR_CLASSPATH:$PULSAR_EXTRA_CLASSPATH"
PULSAR_CLASSPATH="`dirname $PULSAR_LOG_CONF`:$PULSAR_CLASSPATH"
OPTS="$OPTS -Dlog4j.configurationFile=`basename $PULSAR_LOG_CONF`"
OPTS="-Djava.net.preferIPv4Stack=true $OPTS"
# Required to allow sun.misc.Unsafe on JDK 24 without warnings
# Also required for enabling unsafe memory access for Netty since 4.1.121.Final
if [[ $JAVA_MAJOR_VERSION -ge 23 ]]; then
  OPTS="--sun-misc-unsafe-memory-access=allow $OPTS"
fi

# Allow Netty to use reflection access
OPTS="$OPTS -Dio.netty.tryReflectionSetAccessible=true"
OPTS="$OPTS -Dorg.apache.pulsar.shade.io.netty.tryReflectionSetAccessible=true"

if [[ $JAVA_MAJOR_VERSION -gt 8 ]]; then
  OPTS="$OPTS --add-opens java.base/sun.net=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED"
  # Required by Pulsar client optimized checksum calculation on other than Linux x86_64 platforms
  # reflection access to java.util.zip.CRC32C
  OPTS="$OPTS --add-opens java.base/java.util.zip=ALL-UNNAMED"
fi

if [[ $JAVA_MAJOR_VERSION -ge 11 ]]; then
  # Required by Netty for optimized direct byte buffer access
  OPTS="$OPTS --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/jdk.internal.misc=ALL-UNNAMED"
fi
# These two settings work together to ensure the Pulsar process exits immediately and predictably
# if it runs out of either Java heap memory or its internal off-heap memory,
# as these are unrecoverable errors that require a process restart to clear the faulty state and restore operation
OPTS="-XX:+ExitOnOutOfMemoryError -Dpulsar.allocator.exit_on_oom=true $OPTS"
# Netty tuning
# These settings are primarily used to modify the Netty allocator configuration,
# improving memory utilization and reducing the frequency of requesting off-heap memory from the OS
#
# Based on the netty source code, the allocator's default chunk size is calculated as:
# io.netty.allocator.pageSize (default: 8192) shifted left by
# io.netty.allocator.maxOrder (default: 9 after Netty 4.1.76.Final version).
# This equals 8192 * 2^9 = 4 MB：
# https://github.com/netty/netty/blob/4.1/buffer/src/main/java/io/netty/buffer/PooledByteBufAllocator.java#L105
#
# Allocations that are larger than chunk size are considered huge allocations and don't use the pool:
# https://github.com/netty/netty/blob/4.1/buffer/src/main/java/io/netty/buffer/PoolArena.java#L141-L142
#
# Currently, Pulsar defaults to a maximum single message size of 5 MB.
# Therefore, when frequently producing messages whose size exceeds the chunk size,
# Netty cannot utilize resources from the memory pool and must frequently allocate native memory.
# This can lead to increased physical memory fragmentation and higher reclamation costs.
# Thus, increasing io.netty.allocator.maxOrder to 10 to ensure that a single message is larger
# than chunk size (8MB) and can reuse Netty's memory pool.
OPTS="-Dio.netty.recycler.maxCapacityPerThread=4096 -Dio.netty.allocator.maxOrder=10 $OPTS"

OPTS="-cp $PULSAR_CLASSPATH $OPTS"

OPTS="$OPTS $PULSAR_EXTRA_OPTS"

# log directory & file
PULSAR_LOG_DIR=${PULSAR_LOG_DIR:-"$PULSAR_HOME/logs"}
PULSAR_LOG_APPENDER=${PULSAR_LOG_APPENDER:-"RoutingAppender"}
PULSAR_LOG_CONSOLE_JSON_TEMPLATE=${PULSAR_LOG_CONSOLE_JSON_TEMPLATE:-"classpath:EcsLayout.json"}
PULSAR_LOG_LEVEL=${PULSAR_LOG_LEVEL:-"info"}
PULSAR_LOG_ROOT_LEVEL=${PULSAR_LOG_ROOT_LEVEL:-"${PULSAR_LOG_LEVEL}"}
PULSAR_ROUTING_APPENDER_DEFAULT=${PULSAR_ROUTING_APPENDER_DEFAULT:-"Console"}
PULSAR_LOG_IMMEDIATE_FLUSH="${PULSAR_LOG_IMMEDIATE_FLUSH:-"false"}"

#Configure log configuration system properties
OPTS="$OPTS -Dpulsar.log.appender=$PULSAR_LOG_APPENDER"
OPTS="$OPTS -Dpulsar.log.console.json.template=$PULSAR_LOG_CONSOLE_JSON_TEMPLATE"
OPTS="$OPTS -Dpulsar.log.dir=$PULSAR_LOG_DIR"
OPTS="$OPTS -Dpulsar.log.level=$PULSAR_LOG_LEVEL"
OPTS="$OPTS -Dpulsar.log.root.level=$PULSAR_LOG_ROOT_LEVEL"
OPTS="$OPTS -Dpulsar.log.immediateFlush=$PULSAR_LOG_IMMEDIATE_FLUSH"
OPTS="$OPTS -Dpulsar.routing.appender.default=$PULSAR_ROUTING_APPENDER_DEFAULT"
