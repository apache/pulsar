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

# Set JAVA_HOME here to override the environment setting
# JAVA_HOME=

# default settings for starting bookkeeper

# Configuration file of settings used in bookie server
BOOKIE_CONF=${BOOKIE_CONF:-"$BK_HOME/conf/bookkeeper.conf"}

# Log4j configuration file
# BOOKIE_LOG_CONF=

# Logs location
# BOOKIE_LOG_DIR=

# Memory size options
BOOKIE_MEM=${BOOKIE_MEM:-${PULSAR_MEM:-"-Xms2g -Xmx2g -XX:MaxDirectMemorySize=2g"}}

# Garbage collection options
BOOKIE_GC=${BOOKIE_GC:-${PULSAR_GC:-"-XX:+UseG1GC -XX:MaxGCPauseMillis=10 -XX:+ParallelRefProcEnabled -XX:+UnlockExperimentalVMOptions -XX:+DoEscapeAnalysis -XX:ParallelGCThreads=32 -XX:ConcGCThreads=32 -XX:G1NewSizePercent=50 -XX:+DisableExplicitGC -XX:-ResizePLAB"}}

# Extra options to be passed to the jvm
BOOKIE_EXTRA_OPTS="${BOOKIE_EXTRA_OPTS:-"-Dio.netty.leakDetectionLevel=disabled ${PULSAR_EXTRA_OPTS:-"-Dio.netty.recycler.maxCapacity.default=1000 -Dio.netty.recycler.linkCapacity=1024"}"} ${BOOKIE_MEM} ${BOOKIE_GC}"

# Add extra paths to the bookkeeper classpath
# BOOKIE_EXTRA_CLASSPATH=

#Folder where the Bookie server PID file should be stored
#BOOKIE_PID_DIR=

#Wait time before forcefully kill the Bookie server instance, if the stop is not successful
#BOOKIE_STOP_TIMEOUT=

#Entry formatter class to format entries.
#ENTRY_FORMATTER_CLASS=
