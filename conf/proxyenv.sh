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

# default settings for starting proxy

# Configuration file of settings used in bookie server

# Log4j configuration file
# PROXY_LOG_CONF=

# Logs location
# PROXY_LOG_DIR=

# Memory size options
PROXY_MEM=${PROXY_MEM:-"-Xms2g -Xmx2g -XX:MaxDirectMemorySize=4g"}

# Garbage collection options
PROXY_GC=${PROXY_GC:-"-XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+ParallelRefProcEnabled -XX:+UnlockExperimentalVMOptions -XX:+DoEscapeAnalysis -XX:ParallelGCThreads=32 -XX:ConcGCThreads=32 -XX:G1NewSizePercent=50 -XX:-ResizePLAB"}

# Extra options to be passed to the jvm
PROXY_EXTRA_OPTS="${PROXY_EXTRA_OPTS:-"-Dio.netty.leakDetectionLevel=disabled -Dio.netty.recycler.maxCapacity.default=1000 -Dio.netty.recycler.linkCapacity=1024"}"
