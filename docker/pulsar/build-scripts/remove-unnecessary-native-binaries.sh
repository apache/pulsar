#!/usr/bin/env sh
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

set -e

# Retain only native libraries for the architecture of this
# image
ARCH=$(uname -m)

# Remove extra binaries for Netty TCNative
ls /pulsar/lib/io.netty-netty-tcnative-boringssl-static*Final-*.jar | grep -v linux-$ARCH | xargs rm

# Prune extra libs from RocksDB JAR
mkdir /tmp/rocksdb
cd /tmp/rocksdb
ROCKSDB_JAR=$(ls /pulsar/lib/org.rocksdb-rocksdbjni-*.jar)
unzip $ROCKSDB_JAR > /dev/null

ls librocksdbjni-* | grep -v librocksdbjni-linux-$ARCH-musl.so | xargs rm
rm $ROCKSDB_JAR
zip -r -9 $ROCKSDB_JAR * > /dev/null
