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

set -e -x

export SRC_ROOT_DIR=$(git rev-parse --show-toplevel)
cd $SRC_ROOT_DIR/pulsar-client-cpp/pkg/apk

VERSION=`python3 $SRC_ROOT_DIR/src/get-project-version.py`
# Sanitize the version string
export POM_VERSION=`echo $VERSION | sed -E 's/-[a-z]+-/./' | sed -E 's/.[A-Z]+././'`

echo "POM_VERSION: $POM_VERSION"

abuild-keygen -a -i -n
chmod 755 ~
chmod 755 ~/.abuild
chmod 644 ~/.abuild/*

mkdir -p /root/packages
chmod 777 /root/packages

sudo -E -u pulsar abuild -r

mv /root/packages/pkg .
