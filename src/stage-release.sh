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

if [ $# -eq 0 ]; then
    echo "Required argument with destination directory"
    exit 1
fi

DEST_PATH=$1
DEST_PATH="$(cd "$DEST_PATH" && pwd)"

pushd $(dirname "$0")
PULSAR_PATH=$(git rev-parse --show-toplevel)
VERSION=$(./get-pulsar-version.sh)
popd

# Source release: archive the committed tree (HEAD). git archive includes every
# tracked file, which covers the Gradle wrapper, build-logic, settings.gradle.kts,
# gradle.properties and all module build files needed to build Pulsar from source.
pushd "$PULSAR_PATH"
git archive --format=tar.gz --output="$DEST_PATH/apache-pulsar-$VERSION-src.tar.gz" --prefix="apache-pulsar-$VERSION-src/" HEAD
popd

# Binary distributions are produced by the Gradle build under build/distributions
# (e.g. ./gradlew assemble, or the individual *DistTar / *DistZip tasks).
cp $PULSAR_PATH/distribution/server/build/distributions/apache-pulsar-$VERSION-bin.tar.gz $DEST_PATH
cp $PULSAR_PATH/distribution/offloaders/build/distributions/apache-pulsar-offloaders-$VERSION-bin.tar.gz $DEST_PATH
cp $PULSAR_PATH/distribution/shell/build/distributions/apache-pulsar-shell-$VERSION-bin.tar.gz $DEST_PATH
cp $PULSAR_PATH/distribution/shell/build/distributions/apache-pulsar-shell-$VERSION-bin.zip $DEST_PATH

# Note: IO connectors are no longer staged here — pulsar-connectors is released separately.

# Sign all files
cd $DEST_PATH
find . -type f | grep -v LICENSE | grep -v README | xargs $PULSAR_PATH/src/sign-release.sh
