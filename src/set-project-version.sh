#!/bin/bash
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

if [ $# -eq 0 ]; then
    echo "Required argument with new project version"
    exit 1
fi

NEW_VERSION=$1
OLD_VERSION=`mvn -q -Dexec.executable=echo -Dexec.args='${project.version}' --non-recursive exec:exec | sed 's/^\(.*\)-SNAPSHOT/\1/'`

# Go to top level project directory
pushd $(dirname "$0")/..

mvn versions:set -DnewVersion=$NEW_VERSION
mvn versions:set -DnewVersion=$NEW_VERSION -pl buildtools
mvn versions:set -DnewVersion=$NEW_VERSION -pl pulsar-sql/presto-distribution
# install the new version of root pom local, so `update-parent` can update the right parent version
sed "s/${OLD_VERSION}/${NEW_VERSION}/g" protobuf-shaded/pom.xml
mvn versions:set -DnewVersion=$NEW_VERSION -f protobuf-shaded/pom.xml

popd
