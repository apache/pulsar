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

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
cd "$SCRIPT_DIR/.."
SQUASH_PARAM=""
# check if docker experimental mode is enabled which is required for
# using "docker build --squash" for squashing all intermediate layers of the build to a single layer
if [[ "$(docker version -f '{{.Server.Experimental}}' 2>/dev/null)" == "true" ]]; then
  SQUASH_PARAM="-Ddocker.squash=true"
fi
mvn -am -pl tests/docker-images/java-test-image -Pcore-modules,-main,integrationTests,docker \
  -Dmaven.test.skip=true -DskipSourceReleaseAssembly=true -Dspotbugs.skip=true -Dlicense.skip=true $SQUASH_PARAM \
  "$@" install