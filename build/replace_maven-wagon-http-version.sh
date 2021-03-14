#!/bin/bash -xe
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

# patches installed maven version to get fix for https://issues.apache.org/jira/browse/HTTPCORE-634

MAVEN_HOME=$(mvn -v |grep 'Maven home:' | awk '{ print $3 }')
if [ -d "$MAVEN_HOME" ]; then
  cd "$MAVEN_HOME/lib"
  rm wagon-http-*-shaded.jar
  curl -O https://repo1.maven.org/maven2/org/apache/maven/wagon/wagon-http/3.4.3/wagon-http-3.4.3-shaded.jar
fi