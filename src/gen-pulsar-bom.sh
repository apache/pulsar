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

# Script to generate a BOM using the following approach:
#
# - do a local publish to get list of modules for BOM
# - for each published module add dependency entry
# - replace the current <dependencyManagement> section with
#   entries gathered in previous step

set -e

# Determine top level project directory
SRC_DIR=$(dirname "$0")
ROOT_DIR=`cd ${SRC_DIR}/..; pwd`
LOCAL_DEPLOY_DIR=${ROOT_DIR}/target/staging-deploy
PULSAR_BOM_DIR=${ROOT_DIR}/pulsar-bom

pushd ${ROOT_DIR} > /dev/null
echo "Performing local publish to determine modules for BOM."
rm -rf ${LOCAL_DEPLOY_DIR}
./mvnw deploy -DaltDeploymentRepository=local::default::file:${LOCAL_DEPLOY_DIR} -DskipTests
./mvnw deploy -DaltDeploymentRepository=local::default::file:${LOCAL_DEPLOY_DIR} -DskipTests -f tests/pom.xml -pl org.apache.pulsar.tests:tests-parent,org.apache.pulsar.tests:integration
echo "$(ls ${LOCAL_DEPLOY_DIR}/org/apache/pulsar | wc -l) modules locally published to ${LOCAL_DEPLOY_DIR}."
popd > /dev/null

DEPENDENCY_MGMT_PRE=$(cat <<-END
<dependencyManagement>
    <dependencies>
END
)
DEPENDENCY_BLOCK=$(cat <<-END
      <dependency>
        <groupId>org.apache.pulsar</groupId>
        <artifactId>@ARTIFACT_ID@</artifactId>
        <version>\${project.version}</version>
      </dependency>
END
)
DEPENDENCY_MGMT_POST=$(cat <<-END
    </dependencies>
  </dependencyManagement>
</project>
END
)
ALL_DEPS=""
NEWLINE=$'\n'

pushd ${LOCAL_DEPLOY_DIR}/org/apache/pulsar/ > /dev/null
echo "Traversing locally published modules."
for f in */
do
  ARTIFACT_ID="${f%/}"
  DEPENDENCY=$(echo "${DEPENDENCY_BLOCK/@ARTIFACT_ID@/$ARTIFACT_ID}")
  if [ "${ARTIFACT_ID}" = "pulsar-bom" ]; then
      continue
  elif [ -z "$ALL_DEPS" ]; then
      ALL_DEPS="$DEPENDENCY"
  else
      ALL_DEPS="$ALL_DEPS$NEWLINE$DEPENDENCY"
  fi
done
popd > /dev/null

POM_XML=$(<${PULSAR_BOM_DIR}/pom.xml)
POM_XML=$(echo "${POM_XML%%<dependencyManagement>*}")
echo "$POM_XML$DEPENDENCY_MGMT_PRE$NEWLINE$ALL_DEPS$NEWLINE$DEPENDENCY_MGMT_POST" > ${PULSAR_BOM_DIR}/pom.xml

echo "Created BOM ${PULSAR_BOM_DIR}/pom.xml."
echo "You must manually inspect changes and submit a PR with the changes."
