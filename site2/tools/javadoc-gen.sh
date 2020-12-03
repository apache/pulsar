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


ROOT_DIR=$(git rev-parse --show-toplevel)
VERSION=`${ROOT_DIR}/src/get-project-version.py`
DEST_DIR=$ROOT_DIR/generated-site

(
  cd $ROOT_DIR

  # Java client
  mkdir -p $DEST_DIR/api/client/${VERSION}
  mvn -pl pulsar-client-api javadoc:javadoc
  cp -r pulsar-client-api/target/site/apidocs/* $DEST_DIR/api/client/${VERSION}/


  # Java admin
  mkdir -p $DEST_DIR/api/admin/${VERSION}
  mvn -pl pulsar-client-admin javadoc:javadoc
  cp -r pulsar-client-admin/target/site/apidocs/* $DEST_DIR/api/admin/${VERSION}/

  # Pulsar Functions Java SDK
  mkdir -p $DEST_DIR/api/pulsar-functions/${VERSION}
  cd pulsar-functions
  mvn -pl api-java javadoc:javadoc
  cd ..
  cp -r pulsar-functions/api-java/target/site/apidocs/* $DEST_DIR/api/pulsar-functions/${VERSION}/

  # Broker
  mkdir -p $DEST_DIR/api/pulsar-broker/${VERSION}
  mvn -pl pulsar-broker javadoc:javadoc
  cp -r pulsar-broker/target/site/apidocs/* $DEST_DIR/api/pulsar-broker/${VERSION}/

) || true

# The "|| true" is present here to keep this script from failing due to
# Javadoc errors
