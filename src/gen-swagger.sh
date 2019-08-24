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

PULSAR_PATH=$(git rev-parse --show-toplevel)

cd $PULSAR_PATH

git checkout v2.3.0
echo "Generating swagger json file for 2.3.0 ..."
mvn -am -pl pulsar-broker install -DskipTests -Pswagger
echo "Swagger json file is generated for 2.3.0"

cp pulsar-broker/target/docs/swagger*.json site2/website/static/swagger/2.3.0/
echo "Copied swagger json file for 2.3.0."

git checkout v2.3.1
echo "Generating swagger json file for 2.3.1 ..."
mvn -am -pl pulsar-broker install -DskipTests -Pswagger
echo "Swagger json file is generated for 2.3.1"

cp pulsar-broker/target/docs/swagger*.json site2/website/static/swagger/2.3.1/
echo "Copied swagger json file for 2.3.1."

git checkout v2.3.2
echo "Generating swagger json file for 2.3.2 ..."
mvn -am -pl pulsar-broker install -DskipTests -Pswagger
echo "Swagger json file is generated for 2.3.2"

cp pulsar-broker/target/docs/swagger*.json site2/website/static/swagger/2.3.2/
echo "Copied swagger json file for 2.3.2."

git checkout v2.4.0
echo "Generating swagger json file for 2.4.0 ..."
mvn -am -pl pulsar-broker install -DskipTests -Pswagger
echo "Swagger json file is generated for 2.4.0."

cp pulsar-broker/target/docs/swagger*.json site2/website/static/swagger/2.4.0/
echo "Copied swagger json file for 2.4.0."

git checkout master
echo "Generating swagger json file for master ..."
mvn -am -pl pulsar-broker install -DskipTests -Pswagger
echo "Swagger json file is generated for master."

cp pulsar-broker/target/docs/swagger*.json site2/website/static/swagger/master/
echo "Copied swagger json file for master."

