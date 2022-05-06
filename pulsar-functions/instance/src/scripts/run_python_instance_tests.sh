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


# Make sure dependencies are installed
pip3 install mock --user
pip3 install protobuf --user
pip3 install fastavro --user

CUR_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
PULSAR_HOME=$CUR_DIR/../../../../

# run instance tests
PULSAR_HOME=${PULSAR_HOME} PYTHONPATH=${PULSAR_HOME}/pulsar-functions/instance/target/python-instance python3 -m unittest discover -v ${PULSAR_HOME}/pulsar-functions/instance/target/python-instance/tests
