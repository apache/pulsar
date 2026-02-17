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

set -x

# Python dependencies
# The pinned grpcio and protobuf versions should be compatible with the generated Protobuf and gRPC stubs used
# in Pulsar Functions Python runtime. You should also update the grpcio version in src/update_python_protobuf_stubs.sh
# and regenerate the Python stubs if you change the grpcio version here. Please see
# pulsar-functions/instance/src/main/python/README.md for more details.
pip3 install --no-cache-dir --only-binary \
  grpcio==1.78.0 \
  protobuf==6.33.5 \
  pulsar-client[all]==${PULSAR_CLIENT_PYTHON_VERSION}
