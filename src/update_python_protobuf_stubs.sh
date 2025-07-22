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

# Create a temporary virtual environment to avoid polluting the global Python environment
tempvenv=$(mktemp -d /tmp/pulsar-venv.XXXXXX)
python3 -m venv $tempvenv
source $tempvenv/bin/activate

# install the required packages for protobuf and grpc
pip install protobuf grpcio grpcio-tools

# Generate Python gRPC and Protobuf stubs from the .proto files

python -m grpc_tools.protoc \
    --proto_path=managed-ledger/src/main/proto \
    --python_out=bin/proto \
    managed-ledger/src/main/proto/MLDataFormats.proto

python -m grpc_tools.protoc \
    --proto_path=pulsar-functions/proto/src/main/proto \
    --python_out=pulsar-functions/instance/src/main/python \
    pulsar-functions/proto/src/main/proto/Function.proto

python -m grpc_tools.protoc \
    --proto_path=pulsar-functions/proto/src/main/proto \
    --python_out=pulsar-functions/instance/src/main/python \
    --grpc_python_out=pulsar-functions/instance/src/main/python \
    pulsar-functions/proto/src/main/proto/InstanceCommunication.proto