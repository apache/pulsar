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
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

# Create a temporary virtual environment to avoid polluting the global Python environment
tempvenv=$(mktemp -d /tmp/pulsar-venv.XXXXXX)
python3 -m venv $tempvenv
source $tempvenv/bin/activate

# install the required packages for protobuf and grpc
python3 -m pip install grpcio-tools

cd $SCRIPT_DIR/..
echo "Generating Python gRPC and Protobuf stubs from the .proto files..."

# Generate Python gRPC and Protobuf stubs from the .proto files
python3 -m grpc_tools.protoc \
    --proto_path=managed-ledger/src/main/proto \
    --python_out=bin/proto \
    managed-ledger/src/main/proto/MLDataFormats.proto
sed -i '/^_runtime_version\.ValidateProtobufRuntimeVersion($/,/^)$/d' \
  bin/proto/MLDataFormats_pb2.py

python3 -m grpc_tools.protoc \
    --proto_path=pulsar-functions/proto/src/main/proto \
    --python_out=pulsar-functions/instance/src/main/python \
    pulsar-functions/proto/src/main/proto/Function.proto
sed -i '/^_runtime_version\.ValidateProtobufRuntimeVersion($/,/^)$/d' \
  pulsar-functions/instance/src/main/python/Function_pb2.py

python3 -m grpc_tools.protoc \
    --proto_path=pulsar-functions/proto/src/main/proto \
    --python_out=pulsar-functions/instance/src/main/python \
    --grpc_python_out=pulsar-functions/instance/src/main/python \
    pulsar-functions/proto/src/main/proto/InstanceCommunication.proto
sed -i '/^_runtime_version\.ValidateProtobufRuntimeVersion($/,/^)$/d' \
  pulsar-functions/instance/src/main/python/InstanceCommunication_pb2.py
sed -i '/^_version_not_supported = False$/,/^    )$/d' \
  pulsar-functions/instance/src/main/python/InstanceCommunication_pb2_grpc.py

echo "Python gRPC and Protobuf stubs generated successfully."