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

# This script generates Python gRPC and Protobuf stubs from the .proto files
# Set the version of PYTHON_GRPCIO_VERSION to the version which matches the version of grpcio in the Dockerfile
PYTHON_GRPCIO_VERSION=${PYTHON_GRPCIO_VERSION:-"1.73.1"}

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

# Create a temporary virtual environment to avoid polluting the global Python environment
tempvenv=$(mktemp -d /tmp/pulsar-venv.XXXXXX)
python3 -m venv $tempvenv
source $tempvenv/bin/activate

# install the required packages for protobuf and grpc
echo "Installing grpc-tools $PYTHON_GRPCIO_VERSION..."
python3 -m pip install grpcio-tools==$PYTHON_GRPCIO_VERSION

echo "libprotoc library included in grpcio-tools will be used:"
python3 -m grpc_tools.protoc --version
echo "The compatible matching protobuf package version in Python is prefixed with '6.'"
echo "Ensure that you are using a compatible version of the protobuf package such as 6.$(python3 -m grpc_tools.protoc --version | awk '{print $2}') (or a matching patch version)."

cd $SCRIPT_DIR/..
echo "Generating Python gRPC and Protobuf stubs from the .proto files..."

# Generate Python gRPC and Protobuf stubs from the .proto files

# Generate stubs for Function.proto and InstanceCommunication.proto, used for Pulsar Functions Python runtime
python3 -m grpc_tools.protoc \
    --proto_path=pulsar-functions/proto/src/main/proto \
    --python_out=pulsar-functions/instance/src/main/python \
    pulsar-functions/proto/src/main/proto/Function.proto

# Remove the strict version checking in the generated file
sed -i '/^_runtime_version\.ValidateProtobufRuntimeVersion($/,/^)$/d' \
  pulsar-functions/instance/src/main/python/Function_pb2.py

python3 -m grpc_tools.protoc \
    --proto_path=pulsar-functions/proto/src/main/proto \
    --python_out=pulsar-functions/instance/src/main/python \
    --grpc_python_out=pulsar-functions/instance/src/main/python \
    pulsar-functions/proto/src/main/proto/InstanceCommunication.proto

# Remove the strict version checking in the generated file
sed -i '/^_runtime_version\.ValidateProtobufRuntimeVersion($/,/^)$/d' \
  pulsar-functions/instance/src/main/python/InstanceCommunication_pb2.py
sed -i '/^_version_not_supported = False$/,/^    )$/d' \
  pulsar-functions/instance/src/main/python/InstanceCommunication_pb2_grpc.py

# Generate stubs for MLDataFormats.proto, used for managed-ledger python scripts
python3 -m grpc_tools.protoc \
    --proto_path=managed-ledger/src/main/proto \
    --python_out=bin/proto \
    managed-ledger/src/main/proto/MLDataFormats.proto

# Remove the strict version checking in the generated file
sed -i '/^_runtime_version\.ValidateProtobufRuntimeVersion($/,/^)$/d' \
  bin/proto/MLDataFormats_pb2.py

echo "Python gRPC and Protobuf stubs generated successfully."

# Deactivate and remove the temporary virtual environment
deactivate
rm -rf $tempvenv