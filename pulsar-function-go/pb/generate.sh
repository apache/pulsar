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

# Bash script to automate the generation of the api
# package.
#
# It uses the .proto files included in Pulsar's source
# to compile the Go code capable of encoding/decoding the
# wire format used by Pulsar brokers.
#
# Requirements:
#  * protoc and protoc-gen-go are installed. See: https://github.com/golang/protobuf
#  * The Pulsar project is checked out somewhere on the file system
#    in order to source the .proto files
echo "generate pulsar go function protobuf code..."

set -euo pipefail

pkg="api"

defaultPulsarSrc="${HOME}/github.com/apache/pulsar"

help="usage: ${0} <path to Pulsar repo (default \"${defaultPulsarSrc}\")>"

pulsarSrc="${1-${defaultPulsarSrc}}"
if [ ! -d "${pulsarSrc}" ]; then
	echo "error: Pulsar source is not a directory: ${pulsarSrc}"
	echo "${help}"
	exit 1
fi
protoDefinitions="${pulsarSrc}/pulsar-functions/proto/src/main/proto"
if [ ! -d "${protoDefinitions}" ]; then
	echo "error: Proto definitions directory not found: ${protoDefinitions}"
	echo "${help}"
	exit 1
fi
protoFiles="${protoDefinitions}/*.proto"

protoc \
	--go_out=import_path=${pkg},plugins=grpc:. \
	--proto_path="${protoDefinitions}" ${protoFiles}

pulsarGitRev=$(git -C ${pulsarSrc} rev-parse HEAD)
pulsarGitTag=$(git -C ${pulsarSrc} describe --tags HEAD)

# Generate godoc describing this package and the
# git sha it was created from
cat <<EOF > doc.go
// Package ${pkg} provides the protocol buffer messages that Pulsar
// uses for the client/broker wire protocol.
// See "Pulsar binary protocol specification" for more information.
// https://pulsar.incubator.apache.org/docs/latest/project/BinaryProtocol/
//
// The protocol definition files are part of the main Pulsar source,
// located within the Pulsar repository at:
// https://github.com/apache/pulsar/tree/master/pulsar-functions/proto/src/main/proto
//
// The generated Go code was created from the source Pulsar files at git:
//    tag:      ${pulsarGitTag}
//    revision: ${pulsarGitRev}
//
// Files generated by the protoc-gen-go program should not be modified.
package ${pkg}
EOF
