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

# Base build image
FROM golang:1.11-alpine AS build_base

# Install some dependencies needed to build the project
RUN apk add bash git gcc g++ libc-dev
WORKDIR /go/src/github.com/apache/pulsar/tests/docker-images/latest-version-image/go-test-schema

# Force the go compiler to use modules
ENV GO111MODULE=on

# We want to populate the module cache based on the go.{mod,sum} files.
COPY go.mod .
COPY go.sum .

#This is the ‘magic’ step that will download all the dependencies that are specified in
# the go.mod and go.sum file.
# Because of how the layer caching system works in Docker, the  go mod download
# command will _ only_ be re-run when the go.mod or go.sum file change
# (or when we add another docker instruction this line)
RUN go mod download

# This image builds the schema server
FROM build_base AS server_builder
# Here we copy the rest of the source code
COPY producer_schema.go .
COPY consumer_schema.go .
COPY schema_util.go .
# And compile the project
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go install -i -gcflags='all=-N -l' .

# Finally we copy the statically compiled Go binary.
COPY --from=server_builder /go/bin/consumer-schema .
COPY --from=server_builder /go/bin/producer-schema .
