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

ARG ALPINE_VERSION=3.20
ARG IMAGE_JDK_MAJOR_VERSION=21

FROM amazoncorretto:${IMAGE_JDK_MAJOR_VERSION}-alpine AS kpl-build

# Install build dependencies
RUN apk update && apk add --no-cache \
    git \
    binutils \
    coreutils \
    alpine-sdk \
    util-linux \
    cmake \
    autoconf \
    automake \
    libtool \
    maven \
    curl \
    bash \
    tar \
    libuuid \
    linux-headers \
    zlib \
    zlib-dev \
    perl \
    wget \
    boost-dev \
    protobuf-dev \
    openssl-dev \
    curl-dev \
    build-base \
    util-linux-dev \
    g++ \
    make

ENV LANG=C.UTF-8
ENV KINESIS_PRODUCER_LIB_VERSION=0.15.12

# Clone and prepare KPL
RUN mkdir /build && \
    cd /build && \
    git clone --depth 1 --branch v${KINESIS_PRODUCER_LIB_VERSION} https://github.com/awslabs/amazon-kinesis-producer && \
    cd amazon-kinesis-producer