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

ARG ALPINE_VERSION=3.21

# Builds an Alpine image with kinesis_producer compiled for Alpine Linux / musl

# Build stage
FROM alpine:$ALPINE_VERSION AS kinesis-producer-build
ENV KINESIS_PRODUCER_LIB_VERSION=1.0.4

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
    openssl-dev \
    curl-dev \
    build-base \
    util-linux-dev \
    g++ \
    make \
    upx

ENV LANG=C.UTF-8

RUN mkdir /build
COPY kinesis_producer_alpine.patch /build/

# Clone KPL and copy build script
RUN cd /build && \
    git clone --depth 1 --single-branch --branch v${KINESIS_PRODUCER_LIB_VERSION} https://github.com/awslabs/amazon-kinesis-producer && \
    cd amazon-kinesis-producer && \
    git apply ../kinesis_producer_alpine.patch

# Copy and execute build script
COPY build-alpine.sh /build/
RUN chmod +x /build/build-alpine.sh
RUN /build/build-alpine.sh

# Final stage
FROM alpine:$ALPINE_VERSION
COPY --from=kinesis-producer-build /opt/amazon-kinesis-producer /opt/amazon-kinesis-producer
RUN apk update && apk add --no-cache \
    brotli-libs \
    c-ares \
    libcrypto3 \
    libcurl \
    libgcc \
    libidn2 \
    libpsl \
    libssl3 \
    libunistring \
    nghttp2-libs \
    zlib \
    zstd-libs \
    libuuid
WORKDIR /opt/amazon-kinesis-producer/bin
