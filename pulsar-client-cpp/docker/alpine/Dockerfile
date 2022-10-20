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


FROM alpine:3.11

RUN apk update \
    && apk upgrade \
    && apk add --no-cache \
    cmake make build-base curl \
    python3-dev \
    libffi-dev \
    perl linux-headers bash \
    alpine-sdk

####################################
# These dependencies can be found in Ubuntu but they're not compiled with -fPIC,
# so they cannot be statically linked into a shared library
####################################

# Download and compile boost
RUN curl -O -L https://boostorg.jfrog.io/artifactory/main/release/1.72.0/source/boost_1_72_0.tar.gz && \
    tar xfz boost_1_72_0.tar.gz && \
    cd /boost_1_72_0 && \
    ./bootstrap.sh --with-libraries=program_options,regex,system,python --with-python=/usr/bin/python3 && \
    ./b2  cxxflags=-fPIC link=static threading=multi variant=release install && \
    rm -rf /boost_1_72_0.tar.gz /boost_1_72_0

# ZLib
RUN curl -O -L https://zlib.net/fossils/zlib-1.2.13.tar.gz  && \
    tar xfz zlib-1.2.13.tar.gz && \
    cd zlib-1.2.13 && \
    CFLAGS="-fPIC -O3" ./configure && \
    make -j4 && make install && \
    rm -rf /zlib-1.2.13.tar.gz /zlib-1.2.13

# Compile OpenSSL
RUN curl -O -L https://github.com/openssl/openssl/archive/OpenSSL_1_1_1n.tar.gz && \
   tar xfz OpenSSL_1_1_1n.tar.gz && \
   cd openssl-OpenSSL_1_1_1n/ && \
   ./Configure -fPIC no-shared linux-x86_64 && \
   make -j8 && make install && \
   rm -rf /OpenSSL_1_1_1n.tar.gz /openssl-OpenSSL_1_1_1n

ENV LD_LIBRARY_PATH /usr/local/ssl/lib/:

# Download and copile protoubf
RUN curl -O -L  https://github.com/google/protobuf/releases/download/v3.11.3/protobuf-cpp-3.11.3.tar.gz && \
    tar xvfz protobuf-cpp-3.11.3.tar.gz && \
    cd protobuf-3.11.3/ && \
    CXXFLAGS=-fPIC ./configure && \
    make -j8 && make install && \
    rm -rf /protobuf-cpp-3.11.3.tar.gz /protobuf-3.11.3

# LibCurl
RUN curl -O -L  https://github.com/curl/curl/releases/download/curl-7_61_0/curl-7.61.0.tar.gz && \
    tar xvfz curl-7.61.0.tar.gz && \
    cd curl-7.61.0 && \
    CFLAGS=-fPIC ./configure && \
    make -j8 && make install && \
    rm -rf /curl-7.61.0.tar.gz /curl-7.61.0

# Zstandard
RUN curl -O -L https://github.com/facebook/zstd/releases/download/v1.3.7/zstd-1.3.7.tar.gz && \
    tar xvfz zstd-1.3.7.tar.gz && \
    cd zstd-1.3.7 && \
    CFLAGS="-fPIC -O3" make -j8 && \
    make install && \
    rm -rf /zstd-1.3.7 /zstd-1.3.7.tar.gz

# Snappy
RUN curl -L https://github.com/google/snappy/archive/1.1.8.tar.gz --output snappy-1.1.8.tar.gz && \
    tar xvfz snappy-1.1.8.tar.gz && \
    cd snappy-1.1.8 && \
    CXXFLAGS="-fPIC -O3" cmake . && \
    make -j8 && make install && \
    rm -rf /snappy-1.1.8 /snappy-1.1.8.tar.gz


RUN pip3 install wheel twine

RUN adduser pulsar -D && \
    addgroup pulsar abuild
