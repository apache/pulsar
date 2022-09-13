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

# Build pulsar client library in Centos with tools to build static RPM

FROM centos:7

RUN yum update -y && \
    yum install -y gcc-c++ make cmake git rpm-build \
                python-devel createrepo libstdc++-static.x86_64

# Download and compile boost
RUN curl -O -L https://boostorg.jfrog.io/artifactory/main/release/1.64.0/source/boost_1_64_0.tar.gz && \
    tar xvfz boost_1_64_0.tar.gz && \
    cd /boost_1_64_0 && \
    ./bootstrap.sh --with-libraries=program_options,filesystem,regex,thread,system,python && \
    ./b2 address-model=64 cxxflags=-fPIC link=static threading=multi variant=release install && \
    rm -rf /boost_1_64_0.tar.gz /boost_1_64_0

RUN curl -O -L https://github.com/Kitware/CMake/archive/v3.8.2.tar.gz && \
    tar xvfz v3.8.2.tar.gz && \
    cd CMake-3.8.2 && \
    ./configure && \
    make && make install && \
    rm -rf /v3.8.2.tar.gz /CMake-3.8.2

# Download and copile protoubf
RUN curl -O -L  https://github.com/google/protobuf/releases/download/v3.3.0/protobuf-cpp-3.3.0.tar.gz && \
    tar xvfz protobuf-cpp-3.3.0.tar.gz && \
    cd protobuf-3.3.0/ && \
    CXXFLAGS=-fPIC ./configure && \
    make && make install && ldconfig && \
    rm -rf /protobuf-cpp-3.3.0.tar.gz /protobuf-3.3.0

# ZLib
RUN curl -O -L https://github.com/madler/zlib/archive/v1.2.12.tar.gz && \
    tar xvfz v1.2.12.tar.gz && \
    cd zlib-1.2.12 && \
    CFLAGS="-fPIC -O3" ./configure && \
    make && make install && \
    rm -rf /v1.2.12.tar.gz /zlib-1.2.12

# Zstandard
RUN curl -O -L https://github.com/facebook/zstd/releases/download/v1.3.7/zstd-1.3.7.tar.gz && \
    tar xvfz zstd-1.3.7.tar.gz && \
    cd zstd-1.3.7 && \
    CFLAGS="-fPIC -O3" make -j8 && \
    make install && \
    rm -rf /zstd-1.3.7 /zstd-1.3.7.tar.gz

# Snappy
RUN curl -O -L https://github.com/google/snappy/releases/download/1.1.3/snappy-1.1.3.tar.gz && \
    tar xvfz snappy-1.1.3.tar.gz && \
    cd snappy-1.1.3 && \
    CXXFLAGS="-fPIC -O3" ./configure && \
    make && make install && \
    rm -rf /snappy-1.1.3 /snappy-1.1.3.tar.gz

RUN curl -O -L https://github.com/openssl/openssl/archive/OpenSSL_1_1_1n.tar.gz && \
    tar xvfz OpenSSL_1_1_1n.tar.gz && \
    cd openssl-OpenSSL_1_1_1n/ && \
    ./Configure -fPIC --prefix=/usr/local/ssl/ linux-x86_64 && \
    make && make install && \
    rm -rf /OpenSSL_1_1_1n.tar.gz /openssl-OpenSSL_1_1_1n

ENV LD_LIBRARY_PATH /usr/local/ssl/lib/:

# LibCurl
RUN curl -O -L  https://github.com/curl/curl/releases/download/curl-7_61_0/curl-7.61.0.tar.gz && \
    tar xvfz curl-7.61.0.tar.gz && \
    cd curl-7.61.0 && \
    CFLAGS=-fPIC ./configure --with-ssl=/usr/local/ssl/ && \
    make && make install && \
    rm -rf /curl-7.61.0.tar.gz /curl-7.61.0

ENV OPENSSL_ROOT_DIR /usr/local/ssl/
