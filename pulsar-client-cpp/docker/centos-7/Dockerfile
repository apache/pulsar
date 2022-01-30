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


FROM centos:7.6.1810

RUN yum install -y gcc gcc-c++ make \
  protobuf-devel.x86_64 \
  libcurl-devel openssl-devel \
  boost boost-devel

RUN curl -O -L https://github.com/protocolbuffers/protobuf/releases/download/v3.17.3/protobuf-cpp-3.17.3.tar.gz && \
    tar xfz protobuf-cpp-3.17.3.tar.gz && \
    cd protobuf-3.17.3/ && \
    CXXFLAGS=-fPIC ./configure && \
    make -j8 && make install && \
    cd .. && rm -rf protobuf-3.17.3/ protobuf-cpp-3.17.3.tar.gz
RUN mkdir -p /opt/cmake
WORKDIR /opt/cmake
RUN curl -L -O https://cmake.org/files/v3.4/cmake-3.4.0-Linux-x86_64.tar.gz \
  && tar zxf cmake-3.4.0-Linux-x86_64.tar.gz

# googletest
RUN curl -O -L https://github.com/google/googletest/archive/refs/tags/release-1.10.0.tar.gz \
  && tar zxf release-1.10.0.tar.gz \
  && cd googletest-release-1.10.0 \
  && mkdir build && cd build \
  && /opt/cmake/cmake-3.4.0-Linux-x86_64/bin/cmake .. && make install
