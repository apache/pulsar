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

FROM ubuntu:16.04

RUN apt-get update
RUN apt-get install -y maven tig g++ cmake libssl-dev libcurl4-openssl-dev \
                liblog4cxx-dev libprotobuf-dev libboost-all-dev  libgtest-dev \
                libjsoncpp-dev libxml2-utils protobuf-compiler valgrind

# Compile and install gtest
RUN cd /usr/src/gtest && cmake . && make && cp libgtest.a /usr/lib

# Include gtest parallel to speed up unit tests
RUN git clone https://github.com/google/gtest-parallel.git

ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64
