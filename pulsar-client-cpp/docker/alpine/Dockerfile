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

FROM alpine:3.8

RUN apk add --virtual buildpkgs \
        bash \
        git \
        openssh \
        cmake \
        libressl-dev \
        curl-dev \
        protobuf-dev \
        boost-dev \
        boost-python3 \
        gtest-dev \
        make \
        build-base \
        python3-dev \
        zstd-dev \
        snappy-dev \
        libffi-dev

ADD . pulsar/

RUN cd pulsar/pulsar-client-cpp &&  \
	cmake . -DBUILD_TESTS=OFF -DPYTHON_INCLUDE_DIR=/usr/include/python3.6m && make -j8 _pulsar && make install

RUN cd pulsar/pulsar-client-cpp/python && pip3 install wheel && python3 setup.py bdist_wheel && pip3 install --no-cache-dir dist/*.whl

RUN apk del buildpkgs && apk add --no-cache bash python3 boost boost-python3 libcurl protobuf zstd-libs snappy