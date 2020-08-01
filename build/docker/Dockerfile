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

# prepare the directory for pulsar related files
RUN mkdir /pulsar
ADD protobuf.patch /pulsar

RUN apt-get update && \
    apt-get install -y tig g++ cmake libssl-dev libcurl4-openssl-dev \
                liblog4cxx-dev libprotobuf-dev libboost-all-dev google-mock libgtest-dev \
                libjsoncpp-dev libxml2-utils protobuf-compiler wget \
                curl doxygen openjdk-8-jdk-headless clang-format-5.0 \
                gnupg2 golang-1.10-go zip unzip libzstd-dev libsnappy-dev

# Compile and install gtest
RUN cd /usr/src/gtest && cmake . && make && cp libgtest.a /usr/lib

# Compile and install google-mock
RUN cd /usr/src/gmock && cmake . && make && cp libgmock.a /usr/lib

# Include gtest parallel to speed up unit tests
RUN git clone https://github.com/google/gtest-parallel.git

ENV JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64

## Website build dependencies

# Install Ruby-2.4.1
RUN apt-get install -y
RUN gpg2 --keyserver hkp://keys.gnupg.net --recv-keys 409B6B1796C275462A1703113804BB82D39DC0E3 7D2BAF1CF37B13E2069D6956105BD0E739499BDB && \
    (curl -sSL https://get.rvm.io | bash -s stable)
ENV PATH "$PATH:/usr/local/rvm/bin"
RUN rvm install 2.4.1

# Install nodejs and yarn
RUN curl -sL https://deb.nodesource.com/setup_10.x | bash -
RUN apt-get install -y nodejs
RUN curl -sS https://dl.yarnpkg.com/debian/pubkey.gpg | apt-key add -
RUN echo "deb https://dl.yarnpkg.com/debian/ stable main" | tee /etc/apt/sources.list.d/yarn.list
RUN apt-get update && apt-get install yarn

# Install crowdin
RUN wget https://artifacts.crowdin.com/repo/deb/crowdin.deb -O crowdin.deb
RUN dpkg -i crowdin.deb

# Install PIP and PDoc
RUN wget https://bootstrap.pypa.io/get-pip.py && python get-pip.py
RUN pip install pdoc

# Install Protobuf doc generator (requires Go)
ENV GOPATH "$HOME/go"
ENV PATH "/usr/lib/go-1.10/bin:$GOPATH/bin:$PATH"
RUN go get -u github.com/pseudomuto/protoc-gen-doc/cmd/protoc-gen-doc

# Build the patched protoc
RUN git clone https://github.com/google/protobuf.git /pulsar/protobuf && \
    cd /pulsar/protobuf && \
    git checkout v2.4.1 && \
    patch -p1 < /pulsar/protobuf.patch && \
    autoreconf --install && \
    ./configure && \
    make

# Installation
ARG MAVEN_FILENAME="apache-maven-${MAVEN_VERSION}-bin.tar.gz"
ARG MAVEN_URL="http://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/${MAVEN_FILENAME}"
ARG MAVEN_TMP="/tmp/${MAVEN_FILENAME}"
RUN wget --no-verbose -O ${MAVEN_TMP} ${MAVEN_URL} 

# Cleanup
RUN tar xzf ${MAVEN_TMP}  -C /opt/ \
        && ln -s /opt/apache-maven-${MAVEN_VERSION} ${MAVEN_HOME} \
        && ln -s ${MAVEN_HOME}/bin/mvn /usr/local/bin 

RUN unset MAVEN_VERSION
