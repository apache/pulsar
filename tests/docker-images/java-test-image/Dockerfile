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

FROM ubuntu:22.04

RUN groupadd -g 10001 pulsar
RUN adduser -u 10000 --gid 10001 --disabled-login --disabled-password --gecos '' pulsar

ARG PULSAR_TARBALL=target/pulsar-server-distribution-bin.tar.gz
ADD ${PULSAR_TARBALL} /
RUN mv /apache-pulsar-* /pulsar
RUN chown -R root:root /pulsar

COPY target/scripts /pulsar/bin
RUN chmod a+rx /pulsar/bin/*

WORKDIR /pulsar

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
     && apt-get -y dist-upgrade \
     && apt-get -y install wget apt-transport-https

# Install Eclipse Temurin Package
RUN mkdir -p /etc/apt/keyrings \
     && wget -O - https://packages.adoptium.net/artifactory/api/gpg/key/public | tee /etc/apt/keyrings/adoptium.asc \
     && echo "deb [signed-by=/etc/apt/keyrings/adoptium.asc] https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" | tee /etc/apt/sources.list.d/adoptium.list \
     && apt-get update \
     && apt-get -y dist-upgrade \
     && apt-get -y install temurin-17-jdk \
     && export ARCH=$(uname -m | sed -r 's/aarch64/arm64/g' |  awk '!/arm64/{$0="amd64"}1') \
     && echo networkaddress.cache.ttl=1 >> /usr/lib/jvm/temurin-17-jdk-$ARCH/conf/security/java.security

# /pulsar/bin/watch-znode.py requires python3-kazoo
# /pulsar/bin/pulsar-managed-ledger-admin requires python3-protobuf
# gen-yml-from-env.py requires python3-yaml
# make python3 the default
RUN apt-get install -y python3-kazoo python3-protobuf python3-yaml \
    && update-alternatives --install /usr/bin/python python /usr/bin/python3 10

RUN apt-get install -y supervisor procps curl less netcat dnsutils iputils-ping

RUN mkdir -p /var/log/pulsar \
    && mkdir -p /var/run/supervisor/ \
    && mkdir -p /pulsar/certificate-authority

COPY target/conf /etc/supervisord/conf.d/
RUN mv /etc/supervisord/conf.d/supervisord.conf /etc/supervisord.conf

# TLS certificates
COPY target/certificate-authority /pulsar/certificate-authority/

COPY target/java-test-functions.jar /pulsar/examples/

ENV PULSAR_ROOT_LOGGER=INFO,CONSOLE

RUN chown -R pulsar:0 /pulsar && chmod -R g=u /pulsar

# cleanup
RUN apt-get -y --purge autoremove \
    && apt-get autoclean \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
