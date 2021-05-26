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

FROM openjdk:11-jdk-slim

RUN groupadd -g 10001 pulsar
RUN adduser -u 10000 --gid 10001 --disabled-login --disabled-password --gecos '' pulsar

ARG PULSAR_TARBALL=target/pulsar-server-distribution-bin.tar.gz
ADD ${PULSAR_TARBALL} /
RUN mv /apache-pulsar-* /pulsar
RUN chown -R root:root /pulsar

COPY target/scripts /pulsar/bin
RUN chmod a+rx /pulsar/bin/*

RUN echo networkaddress.cache.ttl=1 >> $JAVA_HOME/conf/security/java.security

WORKDIR /pulsar

RUN apt-get update

# /pulsar/bin/watch-znode.py requires python3-kazoo
# /pulsar/bin/pulsar-managed-ledger-admin requires python3-protobuf
RUN apt-get install -y python3-kazoo python3-protobuf
# use python3 for watch-znode.py and pulsar-managed-ledger-admin
RUN sed -i '1 s/.*/#!\/usr\/bin\/python3/' /pulsar/bin/watch-znode.py /pulsar/bin/pulsar-managed-ledger-admin

# required by gen-yml-from-env.py
RUN apt-get install -y python-yaml

RUN apt-get install -y supervisor procps curl less netcat dnsutils iputils-ping


RUN mkdir -p /var/log/pulsar \
    && mkdir -p /var/run/supervisor/ \
    && mkdir -p /pulsar/ssl

COPY target/conf /etc/supervisord/conf.d/
RUN mv /etc/supervisord/conf.d/supervisord.conf /etc/supervisord.conf

COPY target/ssl /pulsar/ssl/

COPY target/java-test-functions.jar /pulsar/examples/

ENV PULSAR_ROOT_LOGGER=INFO,CONSOLE

RUN chown -R pulsar:0 /pulsar && chmod -R g=u /pulsar

# cleanup
RUN apt-get clean \
    && rm -rf /var/lib/apt/lists/*