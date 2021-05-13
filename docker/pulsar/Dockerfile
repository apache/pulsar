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

# First create a stage with the Pulsar tarball, the scripts, the python client,
# the cpp client, and the data directory. Then ensure correct file permissions.
FROM busybox as pulsar

ARG PULSAR_TARBALL

ADD ${PULSAR_TARBALL} /
RUN mv /apache-pulsar-* /pulsar

COPY scripts/apply-config-from-env.py /pulsar/bin
COPY scripts/apply-config-from-env-with-prefix.py /pulsar/bin
COPY scripts/gen-yml-from-env.py /pulsar/bin
COPY scripts/generate-zookeeper-config.sh /pulsar/bin
COPY scripts/pulsar-zookeeper-ruok.sh /pulsar/bin
COPY scripts/watch-znode.py /pulsar/bin
COPY scripts/install-pulsar-client-37.sh /pulsar/bin

COPY target/python-client/ /pulsar/pulsar-client

RUN mkdir /pulsar/data

# In order to support running this docker image as a container on OpenShift
# the final image needs to give the root group enough permission.
# The file permissions are maintained when copied into the target image.
RUN chmod -R g=u /pulsar

### Create 2nd stage from OpenJDK image
### and add Python dependencies (for Pulsar functions)

FROM openjdk:8-jdk-slim

# Create the pulsar group and user to make docker container run as a non root user by default
RUN groupadd -g 10001 pulsar
RUN adduser -u 10000 --gid 10001 --disabled-login --disabled-password --gecos '' pulsar

# Install some utilities
RUN apt-get update \
     && apt-get install -y netcat dnsutils less procps iputils-ping \
                 python3.7 python3.7-dev python3-setuptools python3-yaml python3-kazoo \
                 libreadline-gplv2-dev libncursesw5-dev libssl-dev libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev \
                 curl \
     && apt-get clean \
     && rm -rf /var/lib/apt/lists/*

RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
RUN python3.7 get-pip.py

RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 10

# The pulsar directory is staged correctly in the first stage, above.
# The chown and chmod ensure proper permissions for running as a non root user and non root group
# as well as running on OpenShift with a random user that is part of the root group
RUN mkdir /pulsar && chown pulsar:0 /pulsar && chmod g=u /pulsar
COPY --from=pulsar --chown=pulsar:0 /pulsar /pulsar

RUN echo networkaddress.cache.ttl=1 >> $JAVA_HOME/jre/lib/security/java.security

ENV PULSAR_ROOT_LOGGER=INFO,CONSOLE

WORKDIR /pulsar

# This script is intentionally run as the root user to make the dependencies
# available to the root user and the pulsar user
RUN /pulsar/bin/install-pulsar-client-37.sh

# Switch to the pulsar user to ensure container defaults to run as a non root user
USER pulsar
