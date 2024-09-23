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

# First create a stage with just the Pulsar tarball and scripts
FROM busybox as pulsar

ARG PULSAR_TARBALL

ADD ${PULSAR_TARBALL} /
RUN mv /apache-pulsar-* /pulsar
RUN rm -rf /pulsar/bin/*.cmd

COPY scripts/apply-config-from-env.py /pulsar/bin
COPY scripts/apply-config-from-env-with-prefix.py /pulsar/bin
COPY scripts/gen-yml-from-env.py /pulsar/bin
COPY scripts/generate-zookeeper-config.sh /pulsar/bin
COPY scripts/pulsar-zookeeper-ruok.sh /pulsar/bin
COPY scripts/watch-znode.py /pulsar/bin
COPY scripts/install-pulsar-client.sh /pulsar/bin

# The final image needs to give the root group sufficient permission for Pulsar components
# to write to specific directories within /pulsar
# The ownership is changed to uid 10000 to allow using a different root group. This is necessary when running the
# container when gid=0 is prohibited. In that case, the container must be run with uid 10000 with
# any group id != 0 (for example 10001).
# The file permissions are preserved when copying files from this builder image to the target image.
RUN for SUBDIRECTORY in conf data download logs; do \
     [ -d /pulsar/$SUBDIRECTORY ] || mkdir /pulsar/$SUBDIRECTORY; \
     chmod -R ug+w /pulsar/$SUBDIRECTORY; \
     chown -R 10000:0 /pulsar/$SUBDIRECTORY; \
     done

# Trino writes logs to this directory (at least during tests), so we need to give the process permission
# to create those log directories. This should be removed when Trino is removed.
RUN chmod g+w /pulsar/trino

FROM eclipse-temurin:17 as jre-build

# Create a custom Java runtime
RUN $JAVA_HOME/bin/jlink \
         --add-modules ALL-MODULE-PATH \
         --strip-debug \
         --no-man-pages \
         --no-header-files \
         --compress=2 \
         --output /javaruntime

RUN echo networkaddress.cache.ttl=1 >> /javaruntime/conf/security/java.security
RUN echo networkaddress.cache.negative.ttl=1 >> /javaruntime/conf/security/java.security

### Create 2nd stage from Ubuntu image
### and add OpenJDK and Python dependencies (for Pulsar functions)

FROM ubuntu:22.04

ARG DEBIAN_FRONTEND=noninteractive
ARG UBUNTU_MIRROR=mirror://mirrors.ubuntu.com/mirrors.txt
ARG UBUNTU_SECURITY_MIRROR=http://security.ubuntu.com/ubuntu/
ARG DEFAULT_USERNAME=pulsar

# Install some utilities
RUN sed -i -e "s|http://archive\.ubuntu\.com/ubuntu/|${UBUNTU_MIRROR:-mirror://mirrors.ubuntu.com/mirrors.txt}|g" \
     -e "s|http://security\.ubuntu\.com/ubuntu/|${UBUNTU_SECURITY_MIRROR:-http://security.ubuntu.com/ubuntu/}|g" /etc/apt/sources.list \
     && echo 'Acquire::http::Timeout "30";\nAcquire::ftp::Timeout "30";\nAcquire::Retries "3";' > /etc/apt/apt.conf.d/99timeout_and_retries \
     && apt-get update \
     && apt-get -y dist-upgrade \
     && apt-get -y install netcat dnsutils less procps iputils-ping \
                 curl ca-certificates wget apt-transport-https \
     && apt-get -y install --no-install-recommends python3 python3-kazoo python3-pip \
     && apt-get -y --purge autoremove \
     && apt-get autoclean \
     && apt-get clean \
     && rm -rf /var/lib/apt/lists/* \
     && pip3 install --no-cache-dir --upgrade pip \
     && pip3 install --no-cache-dir pyyaml==6.0.1

# Copy Eclipse Temurin Package
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="$PATH:$JAVA_HOME/bin"
COPY --from=jre-build /javaruntime $JAVA_HOME

# Pulsar currently writes to the below directories, assuming the default configuration.
# Note that number 4 is the reason that pulsar components need write access to the /pulsar directory.
# 1. /pulsar/data - both bookkeepers and zookeepers use this directory
# 2. /pulsar/logs - function workers write to this directory and pulsar-admin initializes this directory
# 3. /pulsar/download - functions write to this directory
# 4. /pulsar - hadoop writes to this directory
RUN mkdir /pulsar && chmod g+w /pulsar

COPY --from=pulsar /pulsar /pulsar
WORKDIR /pulsar

ARG PULSAR_CLIENT_PYTHON_VERSION
ENV PULSAR_CLIENT_PYTHON_VERSION ${PULSAR_CLIENT_PYTHON_VERSION}

# This script is intentionally run as the root user to make the dependencies available for all UIDs.
RUN chmod +x /pulsar/bin/install-pulsar-client.sh
RUN /pulsar/bin/install-pulsar-client.sh

# The UID must be non-zero. Otherwise, it is arbitrary. No logic should rely on its specific value.
RUN useradd ${DEFAULT_USERNAME} -u 10000 -g 0 --no-create-home --home-dir /pulsar/data
USER 10000
