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

# syntax=docker/dockerfile:1
ARG IMAGE_JDK_MAJOR_VERSION=21

# First create a stage with just the Pulsar tarball and scripts
FROM bitnami/minideb:bookworm AS pulsar

RUN apt-get update && apt-get install -y zip

ARG PULSAR_TARBALL
ADD ${PULSAR_TARBALL} /

RUN mv /apache-pulsar-* /pulsar
RUN rm -rf /pulsar/bin/*.cmd

COPY build-scripts /build-scripts/
RUN /build-scripts/remove-unnecessary-native-binaries.sh

COPY scripts/* /pulsar/bin/

# The final image needs to give the root group sufficient permission for Pulsar components
# to write to specific directories within /pulsar
# The ownership is changed to uid 10000 to allow using a different root group. This is necessary when running the
# container when gid=0 is prohibited. In that case, the container must be run with uid 10000 with
# any group id != 0 (for example 10001).
# The file permissions are preserved when copying files from this builder image to the target image.
RUN for SUBDIRECTORY in conf data download logs instances/deps packages-storage; do \
     mkdir -p /pulsar/$SUBDIRECTORY; \
     chmod -R ug+rwx /pulsar/$SUBDIRECTORY; \
     chown -R 10000:0 /pulsar/$SUBDIRECTORY; \
     done
RUN chmod -R g+rx /pulsar/bin
RUN chmod -R o+rx /pulsar

###  Create one stage to include JVM distribution
FROM bitnami/minideb:bookworm AS jvm
# Install Amazon Corretto JDK
RUN <<EOF
set -e
apt-get update
apt-get install -y wget gpg binutils
wget -O - https://apt.corretto.aws/corretto.key | gpg --dearmor -o /usr/share/keyrings/corretto-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/corretto-keyring.gpg] https://apt.corretto.aws stable main" | tee /etc/apt/sources.list.d/corretto.list
apt-get update
apt-get install -y java-${IMAGE_JDK_MAJOR_VERSION}-amazon-corretto-jdk
EOF

# Use JLink to create a slimmer JDK distribution (see: https://adoptium.net/blog/2021/10/jlink-to-produce-own-runtime/)
# This still includes all JDK modules, though in the future we could compile a list of required modules
RUN <<EOF
set -e
jlink --add-modules ALL-MODULE-PATH --compress zip-9 --no-man-pages --no-header-files --strip-debug --output /opt/jvm
echo networkaddress.cache.ttl=1 >> /opt/jvm/conf/security/java.security
echo networkaddress.cache.negative.ttl=1 >> /opt/jvm/conf/security/java.security
EOF

## Create final stage from minideb image
## and add OpenJDK and Python dependencies (for Pulsar functions)
FROM bitnami/minideb:bookworm
ENV LANG=C.UTF-8

# Install packages
RUN <<EOF
set -e
echo 'Acquire::http::Timeout "30";\nAcquire::ftp::Timeout "30";\nAcquire::Retries "3";' > /etc/apt/apt.conf.d/99timeout_and_retries
apt-get update
apt-get -y dist-upgrade
apt-get -y install --no-install-recommends netcat-openbsd dnsutils less procps iputils-ping curl ca-certificates apt-transport-https procps openssl
apt-get -y install --no-install-recommends python3 python3-kazoo python3-pip python3-yaml
apt-get -y --purge autoremove
apt-get autoclean
apt-get clean
rm -rf /var/lib/apt/lists/*
EOF

# Python dependencies

# The grpcio@1.59.3 is installed by apk, and Pulsar-client@3.4.0 requires grpcio>=1.60.0, which causes the grocio to be reinstalled by pip.
# If pip cannot find the grpcio wheel that the doesn't match the OS, the grpcio will be compiled locally.
# Once https://github.com/apache/pulsar-client-python/pull/211 is released, keep only the pulsar-client[all] and kazoo dependencies, and remove comments.
ARG PULSAR_CLIENT_PYTHON_VERSION
RUN <<EOF
set -e
cat > /tmp/requirements.txt <<REQEOF
#pulsar-client[all]==${PULSAR_CLIENT_PYTHON_VERSION}\n\
pulsar-client==${PULSAR_CLIENT_PYTHON_VERSION}
# Zookeeper
kazoo
# functions
protobuf>=3.6.1,<=3.20.3
grpcio>=1.59.3
apache-bookkeeper-client>=4.16.1
prometheus_client
ratelimit
# avro
fastavro>=1.9.2
REQEOF
pip3 install --break-system-packages --no-cache-dir --only-binary grpcio -r /tmp/requirements.txt
rm /tmp/requirements.txt
EOF

COPY --from=jvm /opt/jvm /opt/jvm
ENV JAVA_HOME=/opt/jvm

# The default is /pulsat/bin and cannot be written.
ENV PULSAR_PID_DIR=/pulsar/logs
ENV PULSAR_ROOT_LOGGER=INFO,CONSOLE
COPY --from=pulsar /pulsar /pulsar
WORKDIR /pulsar
ENV PATH=$PATH:$JAVA_HOME/bin:/pulsar/bin

# The UID must be non-zero. Otherwise, it is arbitrary. No logic should rely on its specific value.
ARG DEFAULT_USERNAME=pulsar
RUN useradd ${DEFAULT_USERNAME} -u 10000 -g 0 --no-create-home --home-dir /pulsar/data
USER 10000