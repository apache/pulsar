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

ARG ALPINE_VERSION=3.20
ARG IMAGE_JDK_MAJOR_VERSION=21

# First create a stage with just the Pulsar tarball and scripts
FROM alpine:$ALPINE_VERSION as pulsar

RUN apk add zip

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

# Enable snappy-java to use system lib
RUN echo 'OPTS="$OPTS -Dorg.xerial.snappy.use.systemlib=true"' >> /pulsar/conf/bkenv.sh

###  Create one stage to include JVM distribution
FROM amazoncorretto:${IMAGE_JDK_MAJOR_VERSION}-alpine AS jvm

RUN apk add --no-cache binutils

# Use JLink to create a slimmer JDK distribution (see: https://adoptium.net/blog/2021/10/jlink-to-produce-own-runtime/)
# This still includes all JDK modules, though in the future we could compile a list of required modules
RUN /usr/lib/jvm/default-jvm/bin/jlink --add-modules ALL-MODULE-PATH --compress zip-9 --no-man-pages --no-header-files --strip-debug --output /opt/jvm
RUN echo networkaddress.cache.ttl=1 >> /opt/jvm/conf/security/java.security
RUN echo networkaddress.cache.negative.ttl=1 >> /opt/jvm/conf/security/java.security

## Create one stage to include snappy-java native lib
# Fix the issue when using snappy-java in x86 arch alpine
# See https://github.com/xerial/snappy-java/issues/181 https://github.com/xerial/snappy-java/issues/579
# We need to ensure that the version of the native library matches the version of snappy-java imported via Maven
FROM alpine:$ALPINE_VERSION AS snappy-java

ARG SNAPPY_VERSION
RUN apk add git alpine-sdk util-linux cmake autoconf automake libtool openjdk17 maven curl bash tar
ENV JAVA_HOME=/usr
RUN curl -Ls https://github.com/xerial/snappy-java/archive/refs/tags/v$SNAPPY_VERSION.tar.gz | tar zxf - && cd snappy-java-$SNAPPY_VERSION && make clean-native native
FROM apachepulsar/glibc-base:2.38 as glibc

## Create final stage from Alpine image
## and add OpenJDK and Python dependencies (for Pulsar functions)
FROM alpine:$ALPINE_VERSION
ENV LANG C.UTF-8

# Install some utilities, some are required by Pulsar scripts
RUN apk add --no-cache \
            bash \
            python3 \
            py3-pip \
            py3-grpcio \
            py3-yaml \
            gcompat \
            ca-certificates \
            procps \
            curl \
            bind-tools \
            openssl \
            coreutils

# Upgrade all packages to get latest versions with security fixes
RUN apk upgrade --no-cache

# Python dependencies

# The grpcio@1.59.3 is installed by apk, and Pulsar-client@3.4.0 requires grpcio>=1.60.0, which causes the grocio to be reinstalled by pip.
# If pip cannot find the grpcio wheel that the doesn't match the OS, the grpcio will be compiled locally.
# Once https://github.com/apache/pulsar-client-python/pull/211 is released, keep only the pulsar-client[all] and kazoo dependencies, and remove comments.
ARG PULSAR_CLIENT_PYTHON_VERSION
RUN echo -e "\
#pulsar-client[all]==${PULSAR_CLIENT_PYTHON_VERSION}\n\
pulsar-client==${PULSAR_CLIENT_PYTHON_VERSION}\n\
# Zookeeper\n\
kazoo\n\
# functions\n\
protobuf>=3.6.1,<=3.20.3\n\
grpcio>=1.59.3\n\
apache-bookkeeper-client>=4.16.1\n\
prometheus_client\n\
ratelimit\n\
# avro\n\
fastavro>=1.9.2\n\
" > /requirements.txt

RUN pip3 install --break-system-packages --no-cache-dir --only-binary grpcio -r /requirements.txt
RUN rm /requirements.txt

# Install GLibc compatibility library
COPY --from=glibc /root/packages /root/packages
RUN apk add --allow-untrusted --force-overwrite /root/packages/glibc-*.apk

COPY --from=jvm /opt/jvm /opt/jvm
ENV JAVA_HOME=/opt/jvm

COPY --from=snappy-java /tmp/libsnappyjava.so /usr/lib/libsnappyjava.so

# The default is /pulsat/bin and cannot be written.
ENV PULSAR_PID_DIR=/pulsar/logs

ENV PULSAR_ROOT_LOGGER=INFO,CONSOLE

COPY --from=pulsar /pulsar /pulsar

WORKDIR /pulsar
ENV PATH=$PATH:$JAVA_HOME/bin:/pulsar/bin
# Use musl libc library for RocksDB
ENV ROCKSDB_MUSL_LIBC=true

# The UID must be non-zero. Otherwise, it is arbitrary. No logic should rely on its specific value.
ARG DEFAULT_USERNAME=pulsar
RUN adduser ${DEFAULT_USERNAME} -u 10000 -G root -D -H -h /pulsar/data
USER 10000
