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
FROM alpine as pulsar

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

## Create 2nd stage to build the Python dependencies
## Since it needs to have GCC available, we're doing it in a different layer
FROM alpine:3.19 AS python-deps

RUN apk add --no-cache \
       bash \
       python3-dev \
       g++ \
       musl-dev \
       libffi-dev \
       py3-pip \
       py3-grpcio \
       py3-yaml

RUN pip3 install --break-system-packages \
        kazoo

ARG PULSAR_CLIENT_PYTHON_VERSION
RUN pip3 install --break-system-packages \
    pulsar-client[all]==${PULSAR_CLIENT_PYTHON_VERSION}


###  Create one stage to include JVM distribution
FROM alpine AS jvm

RUN wget -O /etc/apk/keys/amazoncorretto.rsa.pub https://apk.corretto.aws/amazoncorretto.rsa.pub
RUN echo "https://apk.corretto.aws" >> /etc/apk/repositories
RUN apk add --no-cache amazon-corretto-21 binutils

# Use JLink to create a slimmer JDK distribution (see: https://adoptium.net/blog/2021/10/jlink-to-produce-own-runtime/)
# This still includes all JDK modules, though in the future we could compile a list of required modules
RUN /usr/lib/jvm/default-jvm/bin/jlink --add-modules ALL-MODULE-PATH --compress zip-9 --no-man-pages --no-header-files --strip-debug --output /opt/jvm
RUN echo networkaddress.cache.ttl=1 >> /opt/jvm/conf/security/java.security
RUN echo networkaddress.cache.negative.ttl=1 >> /opt/jvm/conf/security/java.security


FROM apachepulsar/glibc-base:2.38 as glibc

## Create final stage from Alpine image
## and add OpenJDK and Python dependencies (for Pulsar functions)
FROM alpine:3.19.1
ENV LANG C.UTF-8

# Install some utilities
RUN apk add --no-cache \
            bash \
            python3 \
            py3-pip \
            gcompat \
            ca-certificates \
            procps \
            curl

# Fix CVE-2024-2511 by upgrading to OpenSSL 3.1.4-r6
# We can remove once new Alpine image is released
RUN apk upgrade --no-cache libssl3 libcrypto3

# Install GLibc compatibility library
COPY --from=glibc /root/packages /root/packages
RUN apk add --allow-untrusted --force-overwrite /root/packages/glibc-*.apk

COPY --from=jvm /opt/jvm /opt/jvm
ENV JAVA_HOME=/opt/jvm

# The default is /pulsat/bin and cannot be written.
ENV PULSAR_PID_DIR=/pulsar/logs

# Copy Python depedencies from the other stage
COPY --from=python-deps /usr/lib/python3.11/site-packages /usr/lib/python3.11/site-packages

ENV PULSAR_ROOT_LOGGER=INFO,CONSOLE

COPY --from=pulsar /pulsar /pulsar

WORKDIR /pulsar
ENV PATH=$PATH:$JAVA_HOME/bin:/pulsar/bin

# The UID must be non-zero. Otherwise, it is arbitrary. No logic should rely on its specific value.
ARG DEFAULT_USERNAME=pulsar
RUN adduser ${DEFAULT_USERNAME} -u 10000 -G root -D -H -h /pulsar/data
USER 10000
