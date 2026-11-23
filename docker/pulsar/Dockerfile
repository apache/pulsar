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
RUN chmod -R ug+rwx /pulsar
RUN chown 10000:0 /pulsar

# Trino writes logs to this directory (at least during tests), so we need to give the process permission
# to create those log directories. This should be removed when Trino is removed.
RUN [ -d /pulsar/trino ] && chmod g+w /pulsar/trino || true

###  Create one stage to include JVM distribution
# Use the official Amazon Corretto image instead of manually fetching the APK signing key
FROM amazoncorretto:17-alpine3.21 AS jvm

# Fail the build if the base image ships a JDK older than the minimum security release.
# 2026-11-17 = Corretto 17.0.21.1.2 / 21.0.13.1.2 (CVE-2026-114938, HotSpot compiler;
# also covers the Oct 2026 CPU, CVE-2026-91403). Bump this after each JDK security release.
ARG MIN_JDK_RELEASE_DATE=2026-11-17
RUN . /usr/lib/jvm/default-jvm/release \
    && echo "JDK ${JAVA_VERSION} released ${JAVA_VERSION_DATE}," \
            "required >= ${MIN_JDK_RELEASE_DATE}" \
    && OLDEST_DATE="$(printf '%s\n%s\n' \
            "${MIN_JDK_RELEASE_DATE}" "${JAVA_VERSION_DATE}" \
            | sort | head -n1)" \
    && [ "${OLDEST_DATE}" = "${MIN_JDK_RELEASE_DATE}" ]

RUN apk add --no-cache binutils

# Use JLink to create a slimmer JDK distribution (see: https://adoptium.net/blog/2021/10/jlink-to-produce-own-runtime/)
# This still includes all JDK modules, though in the future we could compile a list of required modules
RUN /usr/lib/jvm/default-jvm/bin/jlink --add-modules ALL-MODULE-PATH --compress=2 --no-man-pages --no-header-files --strip-debug --output /opt/jvm
RUN echo networkaddress.cache.ttl=1 >> /opt/jvm/conf/security/java.security
RUN echo networkaddress.cache.negative.ttl=1 >> /opt/jvm/conf/security/java.security


## Create final stage from Alpine image
## and add OpenJDK and Python dependencies (for Pulsar functions)
FROM alpine:3.21
ENV LANG C.UTF-8

# Install some utilities and runtime dependencies for Pulsar scripts and native libraries
RUN apk add --no-cache \
            bash \
            curl \
            wget \
            bind-tools \
            netcat-openbsd \
            procps \
            less \
            vim \
            python3 \
            py3-pip \
            py3-yaml \
            gcompat \
            libgcc \
            libstdc++ \
            libuuid \
            ca-certificates \
            openssl \
            coreutils

# Upgrade all packages to get latest versions with security fixes
RUN apk upgrade --no-cache

# Python dependencies
# The pinned grpcio and protobuf versions should be compatible with the generated Protobuf and gRPC stubs used
# in Pulsar Functions Python runtime. You should also update the grpcio version in src/update_python_protobuf_stubs.sh
# and regenerate the Python stubs if you change the grpcio version here. Please see
# pulsar-functions/instance/src/main/python/README.md for more details.
ARG PULSAR_CLIENT_PYTHON_VERSION
# pip>=26.2.2: Alpine's py3-pip bundles a certifi with the compromised
# emSign ECC Root CA - G3 (GHSA-6q48-6c7w-9gw2), so upgrade pip itself too.
RUN pip3 install --break-system-packages --no-cache-dir \
    --only-binary grpcio,pulsar-client \
    grpcio==1.78.0 \
    protobuf==6.33.6 \
    pulsar-client[all]==${PULSAR_CLIENT_PYTHON_VERSION} \
    kazoo \
    "pip>=26.2.2"

COPY --from=jvm /opt/jvm /opt/jvm
ENV JAVA_HOME=/opt/jvm

ENV PULSAR_ROOT_LOGGER=INFO,CONSOLE

COPY --from=pulsar /pulsar /pulsar

# COPY --from resets the destination directory owner to root:root regardless of
# what was set in the source stage. Re-apply ownership here so that uid 10000
# can write directly into /pulsar (e.g. token-superuser-stripped.jwt written by
# the Helm chart startup script when enableTokenAuth=true).
RUN chown 10000:0 /pulsar

WORKDIR /pulsar
ENV PATH=$PATH:$JAVA_HOME/bin:/pulsar/bin
# The default PID dir /pulsar/bin cannot be written by uid 10000; use /pulsar/logs instead
ENV PULSAR_PID_DIR=/pulsar/logs
# Use musl libc library for RocksDB
ENV ROCKSDB_MUSL_LIBC=true
# Preload gcompat library for glibc compatibility with Netty native libraries
ENV LD_PRELOAD=/lib/libgcompat.so.0

# The UID must be non-zero. Otherwise, it is arbitrary. No logic should rely on its specific value.
ARG DEFAULT_USERNAME=pulsar
RUN adduser ${DEFAULT_USERNAME} -u 10000 -G root -D -H -h /pulsar/data
USER 10000
