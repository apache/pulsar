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

ARG ALPINE_VERSION=3.23
ARG IMAGE_JDK_MAJOR_VERSION=25

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
FROM amazoncorretto:${IMAGE_JDK_MAJOR_VERSION}-alpine${ALPINE_VERSION} AS jvm

RUN apk add --no-cache binutils

# Use JLink to create a slimmer JDK runtime (see: https://adoptium.net/blog/2021/10/jlink-to-produce-own-runtime/)
# Include only the modules needed at runtime (one per line below for easy maintenance).
#
# Kept on purpose:
#   - java.desktop + jdk.localedata: Jetty/Jersey/SnakeYAML call java.beans.Introspector (in
#     java.desktop), and localedata is needed for non-English date/number formatting.
#   - jdk.jcmd, jdk.jfr, jdk.management.agent: live ops/diagnostics on a running broker.
#   - jdk.jdwp.agent: lets you attach a remote debugger (-agentlib:jdwp) inside the container.
#
# Left out (verified unused by Pulsar's shipped jars via a jdeps/bytecode scan):
#   - build/compile tools: java.compiler, jdk.compiler (also drops the ~10MB lib/ct.sym),
#     jdk.javadoc, jdk.jdeps, jdk.jlink, jdk.jpackage, jdk.jartool
#   - REPL/console: jdk.jshell, jdk.editpad, jdk.internal.ed, jdk.internal.le, jdk.jconsole
#   - debugger client + serviceability: jdk.jdi, jdk.hotspot.agent, jdk.jstatd
#   - GUI/applet/legacy: java.smartcardio, jdk.jsobject, jdk.accessibility, jdk.sctp, jdk.dynalink
#   - Graal/JVMCI compiler modules (jdk.graal.*): unused by the C2 JIT
#   - java.se aggregator: excluded so it does not transitively pull the above back in
#   - jdk.random: part of java.base on JDK 25, not a separate module
#
RUN JDK_MODULES="\
java.base,\
java.datatransfer,\
java.xml,\
java.prefs,\
java.desktop,\
java.instrument,\
java.logging,\
java.management,\
java.security.sasl,\
java.naming,\
java.rmi,\
java.management.rmi,\
java.net.http,\
java.scripting,\
java.security.jgss,\
java.transaction.xa,\
java.sql,\
java.sql.rowset,\
java.xml.crypto,\
jdk.internal.jvmstat,\
jdk.attach,\
jdk.charsets,\
jdk.internal.opt,\
jdk.zipfs,\
jdk.crypto.ec,\
jdk.crypto.cryptoki,\
jdk.httpserver,\
jdk.incubator.vector,\
jdk.jcmd,\
jdk.management,\
jdk.management.agent,\
jdk.jfr,\
jdk.jdwp.agent,\
jdk.localedata,\
jdk.management.jfr,\
jdk.naming.dns,\
jdk.naming.rmi,\
jdk.net,\
jdk.nio.mapmode,\
jdk.security.auth,\
jdk.security.jgss,\
jdk.unsupported,\
jdk.unsupported.desktop,\
jdk.xml.dom" && \
    /usr/lib/jvm/default-jvm/bin/jlink --module-path /usr/lib/jvm/default-jvm/jmods --add-modules "$JDK_MODULES" --compress=zip-9 --no-man-pages --no-header-files --strip-debug --output /opt/jvm
# Corretto bundles async-profiler as a native lib (not a JDK module), so jlink does not copy it; restore it
RUN if [ -f /usr/lib/jvm/default-jvm/lib/libasyncProfiler.so ]; then cp /usr/lib/jvm/default-jvm/lib/libasyncProfiler.so /opt/jvm/lib/; fi
RUN echo networkaddress.cache.negative.ttl=1 >> /opt/jvm/conf/security/java.security

## Create one stage to include snappy-java native lib
# Fix the issue when using snappy-java in x86 arch alpine
# See https://github.com/xerial/snappy-java/issues/181 https://github.com/xerial/snappy-java/issues/579
# We need to ensure that the version of the native library matches the version of snappy-java imported via Maven
FROM alpine:$ALPINE_VERSION AS snappy-java

ARG SNAPPY_VERSION
RUN apk add git alpine-sdk util-linux cmake autoconf automake libtool openjdk17 maven curl bash tar findutils
ENV JAVA_HOME=/usr/lib/jvm/default-jvm
RUN curl -Ls https://github.com/xerial/snappy-java/archive/refs/tags/v$SNAPPY_VERSION.tar.gz | tar zxf - && cd snappy-java-$SNAPPY_VERSION && CMAKE_POLICY_VERSION_MINIMUM=3.5 make clean-native native


## Extract offloader NARs
FROM busybox AS offloaders
ARG PULSAR_OFFLOADER_TARBALL
ADD ${PULSAR_OFFLOADER_TARBALL} /
RUN mv /apache-pulsar-offloaders-*/offloaders /offloaders
# Exclude the filesystem (Hadoop) offloader: it pulls in hadoop-common and bloats the image by ~50MB.
# It remains available in the apache-pulsar-offloaders distribution tarball for users who need it.
RUN rm -f /offloaders/tiered-storage-file-system-*.nar

## Create final stage from Alpine image
## and add OpenJDK and Python dependencies (for Pulsar functions)
FROM alpine:$ALPINE_VERSION
ENV LANG C.UTF-8

# Install some utilities, some are required by Pulsar scripts
RUN apk add --no-cache \
            bash \
            python3 \
            py3-pip \
            py3-yaml \
            gcompat \
            libgcc \
            libstdc++ \
            libuuid \
            ca-certificates \
            procps \
            curl \
            bind-tools \
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
RUN pip3 install --break-system-packages --no-cache-dir \
    --only-binary \
    grpcio==1.78.0 \
    protobuf==6.33.6 \
    pulsar-client[all]==${PULSAR_CLIENT_PYTHON_VERSION} \
    kazoo

COPY --from=jvm /opt/jvm /opt/jvm
ENV JAVA_HOME=/opt/jvm

COPY --from=snappy-java /tmp/libsnappyjava.so /usr/lib/libsnappyjava.so

# The default is /pulsat/bin and cannot be written.
ENV PULSAR_PID_DIR=/pulsar/logs

COPY --from=pulsar /pulsar /pulsar

# Add tiered-storage offloader NARs
COPY --from=offloaders /offloaders /pulsar/offloaders

WORKDIR /pulsar
ENV PATH=$PATH:$JAVA_HOME/bin:/pulsar/bin
# Use musl libc library for RocksDB
ENV ROCKSDB_MUSL_LIBC=true
# Preload gcompat library for glibc compatibility with Netty native libraries
ENV LD_PRELOAD=/lib/libgcompat.so.0

# The UID must be non-zero. Otherwise, it is arbitrary. No logic should rely on its specific value.
ARG DEFAULT_USERNAME=pulsar
RUN adduser ${DEFAULT_USERNAME} -u 10000 -G root -D -H -h /pulsar/data
USER 10000
