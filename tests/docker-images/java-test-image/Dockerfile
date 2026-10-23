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

ARG PULSAR_IMAGE
FROM $PULSAR_IMAGE

# Base pulsar image is designed not be modified, though we need to add more scripts
USER root

COPY target/scripts /pulsar/bin
RUN chmod a+rx /pulsar/bin/*

RUN apk add --no-cache supervisor jq
# On Alpine, musl's debug symbols, which Alpine strips from musl: with them, profilers name musl's functions, such as
# the system call wrappers under the JNI methods, and walk native stacks through them, instead of showing every frame in
# musl as /lib/ld-musl-*.so.1. apk installs the ones of the installed musl version. The Wolfi image has glibc instead.
RUN if ls /lib/ld-musl-*.so.1 > /dev/null 2>&1; then apk add --no-cache musl-dbg; fi

RUN mkdir -p /var/log/pulsar \
    && mkdir -p /var/run/supervisor/ \
    && mkdir -p /pulsar/certificate-authority

COPY target/conf /etc/supervisord/conf.d/
RUN mv /etc/supervisord/conf.d/supervisord.conf /etc/supervisord.conf

# TLS certificates
COPY target/certificate-authority /pulsar/certificate-authority/

COPY target/java-test-functions.jar /pulsar/examples/

# copy buildtools.jar to /pulsar/lib so that org.apache.pulsar.tests.ExtendedNettyLeakDetector can be used
COPY target/buildtools.jar /pulsar/lib/

# The tiered storage offloader, for the offloading performance tests: the Pulsar images of 4.x releases don't include the
# offloaders, which a release publishes as apache-pulsar-offloaders-<version>-bin.tar.gz. With INSTALL_OFFLOADERS=true,
# install the jcloud offloader of the image's Pulsar version when the image doesn't have it, from the download CDN, which
# has only the latest releases, or else from the slower release archive, checked against the release's SHA-512. Images
# that include it, such as this repository's and those of 5.0 and later, are left as they are. It is off by default,
# since the tarball is 64 MB for 4.2.5 and 281 MB for 4.0.0, which only the scenarios that offload need; Docker keeps
# the layer, so it is downloaded once for each release image.
ARG INSTALL_OFFLOADERS=false
ARG PULSAR_RELEASE_DOWNLOAD_URL=https://dlcdn.apache.org/pulsar
ARG PULSAR_RELEASE_ARCHIVE_URL=https://archive.apache.org/dist/pulsar
RUN if [ "${INSTALL_OFFLOADERS}" = "true" ] && ! ls /pulsar/offloaders/tiered-storage-jcloud-*.nar > /dev/null 2>&1; then \
    version=$(ls /pulsar/lib | sed -n 's/^org\.apache\.pulsar-pulsar-broker-\([0-9][^-]*\)\.jar$/\1/p' | head -1) \
    && if [ -n "${version}" ]; then \
        name="apache-pulsar-offloaders-${version}-bin.tar.gz" \
        && file="pulsar-${version}/${name}" \
        && mkdir -p /pulsar/offloaders /tmp/offloaders /tmp/offloaders-download \
        && cd /tmp/offloaders-download \
        && { { curl -fsSL -o "${name}" "${PULSAR_RELEASE_DOWNLOAD_URL}/${file}" \
                && curl -fsSL -o "${name}.sha512" "${PULSAR_RELEASE_DOWNLOAD_URL}/${file}.sha512"; } \
            || { curl -fsSL -o "${name}" "${PULSAR_RELEASE_ARCHIVE_URL}/${file}" \
                && curl -fsSL -o "${name}.sha512" "${PULSAR_RELEASE_ARCHIVE_URL}/${file}.sha512"; }; } \
        && sha512sum -c "${name}.sha512" \
        && tar -xzf "${name}" -C /tmp/offloaders \
        && cp /tmp/offloaders/*/offloaders/tiered-storage-jcloud-*.nar /pulsar/offloaders/ \
        && cd / && rm -rf /tmp/offloaders /tmp/offloaders-download \
        && ls /pulsar/offloaders/tiered-storage-jcloud-*.nar; \
    else \
        echo "No offloader installed: the Pulsar version of the image wasn't found"; \
    fi; \
    fi

ARG INSTALL_ASYNC_PROFILER=false
ARG ASYNC_PROFILER_VERSION=4.5
# install async-profiler from https://github.com/async-profiler/async-profiler/releases to /opt/async-profiler
RUN if [ "${INSTALL_ASYNC_PROFILER}" = "true" ]; then \
    mkdir -p /opt/async-profiler \
    && cd /opt/async-profiler \
    && curl -L \
    "https://github.com/async-profiler/async-profiler/releases/download/v${ASYNC_PROFILER_VERSION}/async-profiler-${ASYNC_PROFILER_VERSION}-linux-$([ "$(uname -m)" = "aarch64" ] && echo "arm64" || echo "x64").tar.gz" \
    | tar -zxvf - --strip-components=1; \
    else \
    echo "async-profiler installation skipped"; \
    fi
