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

ARG INSTALL_ASYNC_PROFILER=false
# install async-profiler from https://github.com/async-profiler/async-profiler/releases/tag/v4.1 to /opt/async-profiler
RUN if [ "${INSTALL_ASYNC_PROFILER}" = "true" ]; then \
    mkdir -p /opt/async-profiler \
    && cd /opt/async-profiler \
    && curl -L \
    "https://github.com/async-profiler/async-profiler/releases/download/v4.1/async-profiler-4.1-linux-$([ "$(uname -m)" = "aarch64" ] && echo "arm64" || echo "x64").tar.gz" \
    | tar -zxvf - --strip-components=1; \
    else \
    echo "async-profiler installation skipped"; \
    fi