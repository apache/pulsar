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

ARG PULSAR_ALL_IMAGE
ARG GOLANG_IMAGE

# build go lang examples first in a separate layer
FROM $GOLANG_IMAGE as pulsar-function-go

COPY target/pulsar-function-go/ /go/src/github.com/apache/pulsar/pulsar-function-go
RUN cd /go/src/github.com/apache/pulsar/pulsar-function-go && go install ./...
RUN cd /go/src/github.com/apache/pulsar/pulsar-function-go/pf && go install
RUN cd /go/src/github.com/apache/pulsar/pulsar-function-go/examples && go install ./...


# patch Oracle Debezium Connector nar file to include Oracle JDBC dependencies
FROM $PULSAR_ALL_IMAGE as oracle-jdbc-builder

USER root
WORKDIR /

RUN OJDBC_VERSION="19.3.0.0" && \
    OJDBC_BASE_URL="https://repo1.maven.org/maven2/com/oracle/ojdbc" && \
    DEPS_DIR="META-INF/bundled-dependencies" && \
    OJDBC_ARTIFACTS="ojdbc8 ucp oraclepki osdt_cert osdt_core simplefan orai18n xdb xmlparserv2" && \
    cd / && rm -rf "$DEPS_DIR" && mkdir -p "$DEPS_DIR" && \
    cd "$DEPS_DIR" && \
    for ojdbc_artifact in $OJDBC_ARTIFACTS; do \
        ojdbc_jar_file="${ojdbc_artifact}-${OJDBC_VERSION}.jar" && \
        ojdbc_download_url="${OJDBC_BASE_URL}/${ojdbc_artifact}/${OJDBC_VERSION}/${ojdbc_jar_file}" && \
        echo "Downloading $ojdbc_jar_file from $ojdbc_download_url" && \
        curl -sSL --retry 5 --retry-delay 1 --retry-max-time 30 "${ojdbc_download_url}" -o "${ojdbc_jar_file}" && \
        [ -f "${ojdbc_jar_file}" ] || (echo "Failed to download ${ojdbc_jar_file}" && exit 1); \
    done && \
    cd / && \
    jar uvf /pulsar/connectors/pulsar-io-debezium-oracle-*.nar $DEPS_DIR/*.jar >&2

########################################
###### Main image build
########################################
FROM $PULSAR_ALL_IMAGE

# Switch to run as the root user to simplify building container and then running
# supervisord. Each of the pulsar components are spawned by supervisord and their
# process configuration files specify that the process will be run with UID 10000.
# However, any processes exec'ing into the containers will run as root, by default.
USER root

RUN apk add --no-cache supervisor procps curl jq

RUN mkdir -p /var/log/pulsar && mkdir -p /var/run/supervisor/

COPY conf /etc/supervisord/conf.d/
RUN mv /etc/supervisord/conf.d/supervisord.conf /etc/supervisord.conf

COPY scripts /pulsar/bin/
RUN chmod a+rx /pulsar/bin/*

# copy python test examples
RUN mkdir -p /pulsar/instances/deps
COPY python-examples/exclamation_lib.py /pulsar/instances/deps/
COPY python-examples/exclamation_with_extra_deps.py /pulsar/examples/python-examples/
COPY python-examples/exclamation.zip /pulsar/examples/python-examples/
COPY python-examples/producer_schema.py /pulsar/examples/python-examples/
COPY python-examples/consumer_schema.py /pulsar/examples/python-examples/
COPY python-examples/exception_function.py /pulsar/examples/python-examples/
RUN chmod g+rx /pulsar/examples/python-examples/

# copy java test examples
COPY target/java-test-functions.jar /pulsar/examples/

# copy buildtools.jar to /pulsar/lib so that org.apache.pulsar.tests.ExtendedNettyLeakDetector can be used
COPY target/buildtools.jar /pulsar/lib/

# copy go test examples
COPY --from=pulsar-function-go /go/bin /pulsar/examples/go-examples

# TLS certificates
RUN mkdir -p /pulsar/certificate-authority
COPY target/certificate-authority /pulsar/certificate-authority/

# copy broker plugins
COPY target/plugins/ /pulsar/examples/

# Copy patched Oracle Debezium Connector nar file which includes Oracle JDBC dependencies
COPY --from=oracle-jdbc-builder /pulsar/connectors/pulsar-io-debezium-oracle-*.nar /pulsar/connectors/

# Fix permissions for filesystem offloader
RUN mkdir -p pulsar
RUN chmod g+rwx pulsar

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

CMD bash