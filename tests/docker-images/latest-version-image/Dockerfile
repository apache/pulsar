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

# build go lang examples first in a separate layer

FROM golang:1.23 as pulsar-function-go

COPY target/pulsar-function-go/ /go/src/github.com/apache/pulsar/pulsar-function-go
RUN cd /go/src/github.com/apache/pulsar/pulsar-function-go && go install ./...
RUN cd /go/src/github.com/apache/pulsar/pulsar-function-go/pf && go install
RUN cd /go/src/github.com/apache/pulsar/pulsar-function-go/examples && go install ./...

# Reference pulsar-all to copy connectors from there
FROM apachepulsar/pulsar-all:latest as pulsar-all


# patch Oracle Debezium Connector nar file to include Oracle JDBC dependencies
FROM apachepulsar/pulsar-all:latest as oracle-jdbc-builder

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
FROM apachepulsar/pulsar:latest

# Switch to run as the root user to simplify building container and then running
# supervisord. Each of the pulsar components are spawned by supervisord and their
# process configuration files specify that the process will be run with UID 10000.
# However, any processes exec'ing into the containers will run as root, by default.
USER root

RUN rm -rf /var/lib/apt/lists/* && apt update

RUN apt-get clean && apt-get update && apt-get install -y supervisor vim procps curl

RUN mkdir -p /var/log/pulsar && mkdir -p /var/run/supervisor/

COPY conf/supervisord.conf /etc/supervisord.conf
COPY conf/global-zk.conf conf/local-zk.conf conf/bookie.conf conf/broker.conf conf/functions_worker.conf \
     conf/proxy.conf conf/presto_worker.conf conf/websocket.conf /etc/supervisord/conf.d/

COPY scripts/init-cluster.sh scripts/run-global-zk.sh scripts/run-local-zk.sh \
     scripts/run-bookie.sh scripts/run-broker.sh scripts/run-functions-worker.sh scripts/run-proxy.sh scripts/run-presto-worker.sh \
     scripts/run-standalone.sh scripts/run-websocket.sh \
     /pulsar/bin/

COPY conf/presto/jvm.config /pulsar/trino/conf

# copy python test examples
RUN mkdir -p /pulsar/instances/deps
COPY python-examples/exclamation_lib.py /pulsar/instances/deps/
COPY python-examples/exclamation_with_extra_deps.py /pulsar/examples/python-examples/
COPY python-examples/exclamation.zip /pulsar/examples/python-examples/
COPY python-examples/producer_schema.py /pulsar/examples/python-examples/
COPY python-examples/consumer_schema.py /pulsar/examples/python-examples/
COPY python-examples/exception_function.py /pulsar/examples/python-examples/

# copy java test examples
COPY target/java-test-functions.jar /pulsar/examples/

# copy go test examples
COPY --from=pulsar-function-go /go/bin /pulsar/examples/go-examples

# TLS certificates
RUN mkdir -p /pulsar/certificate-authority
COPY target/certificate-authority /pulsar/certificate-authority/

# copy broker plugins
COPY target/plugins/ /pulsar/examples/

# Include all offloaders
COPY --from=pulsar-all /pulsar/offloaders /pulsar/offloaders

# Include only the connectors needed by integration tests
COPY --from=pulsar-all /pulsar/connectors/pulsar-io-debezium-*.nar /pulsar/connectors/
COPY --from=pulsar-all /pulsar/connectors/pulsar-io-elastic-*.nar /pulsar/connectors/
COPY --from=pulsar-all /pulsar/connectors/pulsar-io-jdbc-postgres-*.nar /pulsar/connectors/
COPY --from=pulsar-all /pulsar/connectors/pulsar-io-kafka-*.nar /pulsar/connectors/
COPY --from=pulsar-all /pulsar/connectors/pulsar-io-kinesis-*.nar /pulsar/connectors/

# Copy patched Oracle Debezium Connector nar file which includes Oracle JDBC dependencies
COPY --from=oracle-jdbc-builder /pulsar/connectors/pulsar-io-debezium-oracle-*.nar /pulsar/connectors/

CMD bash
