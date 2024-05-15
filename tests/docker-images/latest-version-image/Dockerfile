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
ARG PULSAR_ALL_IMAGE
ARG PULSAR_IMAGE

FROM golang:1.21 as pulsar-function-go

COPY target/pulsar-function-go/ /go/src/github.com/apache/pulsar/pulsar-function-go
RUN cd /go/src/github.com/apache/pulsar/pulsar-function-go && go install ./...
RUN cd /go/src/github.com/apache/pulsar/pulsar-function-go/pf && go install
RUN cd /go/src/github.com/apache/pulsar/pulsar-function-go/examples && go install ./...

# Reference pulsar-all to copy connectors from there
FROM $PULSAR_ALL_IMAGE as pulsar-all

########################################
###### Main image build
########################################
FROM $PULSAR_IMAGE

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
COPY --from=pulsar-all /pulsar/connectors/pulsar-io-cassandra-*.nar /pulsar/connectors/
COPY --from=pulsar-all /pulsar/connectors/pulsar-io-debezium-*.nar /pulsar/connectors/
COPY --from=pulsar-all /pulsar/connectors/pulsar-io-elastic-*.nar /pulsar/connectors/
COPY --from=pulsar-all /pulsar/connectors/pulsar-io-hdfs*.nar /pulsar/connectors/
COPY --from=pulsar-all /pulsar/connectors/pulsar-io-jdbc-postgres-*.nar /pulsar/connectors/
COPY --from=pulsar-all /pulsar/connectors/pulsar-io-kafka-*.nar /pulsar/connectors/
COPY --from=pulsar-all /pulsar/connectors/pulsar-io-rabbitmq-*.nar /pulsar/connectors/
COPY --from=pulsar-all /pulsar/connectors/pulsar-io-batch-data-generator-*.nar /pulsar/connectors/
COPY --from=pulsar-all /pulsar/connectors/pulsar-io-kinesis-*.nar /pulsar/connectors/

# download Oracle JDBC driver for Oracle Debezium Connector tests
RUN mkdir -p META-INF/bundled-dependencies
RUN cd META-INF/bundled-dependencies && curl -sSLO https://search.maven.org/remotecontent?filepath=com/oracle/ojdbc/ojdbc8/19.3.0.0/ojdbc8-19.3.0.0.jar
RUN cd META-INF/bundled-dependencies && curl -sSLO https://search.maven.org/remotecontent?filepath=com/oracle/ojdbc/ucp/19.3.0.0/ucp-19.3.0.0.jar
RUN cd META-INF/bundled-dependencies && curl -sSLO https://search.maven.org/remotecontent?filepath=com/oracle/ojdbc/oraclepki/19.3.0.0/oraclepki-19.3.0.0.jar
RUN cd META-INF/bundled-dependencies && curl -sSLO https://search.maven.org/remotecontent?filepath=com/oracle/ojdbc/osdt_cert/19.3.0.0/osdt_cert-19.3.0.0.jar
RUN cd META-INF/bundled-dependencies && curl -sSLO https://search.maven.org/remotecontent?filepath=com/oracle/ojdbc/osdt_core/19.3.0.0/osdt_core-19.3.0.0.jar
RUN cd META-INF/bundled-dependencies && curl -sSLO https://search.maven.org/remotecontent?filepath=com/oracle/ojdbc/simplefan/19.3.0.0/simplefan-19.3.0.0.jar
RUN cd META-INF/bundled-dependencies && curl -sSLO https://search.maven.org/remotecontent?filepath=com/oracle/ojdbc/orai18n/19.3.0.0/orai18n-19.3.0.0.jar
RUN cd META-INF/bundled-dependencies && curl -sSLO https://search.maven.org/remotecontent?filepath=com/oracle/ojdbc/xdb/19.3.0.0/xdb-19.3.0.0.jar
RUN cd META-INF/bundled-dependencies && curl -sSLO https://search.maven.org/remotecontent?filepath=com/oracle/ojdbc/xmlparserv2/19.3.0.0/xmlparserv2-19.3.0.0.jar

RUN jar uf connectors/pulsar-io-debezium-oracle-*.nar META-INF/bundled-dependencies/ojdbc8-19.3.0.0.jar META-INF/bundled-dependencies/ucp-19.3.0.0.jar META-INF/bundled-dependencies/oraclepki-19.3.0.0.jar META-INF/bundled-dependencies/osdt_cert-19.3.0.0.jar META-INF/bundled-dependencies/osdt_core-19.3.0.0.jar META-INF/bundled-dependencies/simplefan-19.3.0.0.jar META-INF/bundled-dependencies/orai18n-19.3.0.0.jar META-INF/bundled-dependencies/xdb-19.3.0.0.jar META-INF/bundled-dependencies/xmlparserv2-19.3.0.0.jar


CMD bash
