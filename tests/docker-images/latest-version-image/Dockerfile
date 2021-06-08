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

FROM apachepulsar/pulsar:latest as pulsar-function-go

USER root

RUN rm -rf /var/lib/apt/lists/* && apt-get update
RUN apt-get install -y procps curl git

ENV GOLANG_VERSION 1.13.3

RUN curl -sSL https://golang.org/dl/go$GOLANG_VERSION.linux-amd64.tar.gz \
		| tar -C /usr/local -xz

# RUN wget https://dl.google.com/go/go1.13.3.linux-amd64.tar.gz && tar -xvf go1.13.3.linux-amd64.tar.gz && mv go /usr/local
# RUN export GOROOT=/usr/local/go && export GOPATH=$HOME/go && export PATH=$GOPATH/bin:$GOROOT/bin:$PATH
# RUN echo "export GOROOT=/usr/local/go" >> ~/.profile && echo "export GOPATH=$HOME/go" >> ~/.profile && echo "export PATH=$GOPATH/bin:$GOROOT/bin:$PATH" >> ~/.profile

ENV PATH /usr/local/go/bin:$PATH

RUN mkdir -p /go/src /go/bin && chmod -R 777 /go
ENV GOROOT /usr/local/go
ENV GOPATH /go
ENV PATH /go/bin:$PATH

COPY target/pulsar-function-go/ /go/src/github.com/apache/pulsar/pulsar-function-go
RUN cd /go/src/github.com/apache/pulsar/pulsar-function-go && go install ./...
RUN cd /go/src/github.com/apache/pulsar/pulsar-function-go/pf && go install
RUN cd /go/src/github.com/apache/pulsar/pulsar-function-go/examples && go install ./...

# Reference pulsar-all to copy connectors from there
FROM apachepulsar/pulsar-all:latest as pulsar-all

########################################
###### Main image build
########################################
FROM apachepulsar/pulsar:latest

RUN rm -rf /var/lib/apt/lists/* && apt update

RUN apt-get clean && apt-get update && apt-get install -y supervisor vim procps curl

RUN mkdir -p /var/log/pulsar && mkdir -p /var/run/supervisor/ && mkdir -p /pulsar/ssl

COPY conf/supervisord.conf /etc/supervisord.conf
COPY conf/global-zk.conf conf/local-zk.conf conf/bookie.conf conf/broker.conf conf/functions_worker.conf \
     conf/proxy.conf conf/presto_worker.conf /etc/supervisord/conf.d/

COPY ssl/ca.cert.pem ssl/broker.key-pk8.pem ssl/broker.cert.pem \
     ssl/admin.key-pk8.pem ssl/admin.cert.pem \
     ssl/user1.key-pk8.pem ssl/user1.cert.pem \
     ssl/proxy.key-pk8.pem ssl/proxy.cert.pem \
     ssl/superproxy.key-pk8.pem ssl/superproxy.cert.pem \
     /pulsar/ssl/

COPY scripts/init-cluster.sh scripts/run-global-zk.sh scripts/run-local-zk.sh \
     scripts/run-bookie.sh scripts/run-broker.sh scripts/run-functions-worker.sh scripts/run-proxy.sh scripts/run-presto-worker.sh \
     scripts/run-standalone.sh \
     /pulsar/bin/

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

CMD bash
