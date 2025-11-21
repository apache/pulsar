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

FROM busybox as pulsar-experimental

ARG PULSAR_IO_DIR
ARG PULSAR_OFFLOADER_TARBALL
ARG PULSAR_IO_KINESIS_KPL_IMAGE

ADD ${PULSAR_IO_DIR} /connectors
ADD ${PULSAR_OFFLOADER_TARBALL} /
RUN mv /apache-pulsar-offloaders-*/offloaders /offloaders

FROM ${PULSAR_IO_KINESIS_KPL_IMAGE} AS pulsar-io-kinesis-sink-kinesis_producer
FROM apachepulsar/pulsar:latest
COPY --from=pulsar-experimental /connectors /pulsar/connectors
COPY --from=pulsar-experimental /offloaders /pulsar/offloaders
ENV PULSAR_IO_KINESIS_KPL_PATH=/opt/amazon-kinesis-producer/bin/kinesis_producer
COPY --from=pulsar-io-kinesis-sink-kinesis_producer /opt/amazon-kinesis-producer/bin/kinesis_producer /opt/amazon-kinesis-producer/bin/.os_info /opt/amazon-kinesis-producer/bin/.build_time /opt/amazon-kinesis-producer/bin/.revision /opt/amazon-kinesis-producer/bin/.system_info /opt/amazon-kinesis-producer/bin/.version /opt/amazon-kinesis-producer/bin/
USER 0
RUN rm -rf /pulsar/connectors/*cassandra*.nar && rm -rf /pulsar/connectors/*kafka-connect-adaptor*.nar
USER 10000