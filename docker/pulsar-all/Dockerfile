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

# global arguments (only to be used in FROM clauses)
ARG PULSAR_IMAGE
ARG PULSAR_IO_KINESIS_KPL_IMAGE


FROM busybox AS pulsar-extensions
ARG PULSAR_IO_DIR
ADD ${PULSAR_IO_DIR} /connectors
ARG PULSAR_OFFLOADER_TARBALL
ADD ${PULSAR_OFFLOADER_TARBALL} /
RUN mv /apache-pulsar-offloaders-*/offloaders /offloaders

FROM ${PULSAR_IO_KINESIS_KPL_IMAGE} AS pulsar-io-kinesis-sink-kinesis_producer

FROM ${PULSAR_IMAGE}
COPY --from=pulsar-extensions /connectors /pulsar/connectors
COPY --from=pulsar-extensions /offloaders /pulsar/offloaders
# Copy the kinesis_producer native executable compiled for Alpine musl to the pulsar-all image
# This is required to support the Pulsar IO Kinesis sink connector
COPY --from=pulsar-io-kinesis-sink-kinesis_producer /opt/amazon-kinesis-producer/bin/kinesis_producer /opt/amazon-kinesis-producer/bin/.os_info /opt/amazon-kinesis-producer/bin/.build_time /opt/amazon-kinesis-producer/bin/.revision /opt/amazon-kinesis-producer/bin/.system_info /opt/amazon-kinesis-producer/bin/.version /opt/amazon-kinesis-producer/bin/
# Set the environment variable to point to the kinesis_producer native executable
ENV PULSAR_IO_KINESIS_KPL_PATH=/opt/amazon-kinesis-producer/bin/kinesis_producer
# Install the required dependencies for the kinesis_producer native executable
USER 0
RUN apk update && apk add --no-cache \
    brotli-libs \
    c-ares \
    libcrypto3 \
    libcurl \
    libgcc \
    libidn2 \
    libpsl \
    libssl3 \
    libunistring \
    nghttp2-libs \
    zlib \
    zstd-libs \
    libuuid
USER 10000