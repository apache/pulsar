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

FROM busybox as pulsar-all

ARG PULSAR_IO_DIR
ARG PULSAR_OFFLOADER_TARBALL

ADD ${PULSAR_IO_DIR} /connectors
ADD ${PULSAR_OFFLOADER_TARBALL} /
RUN mv /apache-pulsar-offloaders-*/offloaders /offloaders
RUN chmod -R g=u /connectors /offloaders

FROM apachepulsar/pulsar:latest

# Need permission to create directories and update file permissions
USER root

RUN mkdir /pulsar/connectors /pulsar/offloaders && \
    chown pulsar:root /pulsar/connectors /pulsar/offloaders && \
    chmod g=u /pulsar/connectors /pulsar/offloaders

# Return to pulsar (non root) user
USER pulsar

COPY --from=pulsar-all --chown=pulsar:0 /connectors /pulsar/connectors
COPY --from=pulsar-all --chown=pulsar:0 /offloaders /pulsar/offloaders
