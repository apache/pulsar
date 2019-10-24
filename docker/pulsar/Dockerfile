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

# First create a stage with just the Pulsar tarball and scripts
FROM busybox as pulsar

ARG PULSAR_TARBALL

ADD ${PULSAR_TARBALL} /
RUN mv /apache-pulsar-* /pulsar

COPY scripts/apply-config-from-env.py /pulsar/bin
COPY scripts/gen-yml-from-env.py /pulsar/bin
COPY scripts/generate-zookeeper-config.sh /pulsar/bin
COPY scripts/pulsar-zookeeper-ruok.sh /pulsar/bin
COPY scripts/watch-znode.py /pulsar/bin
COPY scripts/install-pulsar-client-37.sh /pulsar/bin

### Create 2nd stage from OpenJDK image
### and add Python dependencies (for Pulsar functions)

FROM openjdk:8-jdk-slim
COPY --from=pulsar /pulsar /pulsar

# Install some utilities
RUN apt-get update \
     && apt-get install -y netcat dnsutils less procps iputils-ping python3.7 python3-setuptools curl \
                 libreadline-gplv2-dev libncursesw5-dev libssl-dev libsqlite3-dev tk-dev libgdbm-dev libc6-dev libbz2-dev \
     && apt-get clean \
     && rm -rf /var/lib/apt/lists/*

RUN curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
RUN python3.7 get-pip.py
RUN pip3 install setuptools==41.4.0 pyyaml==5.1.2 kazoo==2.6.1

RUN if [ ! -e /usr/bin/pip ]; then ln -s pip3 /usr/bin/pip ; fi
RUN if [ ! -e /usr/bin/python ]; then ln -sf /usr/bin/python3 /usr/bin/python; fi

ADD target/python-client/ /pulsar/pulsar-client
ADD target/cpp-client/ /pulsar/cpp-client
RUN /pulsar/bin/install-pulsar-client-37.sh
RUN echo networkaddress.cache.ttl=1 >> $JAVA_HOME/jre/lib/security/java.security
RUN apt-get update \
     && apt-get clean \
     && rm -rf /var/lib/apt/lists/* \
     && rm get-pip.py

WORKDIR /pulsar

VOLUME  ["/pulsar/conf", "/pulsar/data"]

ENV PULSAR_ROOT_LOGGER=INFO,CONSOLE
