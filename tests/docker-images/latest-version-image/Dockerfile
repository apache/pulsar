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

FROM apachepulsar/pulsar:latest

RUN apt-get update && apt-get install -y supervisor

RUN mkdir -p /var/log/pulsar && mkdir -p /var/run/supervisor/

COPY conf/supervisord.conf /etc/supervisord.conf
COPY conf/global-zk.conf /etc/supervisord/conf.d/global-zk.conf
COPY conf/local-zk.conf /etc/supervisord/conf.d/local-zk.conf
COPY conf/bookie.conf /etc/supervisord/conf.d/bookie.conf
COPY conf/broker.conf /etc/supervisord/conf.d/broker.conf
COPY conf/proxy.conf /etc/supervisord/conf.d/proxy.conf

COPY scripts/init-cluster.sh /pulsar/bin
COPY scripts/run-global-zk.sh /pulsar/bin
COPY scripts/run-local-zk.sh /pulsar/bin
COPY scripts/run-bookie.sh /pulsar/bin
COPY scripts/run-broker.sh /pulsar/bin
COPY scripts/run-proxy.sh /pulsar/bin

