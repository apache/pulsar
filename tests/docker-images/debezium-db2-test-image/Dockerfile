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
FROM ibmcom/db2:11.5.0.0a

MAINTAINER  Devin Bost
# Originally from https://github.com/debezium/debezium-connector-db2/blob/bee92787b1e02bb3e0813a6d8b7f62d34ad6d9e2/src/test/docker/db2-cdc-docker/Dockerfile

RUN mkdir -p /asncdctools/src

RUN cd /asncdctools/src && curl -sSLO "https://raw.githubusercontent.com/debezium/debezium-connector-db2/main/src/test/docker/db2-cdc-docker/asncdc.c"
RUN cd /asncdctools/src && curl -sSLO "https://raw.githubusercontent.com/debezium/debezium-connector-db2/main/src/test/docker/db2-cdc-docker/dbsetup.sh"
RUN cd /asncdctools/src && curl -sSLO "https://raw.githubusercontent.com/debezium/debezium-connector-db2/main/src/test/docker/db2-cdc-docker/asncdc_UDF.sql"
RUN cd /asncdctools/src && curl -sSLO "https://raw.githubusercontent.com/debezium/debezium-connector-db2/main/src/test/docker/db2-cdc-docker/asncdcaddremove.sql"
RUN cd /asncdctools/src && curl -sSLO "https://raw.githubusercontent.com/debezium/debezium-connector-db2/main/src/test/docker/db2-cdc-docker/asncdctables.sql"

RUN chmod -R  777  /asncdctools

RUN mkdir /var/custom
RUN chmod -R  777 /var/custom

RUN cd /var/custom && curl -sSLO "https://raw.githubusercontent.com/debezium/debezium-connector-db2/main/src/test/docker/db2-cdc-docker/cdcsetup.sh"

RUN chmod 777 /var/custom/cdcsetup.sh