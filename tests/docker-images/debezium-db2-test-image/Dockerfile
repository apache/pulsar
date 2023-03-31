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

# As of 3/31/2023:
# "If the Program is designated as "Non-Production", the Program can only be deployed as part of the Licensee's internal development and test environment
# for internal non-production activities, including but not limited to testing, performance
# tuning, fault diagnosis, internal benchmarking, staging, quality assurance activity
# and/or developing internally used additions or extensions to the Program using published
# application programming interfaces. Licensee is not authorized to use any part of the
# Program for any other purposes without acquiring the appropriate production entitlements."
# (https://www.ibm.com/support/customer/csol/terms/?id=L-GMET-8KJN5X&lc=en#detail-document)
# Additionally:
# "Client license files are not required for connections to Db2, Cloudscape,
# or IBM Informix® databases from the IBM Data Server Driver for JDBC and SQLJ version
# 3.50 or later. Client license files are not required for direct connections to Db2 for
# z/OS if Db2 Connect Unlimited Edition for System z® server license activation is performed."
# (https://www.ibm.com/docs/en/db2/11.5?topic=apis-installing-data-server-driver-jdbc-sqlj)

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