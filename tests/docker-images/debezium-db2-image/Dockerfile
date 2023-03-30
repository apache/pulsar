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