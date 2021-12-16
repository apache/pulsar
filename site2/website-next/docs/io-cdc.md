---
id: io-cdc
title: CDC connector
sidebar_label: "CDC connector"
---

CDC source connectors capture log changes of databases (such as MySQL, MongoDB, and PostgreSQL) into Pulsar.

> CDC source connectors are built on top of [Canal](https://github.com/alibaba/canal) and [Debezium](https://debezium.io/) and store all data into Pulsar cluster in a persistent, replicated, and partitioned way.

Currently, Pulsar has the following CDC connectors.

Name|Java Class
|---|---
[Canal source connector](io-canal-source)|[org.apache.pulsar.io.canal.CanalStringSource.java](https://github.com/apache/pulsar/blob/master/pulsar-io/canal/src/main/java/org/apache/pulsar/io/canal/CanalStringSource.java)
[Debezium source connector](io-cdc-debezium)|<li>[org.apache.pulsar.io.debezium.DebeziumSource.java](https://github.com/apache/pulsar/blob/master/pulsar-io/debezium/core/src/main/java/org/apache/pulsar/io/debezium/DebeziumSource.java)<br /></li><li>[org.apache.pulsar.io.debezium.mysql.DebeziumMysqlSource.java](https://github.com/apache/pulsar/blob/master/pulsar-io/debezium/mysql/src/main/java/org/apache/pulsar/io/debezium/mysql/DebeziumMysqlSource.java)<br /></li><li>[org.apache.pulsar.io.debezium.postgres.DebeziumPostgresSource.java](https://github.com/apache/pulsar/blob/master/pulsar-io/debezium/postgres/src/main/java/org/apache/pulsar/io/debezium/postgres/DebeziumPostgresSource.java)</li>

For more information about Canal and Debezium, see the information below.

Subject | Reference
|---|---
How to use Canal source connector with MySQL|[Canal guide](https://github.com/alibaba/canal/wiki)
How does Canal work | [Canal tutorial](https://github.com/alibaba/canal/wiki)
How to use Debezium source connector with MySQL | [Debezium guide](https://debezium.io/docs/connectors/mysql/)
How does Debezium work | [Debezium tutorial](https://debezium.io/docs/tutorial/)
