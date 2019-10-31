---
id: version-2.3.0-io-cdc
title: CDC Connector
sidebar_label: CDC Connector
original_id: io-cdc
---

## Source

The CDC Source connector is used to capture change log of existing databases like MySQL, MongoDB, PostgreSQL into Pulsar.

The CDC Source connector is built on top of [Debezium](https://debezium.io/) and [Canal](https://github.com/alibaba/canal). This connector stores all data into Pulsar Cluster in a persistent, replicated and partitioned way.
This CDC Source are tested by using MySQL, and you could get more information regarding how it works at [this debezium link](https://debezium.io/docs/connectors/mysql/) or [this canal link](https://github.com/alibaba/canal/wiki).
Regarding how Debezium works, please reference to [Debezium tutorial](https://debezium.io/docs/tutorial/). Regarding how Canal works, please reference to [Canal tutorial](https://github.com/alibaba/canal/wiki). It is recommended that you go through this tutorial first.

- [Debezium Connector](io-cdc-debezium.md)
- [Alibaba Canal Connector](io-cdc-canal.md)