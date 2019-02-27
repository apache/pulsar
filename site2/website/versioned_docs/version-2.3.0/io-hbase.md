---
id: version-2.3.0-io-hbase
title: hbase Connector
sidebar_label: hbase Connector
original_id: io-hbase
---

## Sink

The hbase Sink Connector is used to pull messages from Pulsar topics and persist the messages
to a hbase table.

## Sink Configuration Options

All the Hbase sink settings are listed as below. All the settings are required to run a Hbase sink.

| Name | Default | Required | Description |
|------|---------|----------|-------------|
| `hbaseConfigResources` | `null` | `false` | hbase system configuration 'hbase-site.xml' file. |
| `zookeeperQuorum` | `null` | `true` | hbase system configuration about hbase.zookeeper.quorum value. |
| `zookeeperClientPort` | `2181` | `false` | hbase system configuration about hbase.zookeeper.property.clientPort value. |
| `zookeeperZnodeParent` | `/hbase` | `false` | hbase system configuration about zookeeper.znode.parent value. |
| `tableName` | `null` | `true` | hbase table, value is namespace:tableName, namespace default value is default. |
| `rowKeyName` | `null` | `true` | hbase table rowkey name. |
| `familyName` | `null` | `true` | hbase table column family name. |
| `qualifierNames` | `null` | `true` | hbase table column qualifier names. |
| `timeoutMs` | `1000l` | `false` | hbase table operation timeout in milliseconds. |
| `batchSize` | `200` | `false` | The batch size of updates made to the hbase table. |
