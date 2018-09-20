---
id: version-2.1.0-incubating-io-cassandra
title: Cassandra Sink Connector
sidebar_label: Cassandra Sink Connector
original_id: io-cassandra
---

The Cassandra Sink connector is used to write messages to a Cassandra Cluster.

The tutorial [Connecting Pulsar with Apache Cassandra](io-quickstart.md) shows an example how to use Cassandra Sink
connector to write messages to a Cassandra table.

## Sink Configuration Options

All the Cassandra sink settings are listed as below. All the settings are required to run a Cassandra sink.

| Name | Default | Required | Description |
|------|---------|----------|-------------|
| `roots` | `null` | `true` | Cassandra Contact Points. A list of one or many node address. It is a comma separated `String`. |
| `keyspace` | `null` | `true` | Cassandra Keyspace name. The keyspace should be created prior to creating the sink. |
| `columnFamily` | `null` | `true` | Cassandra ColumnFamily name. The column family should be created prior to creating the sink. |
| `keyname` | `null` | `true` | Key column name. The key column is used for storing Pulsar message keys. If a Pulsar message doesn't have any key associated, the message value will be used as the key. |
| `columnName` | `null` | `true` | Value column name. The value column is used for storing Pulsar message values. |
