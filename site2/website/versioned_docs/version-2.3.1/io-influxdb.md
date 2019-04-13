---
id: version-2.3.1-io-influxdb
title: InfluxDB Connector
sidebar_label: InfluxDB Connector
original_id: io-influxdb
---

## Sink

The InfluxDB Sink Connector is used to pull messages from Pulsar topics and persist the messages
to an InfluxDB database.

## Sink Configuration Options

| Name | Default | Required | Description |
|------|---------|----------|-------------|
| `influxdbUrl` | `null` | `true` | The url of the InfluxDB instance to connect to. |
| `username` | `null` | `false` | The username used to authenticate to InfluxDB. |
| `password` | `null` | `false` | The password used to authenticate to InfluxDB. |
| `database` | `null` | `true` | The InfluxDB database to write to. |
| `consistencyLevel` | `ONE` | `false` | The consistency level for writing data to InfluxDB. Possible values [ALL, ANY, ONE, QUORUM]. |
| `logLevel` | `NONE` | `false` | The log level for InfluxDB request and response. Possible values [NONE, BASIC, HEADERS, FULL]. |
| `retentionPolicy` | `autogen` | `false` | The retention policy for the InfluxDB database. |
| `gzipEnable` | `false` | `false` | Flag to determine if gzip should be enabled. |
| `batchTimeMs` | `1000` | `false` | The InfluxDB operation time in milliseconds. |
| `batchSize` | `200` | `false` | The batch size of write to InfluxDB database. |
