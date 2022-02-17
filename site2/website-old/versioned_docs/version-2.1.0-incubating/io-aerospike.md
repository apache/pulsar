---
id: version-2.1.0-incubating-io-aerospike
title: Aerospike Sink Connector
sidebar_label: Aerospike Sink Connector
original_id: io-aerospike
---

The Aerospike Sink connector is used to write messages to an Aerospike Cluster.

## Sink Configuration Options

The following configuration options are specific to the Aerospike Connector:

| Name | Required | Default | Description |
|------|----------|---------|-------------|
| `seedHosts` | `true` | `null` | Comma separated list of one or more Aerospike cluster hosts; each host can be specified as a valid IP address or hostname followed by an optional port number (default is 3000). | 
| `keyspace` | `true` | `null` | Aerospike namespace to use. |
| `keySet` | `false` | `null` | Aerospike set name to use. |
| `columnName` | `true` | `null` | Aerospike bin name to use. |
| `maxConcurrentRequests` | `false` | `100` | Maximum number of concurrent Aerospike transactions that a Sink can open. |
| `timeoutMs` | `false` | `100` | A single timeout value controls `socketTimeout` and `totalTimeout` for Aerospike transactions.  |
| `retries` | `false` | `1` | Maximum number of retries before aborting a write transaction to Aerospike. |
