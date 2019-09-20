---
id: io-overview
title: Pulsar connector overview
sidebar_label: Overview
---

Messaging systems are most powerful when you can easily use them with external systems like databases and other messaging systems. 

**Pulsar IO connectors** enable you to easily create, deploy, and manage connectors that interact with external systems, such as [Apache Cassandra](https://cassandra.apache.org), [Aerospike](https://www.aerospike.com), and many others.


## Concept

Pulsar IO connectors come in two types: **source** and **sink**.

This diagram illustrates the relationship between source, Pulsar, and sink:

![Pulsar IO diagram](assets/pulsar-io.png "Pulsar IO connectors (sources and sinks)")


### Source

> Sources **feed data from external systems into Pulsar**. 

Common sources include other messaging systems and firehose-style data pipeline APIs.

For the complete list of Pulsar built-in source connectors, see [source connector](io-connectors.md#source-connector).

### Sink

> Sinks **feed data from Pulsar into external systems**. 

Common sinks include other messaging systems and SQL and NoSQL databases.

For the complete list of Pulsar built-in sink connectors, see [sink connector](io-connectors.md#sink-connector).

## Work with connector

You can manage Pulsar connectors (for example, create, update, start, stop, restart, reload, delete and perform other operations on connectors) via the [Connector CLI](reference-connector-admin.md) with [sources](reference-connector-admin.md#sources) and [sinks](reference-connector-admin.md#sinks) subcommands.

Connectors (sources and sinks) and Functions are components of instances, and they all run on Functions workers. When managing a source, sink or function via **Pulsar admin CLI** (that is, [Connector CLI](reference-connector-admin.md) and [Functions CLI](functions-cli.md)), an instance is started on a worker. For more information, see [Functions worker](functions-worker.md#run-functions-worker-separately).

