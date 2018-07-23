---
id: io-overview
title: Pulsar IO Overview
sidebar_label: Overview
---

Messaging systems are most powerful when you can easily use them in conjunction with external systems like databases and other messaging systems. **Pulsar IO** is a feature of Pulsar that enables you to easily create, deploy, and manage Pulsar **connectors** that interact with external systems, such as [Apache Cassandra](https://cassandra.apache.org), [Aerospike](https://www.aerospike.com), and many others.

> #### Pulsar IO and Pulsar Functions
> Under the hood, Pulsar IO connectors are specialized [Pulsar Functions](functions-overview.md) purpose-built to interface with external systems. The [administrative interface](io-quickstart.md) for Pulsar IO is, in fact, quite similar to that of Pulsar Functions."

## Sources and sinks

Pulsar IO connectors come in two types:

* **Sources** feed data *into* Pulsar from other systems. Common sources include other messaging systems and "firehose"-style data pipeline APIs.
* **Sinks** are fed data *from* Pulsar. Common sinks include other messaging systems and SQL and NoSQL databases.

This diagram illustrates the relationship between sources, sinks, and Pulsar:

![Pulsar IO diagram](/docs/assets/pulsar-io.png "Pulsar IO connectors (sources and sinks")

## Working with connectors

Pulsar IO connectors can be managed via the [`pulsar-admin`](reference-pulsar-admin.md) CLI tool, in particular the [`source`](reference-pulsar-admin.md#source) and [`sink`](reference-pulsar-admin.md#sink) commands.

> For a guide to managing connectors in your Pulsar installation, see the [Getting started with Pulsar IO](io-quickstart.md)

The following connectors are currently available for Pulsar:

|Name|Java Class|
|---|---|
|[Aerospike sink](https://www.aerospike.com/)|[`org.apache.pulsar.io.aerospike.AerospikeSink`](https://github.com/apache/incubator-pulsar/blob/master/pulsar-io/aerospike/src/main/java/org/apache/pulsar/io/aerospike/AerospikeStringSink.java)|
|[Cassandra sink](https://cassandra.apache.org)|[`org.apache.pulsar.io.cassandra.CassandraSink`](https://github.com/apache/incubator-pulsar/blob/master/pulsar-io/cassandra/src/main/java/org/apache/pulsar/io/cassandra/CassandraStringSink.java)|
|[Kafka source](https://kafka.apache.org)|[`org.apache.pulsar.io.kafka.KafkaSource`](https://github.com/apache/incubator-pulsar/blob/master/pulsar-io/kafka/src/main/java/org/apache/pulsar/io/kafka/KafkaStringSource.java)|
|[Kafka sink](https://kafka.apache.org)|[`org.apache.pulsar.io.kafka.KafkaSink`](https://github.com/apache/incubator-pulsar/blob/master/pulsar-io/kafka/src/main/java/org/apache/pulsar/io/kafka/KafkaStringSink.java)|
|[RabbitMQ source](https://www.rabbitmq.com)|[`org.apache.pulsar.io.rabbitmq.RabbitMQSource`](https://github.com/apache/incubator-pulsar/blob/master/pulsar-io/rabbitmq/src/main/java/org/apache/pulsar/io/rabbitmq/RabbitMQSource.java)|
|[Twitter Firehose source](https://developer.twitter.com/en/docs)|[org.apache.pulsar.io.twitter.TwitterFireHose](https://github.com/apache/incubator-pulsar/blob/master/pulsar-io/twitter/src/main/java/org/apache/pulsar/io/twitter/TwitterFireHose.java)|
