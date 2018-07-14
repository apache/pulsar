---
id: io-quickstart
title: Pulsar IO Overview
sidebar_label: Getting started
---

[Pulsar IO](../../getting-started/ConceptsAndArchitecture#pulsar-io) is a feature of Pulsar that enables you to easily create and manage **connectors** that interface with external systems, such as databases and other messaging systems.

## Setup

In order to run Pulsar IO connectors, you'll need to have a binary distribution of pulsar locally.

## Managing connectors

Pulsar connectors can be managed using the [`source`](../../reference/CliTools#pulsar-admin-source) and [`sink`](../../reference/CliTools#pulsar-admin-sink) commands of the [`pulsar-admin`](../../reference/CliTools#pulsar-admin) CLI tool.

### Running sources

You can use the [`create`](../../reference/CliTools#pulsar-admin-source-create)

You can submit a sink to be run in an existing Pulsar cluster using a command of this form:

```bash
$ ./bin/pulsar-admin sink create --className  <classname> --jar <jar-location> --tenant test --namespace <namespace> --name <sink-name> --inputs <input-topics>
```

Here’s an example command:

```bash
bin/pulsar-admin source create --className  org.apache.pulsar.io.twitter.TwitterFireHose --jar ~/application.jar --tenant test --namespace ns1 --name twitter-source --destinationTopicName twitter_data
```

Instead of submitting a source to run on an existing Pulsar cluster, you alternatively can run a source as a process on your local machine:

```bash
bin/pulsar-admin source localrun --className  org.apache.pulsar.io.twitter.TwitterFireHose --jar ~/application.jar --tenant test --namespace ns1 --name twitter-source --destinationTopicName twitter_data
```

### Running Sinks

You can submit a sink to be run in an existing Pulsar cluster using a command of this form:

```bash
./bin/pulsar-admin sink create --className  <classname> --jar <jar-location> --tenant test --namespace <namespace> --name <sink-name> --inputs <input-topics>
```

Here’s an example command:

```bash
./bin/pulsar-admin sink create --className  org.apache.pulsar.io.cassandra --jar ~/application.jar --tenant test --namespace ns1 --name cassandra-sink --inputs test_topic
```

Instead of submitting a sink to run on an existing Pulsar cluster, you alternatively can run a sink as a process on your local machine:

```bash
./bin/pulsar-admin sink localrun --className  org.apache.pulsar.io.cassandra --jar ~/application.jar --tenant test --namespace ns1 --name cassandra-sink --inputs test_topic
```

## Available connectors

At the moment, the following connectors are available for Pulsar:

|Name|Java Class|
|---|---|
|[Aerospike sink](https://www.aerospike.com/)|[`org.apache.pulsar.io.aerospike.AerospikeSink`](https://github.com/apache/incubator-pulsar/blob/master/pulsar-io/aerospike/src/main/java/org/apache/pulsar/connect/aerospike/AerospikeSink.java)|
|[Cassandra sink](https://cassandra.apache.org")|[`org.apache.pulsar.io.cassandra.CassandraSink`](https://github.com/apache/incubator-pulsar/blob/master/pulsar-io/cassandra/src/main/java/org/apache/pulsar/connect/cassandra/CassandraSink.java)|
|[Kafka source](https://kafka.apache.org)|[`org.apache.pulsar.io.kafka.KafkaSource`](https://github.com/apache/incubator-pulsar/blob/master/pulsar-io/kafka/src/main/java/org/apache/pulsar/connect/kafka/KafkaSource.java)|
|[Kafka sink](https://kafka.apache.org)|[`org.apache.pulsar.io.kafka.KafkaSink`](https://github.com/apache/incubator-pulsar/blob/master/pulsar-io/kafka/src/main/java/org/apache/pulsar/connect/kafka/KafkaSink.java)|
|[RabbitMQ source](https://www.rabbitmq.com)|[`org.apache.pulsar.io.rabbitmq.RabbitMQSource`](https://github.com/apache/incubator-pulsar/blob/master/pulsar-io/rabbitmq/src/main/java/org/apache/pulsar/connect/rabbitmq/RabbitMQSource.java)|
|[Twitter Firehose source](https://developer.twitter.com/en/docs)|[org.apache.pulsar.io.twitter.TwitterFireHose](https://github.com/apache/incubator-pulsar/blob/master/pulsar-io/twitter/src/main/java/org/apache/pulsar/connect/twitter/TwitterFireHose.java)|


