---
id: version-2.2.0-io-managing
title: Managing Connectors
sidebar_label: Managing Connectors
original_id: io-managing
---

This section describes how to manage Pulsar IO connectors in a Pulsar cluster. You will learn how to:

- Deploy builtin connectors
- Monitor and update running connectors with Pulsar Admin CLI
- Deploy customized connectors
- Upgrade a connector

## Using Builtin Connectors

Pulsar bundles several [builtin connectors](io-overview.md#working-with-connectors) that should be used for moving data in and out
of commonly used systems such as databases, messaging systems. Getting set up to use these builtin connectors is simple. You can follow
the [instructions](getting-started-standalone.md#installing-builtin-connectors) on installing builtin connectors. After setup, all
the builtin connectors will be automatically discovered by Pulsar brokers (or function-workers), so no additional installation steps are
required.

## Configuring Connectors

Configuring Pulsar IO connectors is straightforward. What you need to do is to provide a yaml configuration file when your [run connectors](#running-connectors).
The yaml configuration file basically tells Pulsar where to locate the sources and sinks and how to connect those sources and sinks with Pulsar topics.

Below is an example yaml configuration file for Cassandra Sink:

```shell
tenant: public
namespace: default
name: cassandra-test-sink
...
# cassandra specific config
configs:
    roots: "localhost:9042"
    keyspace: "pulsar_test_keyspace"
    columnFamily: "pulsar_test_table"
    keyname: "key"
    columnName: "col"
```

The example yaml basically tells Pulsar which Cassandra cluster to connect, what is the `keyspace` and `columnFamily` to be used in Cassandra for collecting data,
and how to map a Pulsar message into Cassandra table key and columns.

For details, consult the documentation for [individual connectors](io-overview.md#working-with-connectors).

## Running Connectors

Pulsar connectors can be managed using the [`source`](reference-pulsar-admin.md#source) and [`sink`](reference-pulsar-admin.md#sink) commands of the [`pulsar-admin`](reference-pulsar-admin.md) CLI tool.

### Running sources

You can submit a source to be run in an existing Pulsar cluster using a command of this form:

```bash
$ ./bin/pulsar-admin source create --classname  <classname> --archive <jar-location> --tenant <tenant> --namespace <namespace> --name <source-name> --destination-topic-name <output-topic>
```

Here’s an example command:

```bash
bin/pulsar-admin source create --classname org.apache.pulsar.io.twitter.TwitterFireHose --archive ~/application.jar --tenant test --namespace ns1 --name twitter-source --destination-topic-name twitter_data
```

Instead of submitting a source to run on an existing Pulsar cluster, you alternatively can run a source as a process on your local machine:

```bash
bin/pulsar-admin source localrun --classname  org.apache.pulsar.io.twitter.TwitterFireHose --archive ~/application.jar --tenant test --namespace ns1 --name twitter-source --destination-topic-name twitter_data
```

If you are submitting a built-in source, you don't need to specify `--classname` and `--archive`.
You can simply specify the source type `--source-type`. The command to submit a built-in source is
in following form:

```bash
./bin/pulsar-admin source create \
    --tenant <tenant> \
    --namespace <namespace> \
    --name <source-name> \
    --destination-topic-name <input-topics> \
    --source-type <source-type>
```

Here's an example to submit a Kafka source:

```bash
./bin/pulsar-admin source create \
    --tenant test-tenant \
    --namespace test-namespace \
    --name test-kafka-source \
    --destination-topic-name pulsar_sink_topic \
    --source-type kafka
```

### Running Sinks

You can submit a sink to be run in an existing Pulsar cluster using a command of this form:

```bash
./bin/pulsar-admin sink create --classname  <classname> --archive <jar-location> --tenant test --namespace <namespace> --name <sink-name> --inputs <input-topics>
```

Here’s an example command:

```bash
./bin/pulsar-admin sink create --classname  org.apache.pulsar.io.cassandra --archive ~/application.jar --tenant test --namespace ns1 --name cassandra-sink --inputs test_topic
```

Instead of submitting a sink to run on an existing Pulsar cluster, you alternatively can run a sink as a process on your local machine:

```bash
./bin/pulsar-admin sink localrun --classname  org.apache.pulsar.io.cassandra --archive ~/application.jar --tenant test --namespace ns1 --name cassandra-sink --inputs test_topic
```

If you are submitting a built-in sink, you don't need to specify `--classname` and `--archive`.
You can simply specify the sink type `--sink-type`. The command to submit a built-in sink is
in following form:

#### Note

> The `sink-type` parameter of the currently built-in connectors is determined by the setting of the `name` parameter specified in the pulsar-io.yaml file.

```bash
./bin/pulsar-admin sink create \
    --tenant <tenant> \
    --namespace <namespace> \
    --name <sink-name> \
    --inputs <input-topics> \
    --sink-type <sink-type>
```

Here's an example to submit a Cassandra sink:

```bash
./bin/pulsar-admin sink create \
    --tenant test-tenant \
    --namespace test-namespace \
    --name test-cassandra-sink \
    --inputs pulsar_input_topic \
    --sink-type cassandra
```

## Monitoring Connectors

Since Pulsar IO connectors are running as [Pulsar Functions](functions-overview.md), so you can use [`functions`](reference-pulsar-admin.md#source) commands
available in the [`pulsar-admin`](reference-pulsar-admin.md) CLI tool.

### Retrieve Connector Metadata

```
bin/pulsar-admin functions get \
    --tenant <tenant> \
    --namespace <namespace> \
    --name <connector-name>
```

### Retrieve Connector Running Status

```
bin/pulsar-admin functions getstatus \
    --tenant <tenant> \
    --namespace <namespace> \
    --name <connector-name>
```
