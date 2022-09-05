---
id: getting-started-standalone
title: Run a standalone Pulsar cluster locally
sidebar_label: "Run Pulsar locally"
---

For local development and testing, you can run Pulsar in standalone mode on your machine. The standalone mode runs all components inside a single Java Virtual Machine (JVM) process.

:::tip

If you're looking to run a full production Pulsar installation, see the [Deploying a Pulsar instance](deploy-bare-metal.md) guide.

:::

## Prerequisites

Nothing more than a 64-bit JRE is required to run a standalone Pulsar cluster. For the required JRE version, see [Pulsar Runtime Java Version Recommendation](https://github.com/apache/pulsar/blob/master/README.md#pulsar-runtime-java-version-recommendation) according to your target Pulsar version.

## Download Pulsar distribution

Download the official Apache Pulsar distribution:

```bash
wget https://archive.apache.org/dist/pulsar/pulsar-@pulsar:version@/apache-pulsar-@pulsar:version@-bin.tar.gz
```

Once downloaded, unpack the tar file:

```bash
tar xvfz apache-pulsar-@pulsar:version@-bin.tar.gz
```

For the rest of this quickstart we'll run commands from the root of the distribution folder, so switch to it:

```bash
cd apache-pulsar-@pulsar:version@
```

## Browse Pulsar distribution

List the contents by executing:

```bash
ls -1F
```

You will see it layouts as:

```text
LICENSE
NOTICE
README
bin/
conf/
examples/
instances/
lib/
licenses/
```

You may want to note that:

* `bin` directory contains the [`pulsar`](reference-cli-tools.md#pulsar) entry point script, and many other command-line tools.
* `conf` directory contains configuration files, including `broker.conf`.
* `lib` directory contains JARs used by Pulsar.
* `examples` directory contains [Pulsar Functions](functions-overview.md) examples.
* `instances` directory artifacts for [Pulsar Functions](functions-overview.md).

## Start the Pulsar standalone cluster

Run this command to start a standalone Pulsar cluster:

```bash
bin/pulsar standalone
```

By default, the standalone mode runs a RocksDB instance for metadat storage. If you'd prefer to start a cluster with standalone ZooKeeper server, set `PULSAR_STANDALONE_USE_ZOOKEEPER` to 1:

```bash
PULSAR_STANDALONE_USE_ZOOKEEPER=1 bin/pulsar standalone
```

These directories are created once you started the Pulsar cluster.

* `data` directory stores all data created by BookKeeper and RocksDB.
* `logs` directory contains all server-side logs.

## Create a topic

Pulsar stores messages in topics. It's good practice to explicitly create them before using them, even if Pulsar can automagically create them when referenced.

Run this command to create a new topic into which we'll write and read some test messages:

```bash
bin/pulsar-admin topics create persistent://public/default/quickstart
```

## Write messages to the topic

You can use the `pulsar` command line tool to write messages to a topic. This is useful for experimentation, but in practice you'll use the Producer API in your application code, or Pulsar IO connectors for pulling data in from other systems to Pulsar.

Run this command to produce a message:

```bash
bin/pulsar-client produce quickstart --messages 'Hello Pulsar!'
```

## Read messages from the topic

Now that we've written message to the topic, we'll read those messages back.

Run this command to launch the consumer:

```bash
bin/pulsar-client consume quickstart -s 'first-subscription' -p Earliest -n 0
```

Earliest means consuming from the earliest **unconsumed** message. `-n` configures the number of messages to consume, 0 means to consume forever.

As before, this is useful for trialling things on the command line, but in practice you'll use the Consumer API in your application code, or Pulsar IO connectors for reading data from Pulsar to push to other systems.

You'll see the messages that you produce in the previous step:

```text
----- got message -----
key:[null], properties:[], content:Hello Pulsar!
```

## Write some more messages

Leave the consume command from the previous step running. If you've already closed it, just re-run it.

Now open a new terminal window and produce more messages, the default message separator is `,`:

```bash
bin/pulsar-client produce quickstart --messages "$(seq -s, -f 'Message NO.%g' -t '\n' 1 10)"
```

Note how they are displayed almost instantaneously in the consumer terminal.

## Stop the Pulsar cluster

Once you've finished you can shut down the Pulsar cluster. Press **Ctrl-C** in the terminal window in which you started the cluster.

## Further readings

* Read [Pulsar Concepts and Architecture](concepts-architecture-overview.md) to learn more about Pulsar fundamentals.
* Read [Pulsar Client Libraries](client-libraries.md) to connect Pulsar with your application.
* Read [Pulsar Connectors](io-overview.md) to connect Pulsar with your existing data pipelines.
* Read [Pulsar Functions](functions-overview.md) to run serverless computations against Pulsar.
