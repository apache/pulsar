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

- JRE (64-bit). Different Pulsar versions rely on different JRE versions. For how to choose the JRE version, see [Pulsar Runtime Java Version Recommendation](https://github.com/apache/pulsar/blob/master/README.md#pulsar-runtime-java-version-recommendation).

## Step 1. Download Pulsar distribution

Download the official Apache Pulsar distribution:

```bash
wget https://archive.apache.org/dist/pulsar/pulsar-@pulsar:version@/apache-pulsar-@pulsar:version@-bin.tar.gz
```

Once downloaded, unpack the tar file:

```bash
tar xvfz apache-pulsar-@pulsar:version@-bin.tar.gz
```

For the rest of this quickstart all commands are run from the root of the distribution folder, so switch to it:

```bash
cd apache-pulsar-@pulsar:version@
```

List the contents by executing:

```bash
ls -1F
```

You may want to note that:

| Directory     | Description                                                                                         |
| ------------- | --------------------------------------------------------------------------------------------------- |
| **bin**       | The [`pulsar`](reference-cli-tools.md#pulsar) entry point script, and many other command-line tools |
| **conf**      | Configuration files, including `broker.conf`                                                        |
| **lib**       | JARs used by Pulsar                                                                                 |
| **examples**  | [Pulsar Functions](functions-overview.md) examples                                                  |
| **instances** | Artifacts for [Pulsar Functions](functions-overview.md)                                             |

## Step 2. Start a Pulsar standalone cluster

Run this command to start a standalone Pulsar cluster:

```bash
bin/pulsar standalone
```

These directories are created once you started the Pulsar cluster:

| Directory | Description                                |
| --------- | ------------------------------------------ |
| **data**  | All data created by BookKeeper and RocksDB |
| **logs**  | All server-side logs                       |

:::tip

* To run the service as a background process, you can use the `bin/pulsar-daemon start standalone` command. For more information, see [pulsar-daemon](reference-cli-tools.md#pulsar-daemon).
* The `public/default` namespace is created when you start a Pulsar cluster. This namespace is for development purposes. All Pulsar topics are managed within namespaces. For more information, see [Namespaces](concepts-messaging.md#namespaces) and [Topics](concepts-messaging.md#topics).

:::

## Step 3. Create a topic

Pulsar stores messages in topics. It's a good practice to explicitly create topics before using them, even if Pulsar can automatically create topics when they are referenced.

To create a new topic, run this command:

```bash
bin/pulsar-admin topics create persistent://public/default/my-topic
```

## Step 4. Write messages to the topic

You can use the `pulsar` command line tool to write messages to a topic. This is useful for experimentation, but in practice you'll use the Producer API in your application code, or Pulsar IO connectors for pulling data in from other systems to Pulsar.

Run this command to produce a message:

```bash
bin/pulsar-client produce my-topic --messages 'Hello Pulsar!'
```

## Step 5. Read messages from the topic

Now that some messages have been written to the topic, run this command to launch the consumer and read those messages back:

```bash
bin/pulsar-client consume my-topic -s 'my-subscription' -p Earliest -n 0
```

Earliest means consuming from the earliest **unconsumed** message. `-n` configures the number of messages to consume, 0 means to consume forever.

As before, this is useful for trialling things on the command line, but in practice you'll use the Consumer API in your application code, or Pulsar IO connectors for reading data from Pulsar to push to other systems.

You'll see the messages that you produce in the previous step:

```text
----- got message -----
key:[null], properties:[], content:Hello Pulsar!
```

## Step 6. Write some more messages

Leave the consume command from the previous step running. If you've already closed it, just re-run it.

Now open a new terminal window and produce more messages, the default message separator is `,`:

```bash
bin/pulsar-client produce my-topic --messages "$(seq -s, -f 'Message NO.%g' -t '\n' 1 10)"
```

Note how they are displayed almost instantaneously in the consumer terminal.

## Step 7. Stop the Pulsar cluster

Once you've finished you can shut down the Pulsar cluster. Press **Ctrl-C** in the terminal window in which you started the cluster.

## Further readings

* Read [Pulsar Concepts and Architecture](concepts-architecture-overview.md) to learn more about Pulsar fundamentals.
* Read [Pulsar Client Libraries](client-libraries.md) to connect Pulsar with your application.
* Read [Pulsar Connectors](io-overview.md) to connect Pulsar with your existing data pipelines.
* Read [Pulsar Functions](functions-overview.md) to run serverless computations against Pulsar.
