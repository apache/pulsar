---
title: Getting started with Pulsar Functions
lead: Write and run your first Pulsar Function in just a few steps
---

This tutorial will walk you through running your first Pulsar Function in local run mode.

{% include admonition.html content="In local run mode, your Pulsar Function will communicate with your Pulsar cluster but will run outside of the cluster." %}

## Prerequisites

In order to follow along with this tutorial, you'll need to have [Maven](https://maven.apache.org) installed.

## Run a standalone Pulsar cluster

In order to run our Pulsar Function, we'll need to run a Pulsar cluster locally. The easiest way to do that is to run Pulsar in {% popover standalone %} mode. The following steps will enable you to get Pulsar running:

```bash
$ wget https://github.com/streamlio/incubator-pulsar/releases/download/2.0.0-incubating-functions-preview/apache-pulsar-2.0.0-incubating-functions-preview-bin.tar.gz
$ tar xvf apache-pulsar-2.0.0-incubating-functions-preview-bin.tar.gz
$ cd apache-pulsar-2.0.0-incubating-functions-preview
$ bin/pulsar standalone
```

Namespace, tenant, etc.

## Run a Pulsar Function in local run mode

```bash
$ bin/pulsar-admin functions localrun \
  --jar examples/api-examples.jar \
  --className org.apache.pulsar.functions.api.examples.ExclamationFunction \
  --inputs persistent://sample/standalone/ns1/exclamation-input \
  --output persistent://sample/standalone/ns1/exclamation-output
```

We can use the [`pulsar-client`](../../reference/CliTools#pulsar-client) CLI tool to publish a message to the input topic:

```bash
$ bin/pulsar-client produce persistent://sample/standalone/ns1/exclamation-input \
  --num-produce 1 \
  --messages "Hello world"
```

Here's what happens next:

* The `Hello world` message that we published to the input {% popover topic %} (`persistent://sample/standalone/ns1/exclamation-input`) will be passed to the exclamation function that we're now running on our laptop
* The exclamation function will process the message (providing a result of `Hello world!`) and publish the result to the output topic (`persistent://sample/standalone/ns1/exclamation-output`).
* Pulsar will durably store the message data published to the output topic in [Apache BookKeeper](https://bookkeeper.apache.org) until a {% popover consumer %} consumes and {% popover acknowledges %} the message

To consume the message, we can use the same [`pulsar-client`](../../reference/CliTools#pulsar-client) tool that we used to publish the original message:

```bash
$ bin/pulsar-client consume persistent://sample/standalone/ns1/exclamation-output \
  --subscription-name my-subscription \
  --num-messages 1
```

In the output, you should see the following:

```
----- got message -----
Hello world!
```

Success! As you can see, the message has been successfully processed by the exclamation function.

## Run a Pulsar Function in cluster mode

[Local run mode](#run-a-pulsar-function-in-local-run-mode) is useful for development and experimentation, but if you wanted to use Pulsar Functions in a real Pulsar deployment, you'd want to run them in **cluster mode**. In cluster mode, Pulsar Functions run *inside* your Pulsar cluster.

```bash
$ bin/pulsar-admin functions create \
  --jar examples/api-examples.jar \
  --className org.apache.pulsar.functions.api.examples.ExclamationFunction \
  --inputs persistent://sample/standalone/ns1/exclamation-input \
  --output persistent://sample/standalone/ns1/exclamation-output \
  --tenant sample \
  --namespace ns1
```