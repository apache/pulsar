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
$ 
```

## Run a Pulsar Function in local run mode

```bash
$ bin/pulsar
```

## The `pulsar-functions` CLI tool

[`pulsar-functions`](../../reference/CliTools#pulsar-functions)

```bash
$ alias pulsar-functions='/path/to/pulsar/bin/pulsar-functions'
```

## Querying state

```bash
$ bin/pulsar-functions querystate \
  --tenant sample \
  --namespace my-functions \
  --function-name my-function \
  --key "some-key"
```

## Running functions locally

[`localrun`](../../reference/CliTools#pulsar-functions-localrun)

```bash
$ bin/pulsar-functions localrun
```