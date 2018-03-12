---
title: Deploying Pulsar Functions
new: true
---

## Local run mode

At the moment, Pulsar Functions are deployed

## Cluster mode

## State storage

By default, Pulsar uses [Apache BookKeeper](https://bookkeeper.apache.org).

## Parallelism

Pulsar Functions run as processes called **instances**. When you run a Pulsar Function, it runs as a single instance by default. You can increase the **parallelism** of a function (i.e. the number of instances) via the [command line](../../references/CliTools#pulsar-admin-functions). Here's an example:

```bash
$ bin/pulsar-admin functions create \
  --parallelism 3 \
  # Other function info
```

You can adjust the parallelism of an already created function using the [`update`](../../reference/CliTools#pulsar-admin-functions-update) interface.