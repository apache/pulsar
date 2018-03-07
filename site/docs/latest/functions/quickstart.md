---
title: Getting started with Pulsar Functions
---

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