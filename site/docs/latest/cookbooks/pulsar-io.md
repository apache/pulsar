---
title: The Pulsar IO cookbook
---

## Managing connectors

Pulsar connectors can be managed using the [`source`](../../reference/CliTools#pulsar-admin-source) and [`sink`](../../reference/CliTools#pulsar-admin-sink) commands of the [`pulsar-admin`](../../reference/CliTools#pulsar-admin).

This command, for example, would create a new connector in a Pulsar cluster:

```bash
$ bin/pulsar-admin source create \
  --name 
```

## Available connectors

At the moment, the following connectors are available for Pulsar:

{% include connectors.html %}

## Custom sources

In order to create 

## Custom sinks