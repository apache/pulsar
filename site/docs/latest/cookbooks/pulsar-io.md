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

## Custom sources

## Custom sinks