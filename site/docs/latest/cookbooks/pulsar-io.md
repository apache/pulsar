---
title: The Pulsar IO cookbook
---

[Pulsar IO](../../getting-started/ConceptsAndArchitecture#pulsar-io) is a feature of Pulsar that enables you to easily create and manage **connectors** that interface with external systems, such as databases and other messaging systems.

## Managing connectors

Pulsar connectors can be managed using the [`source`](../../reference/CliTools#pulsar-admin-source) and [`sink`](../../reference/CliTools#pulsar-admin-sink) commands of the [`pulsar-admin`](../../reference/CliTools#pulsar-admin) CLI tool.

### Running sources

You can use the [`create`](../../reference/CliTools#pulsar-admin-source-create)

This command, for example, would create a new connector in a Pulsar cluster:

```bash
$ bin/pulsar-admin source create \
  --name 
```

## Available connectors

At the moment, the following connectors are available for Pulsar:

{% include connectors.html %}

{% include admonition.html type="success" title="Custom connectors" content="You can easily extend Pulsar IO by creating your own custom connectors in Java. See the [Custom Pulsar IO connectors](../../project/custom-connectors) for instructions." %}