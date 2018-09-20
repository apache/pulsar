---
title: Pulsar IO overview
lead: Connecting the world using Pulsar Functions
---

<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

Messaging systems are most powerful when you can easily use them in conjunction with external systems like databases and other messaging systems. **Pulsar IO** is a feature of Pulsar that enables you to easily create, deploy, and manage Pulsar **connectors** that interact with external systems, such as [Apache Cassandra](https://cassandra.apache.org), [Aerospike](https://www.aerospike.com), and many others.

{% include admonition.html type="info" title="Pulsar IO and Pulsar Functions"
   content="Under the hood, Pulsar IO connectors are specialized [Pulsar Functions](../../functions/overview) purpose-built to interface with external systems. The [administrative interface](../quickstart) for Pulsar IO is, in fact, quite similar to that of Pulsar Functions." %}

## Sources and sinks

Pulsar IO connectors come in two types:

* **Sources** feed data *into* Pulsar from other systems. Common sources include other messaging systems and "firehose"-style data pipeline APIs.
* **Sinks** are fed data *from* Pulsar. Common sinks include other messaging systems and SQL and NoSQL databases.

This diagram illustrates the relationship between sources, sinks, and Pulsar:

{% include figure.html src="/img/pulsar-io.png" alt="Pulsar IO diagram" caption="Pulsar IO connectors (sources and sinks)" width="80" %}

## Working with connectors

Pulsar IO connectors can be managed via the [`pulsar-admin`](../../reference/CliTools#pulsar-admin) CLI tool, in particular the [`source`](../../reference/CliTools#pulsar-admin-source) and [`sink`](../../reference/CliTools#pulsar-admin-sink) commands.

{% include admonition.html type="info" content="For a guide to managing connectors in your Pulsar installation, see the [Getting started with Pulsar IO](../quickstart)." %}

The following connectors are currently available for Pulsar:

{% include connectors.html %}
