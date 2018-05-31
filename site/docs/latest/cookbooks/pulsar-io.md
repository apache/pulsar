---
title: The Pulsar IO cookbook
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

[Pulsar IO](../../getting-started/ConceptsAndArchitecture#pulsar-io) is a feature of Pulsar that enables you to easily create and manage **connectors** that interface with external systems, such as databases and other messaging systems.

## Setup

In order to run Pulsar IO connectors, you'll need to install a source distribution.

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