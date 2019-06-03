---
title: Getting started with Pulsar IO
lead: Connecting your systems with Pulsar using Pulsar IO
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

In order to run Pulsar IO connectors, you'll need to have a binary distribution of pulsar locally.

## Managing connectors

Pulsar connectors can be managed using the [`source`](../../reference/CliTools#pulsar-admin-source) and [`sink`](../../reference/CliTools#pulsar-admin-sink) commands of the [`pulsar-admin`](../../reference/CliTools#pulsar-admin) CLI tool.

### Running sources

You can use the [`create`](../../reference/CliTools#pulsar-admin-source-create)

You can submit a sink to be run in an existing Pulsar cluster using a command of this form:

```bash
$ ./bin/pulsar-admin sinks create --className  <classname> --jar <jar-location> --tenant test --namespace <namespace> --name <sink-name> --inputs <input-topics>
```

Here’s an example command:

```bash
bin/pulsar-admin sources create --className  org.apache.pulsar.io.twitter.TwitterFireHose --jar ~/application.jar --tenant test --namespace ns1 --name twitter-source --destinationTopicName twitter_data
```

Instead of submitting a source to run on an existing Pulsar cluster, you alternatively can run a source as a process on your local machine:

```bash
bin/pulsar-admin sources localrun --className  org.apache.pulsar.io.twitter.TwitterFireHose --jar ~/application.jar --tenant test --namespace ns1 --name twitter-source --destinationTopicName twitter_data
```

### Running Sinks

You can submit a sink to be run in an existing Pulsar cluster using a command of this form:

```bash
./bin/pulsar-admin sinks create --className  <classname> --jar <jar-location> --tenant test --namespace <namespace> --name <sink-name> --inputs <input-topics>
```

Here’s an example command:

```bash
./bin/pulsar-admin sinks create --className  org.apache.pulsar.io.cassandra --jar ~/application.jar --tenant test --namespace ns1 --name cassandra-sink --inputs test_topic
```

Instead of submitting a sink to run on an existing Pulsar cluster, you alternatively can run a sink as a process on your local machine:

```bash
./bin/pulsar-admin sinks localrun --className  org.apache.pulsar.io.cassandra --jar ~/application.jar --tenant test --namespace ns1 --name cassandra-sink --inputs test_topic
```

## Available connectors

At the moment, the following connectors are available for Pulsar:

{% include connectors.html %}

