---
title: Non-persistent messaging
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

**Non-persistent topics** are Pulsar {% popover topics %} in which message data is *never* [persistently stored](../../getting-started/ConceptsAndArchitecture#persistent-storage) and kept only in memory. This cookbook provides:

* A basic [conceptual overview](#overview) of non-persistent topics
* Information about [configurable parameters](#configuration) related to non-persistent topics
* A guide to the [CLI interface](#cli) for managing non-persistent topics

## Overview of non-persistent topics {#overview}

{% include explanations/non-persistent-topics.md %}

{% include admonition.html type="info" content='For more high-level information about non-persistent topics, see the [Concepts and Architecture](../../getting-started/ConceptsAndArchitecture#non-persistent-topics) documentation.' %}

## Using non-persistent topics {#using}

{% include admonition.html type="warning" content='In order to use non-persistent topics, they must be [enabled](#enabling) in your Pulsar broker configuration.' %}

In order to use non-persistent topics, you only need to differentiate them by name when interacting with them. This [`pulsar-client produce`](../../CliTools#pulsar-client-produce) command, for example, would produce one message on a non-persistent topic in a {% popover standalone %} cluster:

```bash
$ bin/pulsar-client produce non-persistent://public/default/example-np-topic \
  --num-produce 1 \
  --messages "This message will be stored only in memory"
```

{% include admonition.html type="success" content="For a more thorough guide to non-persistent topics from an administrative perspective, see the [Non-persistent topics](../../admin-api/non-persistent-topics) guide." %}

## Enabling non-persistent topics {#enabling}

In order to enable non-persistent topics in a Pulsar {% popover broker %}, the [`enableNonPersistentTopics`](../../reference/Configuration#broker-enableNonPersistentTopics) must be set to `true`. This is the default, and so you won't need to take any action to enable non-persistent messaging.

{% include admonition.html type="info" title="Configuration for standalone mode" content="If you're running Pulsar in standalone mode, the same configurable parameters are available but in the [`standalone.conf`](../../reference/Configuration#standalone) configuration file." %}

If you'd like to enable *only* non-persistent topics in a broker, you can set the [`enablePersistentTopics`](../../reference/Configuration#broker-enablePersistentTopics) parameter to `false` and the `enableNonPersistentTopics` parameter to `true`.

## Managing non-persistent topics via the CLI {#cli}

Non-persistent topics can be managed using the [`pulsar-admin non-persistent`](../../reference/CliTools#pulsar-admin-non-persistent) command-line interface. With that interface you can perform actions like [create a partitioned non-persistent topic](../../reference/CliTools#pulsar-admin-non-persistent-create-partitioned-topic), get [stats](../../reference/CliTools#pulsar-admin-non-persistent-stats) for a non-persistent topic, [list](../../) non-persistent topics under a namespace, and more.

## Non-persistent topics and Pulsar clients {#clients}

You shouldn't need to make any changes to your Pulsar clients to use non-persistent messaging beyond making sure that you use proper [topic names](#using) with `non-persistent` as the topic type.