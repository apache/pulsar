---
title: Non-persistent topics
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

{% include admonition.html type="info" content='For high-level information about non-persistent topics, see the [Concepts and Architecture](../../getting-started/ConceptsAndArchitecture#non-persistent-topics) documentation.' %}

## Configuring non-persistent topics {#configuration}

{% include admonition.html type="info" title="Configuration for standalone mode" content="If you're running Pulsar in standalone mode, the same configurable parameters are available, in the [`standalone.conf`](../../reference/Configuration#standalone) configuration file." %}

## Managing non-persistent topics via the CLI {#cli}

Non-persistent topics can be managed using the [`pulsar-admin non-persistent`](../../reference/CliTools#pulsar-admin-non-persistent) CLI.

### Creating a partitioned non-persistent topic

```bash
$ bin/pulsar-admin non-persistent create-partitioned-topic
```

## Non-persistent topics and Pulsar clients {#clients}

You shouldn't need to make

