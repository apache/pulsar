---
title: Partitioned topics
lead: Expand message throughput by distributing load within topics
tags:
- topics
- partitioning
- admin
- clients
- cookbook
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

By default, Pulsar {% popover topics %} are served by a single {% popover broker %}. Using only a single broker, however, limits a topic's maximum throughput. *Partitioned topics* are a special type of topic that can span multiple brokers and thus allow for much higher throughput. For an explanation of how partitioned topics work, see the [Concepts](#concepts) section below.

You can [publish](#publishing-to-partitioned-topics) to partitioned topics using Pulsar's client libraries and you can [create and manage](#managing-partitioned-topics) partitioned topics using Pulsar's [admin API](../../admin-api/overview).

## Publishing to partitioned topics

When publishing to partitioned topics, the only difference from non-partitioned topics is that you need to specify a [routing mode](../../getting-started/ConceptsAndArchitecture#routing-modes) when you create a new {% popover producer %}. Examples for [Java](#java) are below.

### Java

Publishing messages to partitioned topics in the Java client works much like [publishing to normal topics](../../clients/Java#using-producers). The difference is that you need to specify either one of the currently available message routers or a custom router.

#### Routing mode

You can specify the routing mode in the {% javadoc ProducerConfiguration client org.apache.pulsar.client.api.ProducerConfiguration %} object that you use to configure your producer. You have three options:

* `SinglePartition`
* `RoundRobinPartition`
* `CustomPartition`

Here's an example:

```java
String pulsarBrokerRootUrl = "pulsar://localhost:6650";
String topic = "persistent://my-property/my-cluster-my-namespace/my-topic";

PulsarClient client = PulsarClient.create(pulsarBrokerRootUrl);
ProducerConfiguration config = new ProducerConfiguration();
config.setMessageRoutingMode(ProducerConfiguration.MessageRoutingMode.SinglePartition);
Producer producer = client.createProducer(topic, config);
producer.send("Partitioned topic message".getBytes());
```

#### Custom message router

To use a custom message router, you need to provide an implementation of the {% javadoc MessageRouter client org.apache.pulsar.client.api.MessageRouter %} interface, which has just one `choosePartition` method:

```java
public interface MessageRouter extends Serializable {
    int choosePartition(Message msg);
}
```

Here's a (not very useful!) router that routes every message to partition 10:

```java
public class AlwaysTenRouter implements MessageRouter {
    public int choosePartition(Message msg) {
        return 10;
    }
}
```

With that implementation in hand, you can send

```java
String pulsarBrokerRootUrl = "pulsar://localhost:6650";
String topic = "persistent://my-property/my-cluster-my-namespace/my-topic";

PulsarClient client = PulsarClient.create(pulsarBrokerRootUrl);
ProducerConfiguration config = new ProducerConfiguration();
config.setMessageRouter(AlwaysTenRouter);
Producer producer = client.createProducer(topic, config);
producer.send("Partitioned topic message".getBytes());
```


## Pulsar admin setup

{% include explanations/admin-setup.md %}

## Managing partitioned topics

{% include explanations/partitioned-topic-admin.md %}

## Concepts

{% include explanations/partitioned-topics.md %}
