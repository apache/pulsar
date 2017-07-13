---
title: Partitioned topics
lead: Expand message throughput by distributing load within topics
tags:
- topics
- partitioning
- admin
- clients
---

Normal topics can be served only by a single {% popover broker %}, which limits the topic's maximum throughput. *Partitioned topics* are a special type of topic that can span multiple brokers and thus allow for much higher throughput.

Partitioned topics are [created and managed](#managing-partitioned-topics) using Pulsar's admin API.

## Publishing to partitioned topics

From the standpoint of message-producing clients, the only difference is that you need to specify a message router when you create a new producer. Examples for [Java](#java) are below.

### Java

Publishing messages to partitioned topics in the Java client works much like [publishing to normal topics](../../applications/JavaClient#using-producers). The difference is that you need to specify either one of the currently available message routers or a custom router.

#### Default routers

You can specify the routing mode in the {% javadoc ProducerConfiguration client com.yahoo.pulsar.client.api.ProducerConfiguration %} object that you use to configure your producer. You have three options:



The available routing modes are `SinglePartition`, `RoundRobinPartition`, and `CustomPartition`. Here's an example:

```java
String pulsarBrokerRootUrl = "pulsar://localhost:6650";
String topic = "persistent://sample/standalone/ns1/my-partitioned-topic";

PulsarClient client = PulsarClient.create(pulsarBrokerRootUrl);
ProducerConfiguration config = new ProducerConfiguration();
config.setMessageRoutingMode(SinglePartition);
Producer producer = client.createProducer(topic, config);
producer.send("Partitioned topic message".getBytes());
```

#### Custom message router

To use a custom message router, you need to provide an implementation of the {% javadoc MessageRouter client com.yahoo.pulsar.client.api.MessageRouter %} interface, which has just one `choosePartition` method:

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
String topic = "persistent://sample/standalone/ns1/my-partitioned-topic";

PulsarClient client = PulsarClient.create(pulsarBrokerRootUrl);
ProducerConfiguration config = new ProducerConfiguration();
config.setMessageRouter(AlwaysTenRouter);
Producer producer = client.createProducer(topic, config);
producer.send("Partitioned topic message".getBytes());
```

## Managing partitioned topics

{% include explanations/partitioned-topic-admin.md %}


## Concepts

{% include explanations/partitioned-topics.md %}
