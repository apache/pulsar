---
title: Topic compaction
tags: [admin, clients, compaction]
---

## Admin

## Clients

{% include admonition.html type="warning" title="Java only" content="Currently, only [Java](#java) clients can consume messages from compacted topics." %}

### Java

[Java client](../../clients/Java)

```java
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageBuilder;

Message<byte[]> msg = MessageBuilder.create()
        .setContent(someByteArray)
        .setKey("some-key")
        .build();
```

```java
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;

PulsarClient client = PulsarClient.builder()
        .serviceUrl("pulsar://localhost:6650")
        .build();

Producer<byte[]> compactedTopicProducer = client.newProducer()
        .topic("some-compacted-topic")
        .create();

Message<byte[]> msg = MessageBuilder.create()
        .setContent(someByteArray)
        .setKey("some-key")
        .build();
```

In order to 

```java

Consumer<byte[]> compactedTopicConsumer = client.newConsumer()
        .topic("some-compacted-topic")
        .readCompacted(true)
        .subscribe();
```