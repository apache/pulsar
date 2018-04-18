---
title: Topic compaction
tags: [admin, clients, compaction]
---

## Admin



[`pulsar-admin topics compact`](../../CliTools#pulsar-admin-topics-compact)

```bash
$ bin/pulsar-admin topics compact \
  persistent://my-tenant/my-namespace/my-topic
```

To run compaction locally, i.e. *not* through the Pulsar [REST API](../../reference/RestApi), you can use the [`pulsar compact-topic`](../../CliTools#pulsar-compact-topic) command. Here's an example:

```bash
$ bin/pulsar compact-topic \
  --topic persistent://my-tenant-namespace/my-topic
```

This command communicates with ZooKeeper directly. In order to establish communication with ZooKeeper, though, you'll need to have a valid [broker configuration](../../Configuration#broker)

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