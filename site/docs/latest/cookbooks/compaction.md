---
title: Topic compaction
tags: [admin, clients, compaction]
---

To use compaction:

* You must manually [trigger](#trigger) compaction using the Pulsar administrative API
* Your {% popover consumers %} must be [configured](#config) to read compacted topics

## When should I use compacted topics?

The classic example of a topic that could benefit from compaction would be a stock ticker topic. In such a topic, you only care about the most recent value.

## Which messages get compacted?

When you [trigger](#trigger) compaction on a topic, all messages with the following

{% include admonition.html type="warning" title="Message keys are required"
content="Messages that don't have keys are *never* compacted." %}

If a message with a key

## Triggering compaction {#trigger}

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

This command communicates with ZooKeeper directly. In order to establish communication with ZooKeeper, though, the `pulsar` CLI tool will need to have a valid [broker configuration](../../Configuration#broker). You can either supply a proper configuration in `conf/broker.conf` or specify a non-default location for the configuration:

```bash
$ bin/pulsar compact-topic \
  --broker-conf /path/to/broker.conf \
  --topic persistent://my-tenant-namespace/my-topic
```

## Consumer configuration {#config}

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

```java

Consumer<byte[]> compactedTopicConsumer = client.newConsumer()
        .topic("some-compacted-topic")
        .readCompacted(true)
        .subscribe();
```

#### Stock ticker example

Below is a "stock ticker" example for Java;

```java
Arrays.asList("GOOG", "FB", "AAPL").forEach(stockSymbol -> {
        Message<byte[]> msg = MessageBuilder.create()
                .setContent(someByteArray)
                .setKey(stockSymbol)
                .build();
        producer.send(msg);
});
```

If you send this no-payload message, all messages earlier than this message with the key `GOOG` will be removed from the compacted topic:

```java
Message<byte[]> noPayload = MessageBuilder.create()
        .setContent(new byte[0])
        .setKey("GOOG")
        .build();
```