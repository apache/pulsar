---
title: Topic compaction
tags: [admin, clients, compaction]
---

Pulsar's [topic compaction](../../getting-started/ConceptsAndArchitecture#compaction) feature enables you to create **compacted** topics in which older, "obscured" entries are pruned from the topic, allowing for faster reads through the topic's history (which messages are deemed obscured/outdated/irrelevant will depend on your use case).

To use compaction:

* You must manually [trigger](#trigger) compaction using the Pulsar administrative API. This will both run a compaction operation *and* mark the topic as a compacted topic.
* Your {% popover consumers %} must be [configured](#config) to read from compacted topics (or else the messages won't be properly read/processed/acknowledged).

## When should I use compacted topics?

The classic example of a topic that could benefit from compaction would be a stock ticker topic. In such a topic, you only care about the most recent value of each stock; "historical values" don't matter, so there's no need to read through outdated data when processing a topic's messages.

In Pulsar, topic compaction takes place on a *per-key basis*, meaning that messages are compacted based on their key. For the stock ticker use case, the stock symbol---e.g. `AAPL` or `GOOG`---could serve as the key.

{% include admonition.html type="warning" content="Compaction only works on topics where each message has a key (as in the stock ticker example, where the stock symbol serves as the key). Keys can be thought of as the axis along which compaction is applied." %}

## When should I trigger compaction?

How often you trigger compaction will vary widely based on the use case. If you want a compacted topic to be extremely speedy on read, then you should run compaction fairly frequently.

## Which messages get compacted?

When you [trigger](#trigger) compaction on a topic, all messages with the following

{% include admonition.html type="warning" title="Message keys are required"
content="Messages that don't have keys are *never* compacted. In order to use compaction, you'll need to come up with some kind of key-based scheme for messages on the topic." %}

## Triggering compaction {#trigger}

In order to run compaction on a topic, you need to use the [`topics compact`](../../CliTools#pulsar-admin-topics-compact) command for the [`pulsar-admin`](../../CliTools#pulsar-admin) CLI tool. Here's an example:

```bash
$ bin/pulsar-admin topics compact \
  persistent://my-tenant/my-namespace/my-topic
```

The `pulsar-admin` tool runs compaction via the Pulsar [REST API](../../reference/RestApi). To run compaction locally, i.e. *not* through the REST API, you can use the [`pulsar compact-topic`](../../CliTools#pulsar-compact-topic) command. Here's an example:

```bash
$ bin/pulsar compact-topic \
  --topic persistent://my-tenant-namespace/my-topic
```

The `pulsar compact-topic` command communicates with [ZooKeeper](https://zookeeper.apache.org) directly. In order to establish communication with ZooKeeper, though, the `pulsar` CLI tool will need to have a valid [broker configuration](../../Configuration#broker). You can either supply a proper configuration in `conf/broker.conf` or specify a non-default location for the configuration:

```bash
$ bin/pulsar compact-topic \
  --broker-conf /path/to/broker.conf \
  --topic persistent://my-tenant/my-namespace/my-topic

# If the configuration is in conf/broker.conf
$ bin/pulsar compact-topic \
  --topic persistent://my-tenant/my-namespace/my-topic
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

In order to read from a compacted topic using a Java consumer, the `readCompacted` parameter must be set to `true`. Here's an example consumer for a compacted topic:

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