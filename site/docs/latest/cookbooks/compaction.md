---
title: Topic compaction cookbook
tags: [admin, clients, compaction]
---

Pulsar's [topic compaction](../../getting-started/ConceptsAndArchitecture#compaction) feature enables you to create **compacted** topics in which older, "obscured" entries are pruned from the topic, allowing for faster reads through the topic's history (which messages are deemed obscured/outdated/irrelevant will depend on your use case).

To use compaction:

* You need to give messages keys, as topic compaction in Pulsar takes place on a *per-key basis* (i.e. messages are compacted based on their key). For a stock ticker use case, the stock symbol---e.g. `AAPL` or `GOOG`---could serve as the key (more on this [below](#when)). Messages without keys will be left alone by the compaction process.
* Compaction can be configured to run [automatically](#automatic), or you can manually [trigger](#trigger) compaction using the Pulsar administrative API.
* Your {% popover consumers %} must be [configured](#config) to read from compacted topics ([Java consumers](#java), for example, have a `readCompacted` setting that must be set to `true`). If this configuration is not set, consumers will still be able to read from the non-compacted topic.

## When should I use compacted topics? {#when}

The classic example of a topic that could benefit from compaction would be a stock ticker topic through which {% popover consumers %} can access up-to-date values for specific stocks. On a stock ticker topic you only care about the most recent value of each stock; "historical values" don't matter, so there's no need to read through outdated data when processing a topic's messages.

{% include admonition.html type="info" content="For topics where older values are important, for example when you need to process a long series of messages in order, many of which have the same key, compaction is unnecessary and could possibly even be harmful." %}

{% include admonition.html type="warning" content="Compaction only works on topics where each message has a key (as in the stock ticker example, where the stock symbol serves as the key). Keys can be thought of as the axis along which compaction is applied." %}

## Configuring compaction to run automatically {#automatic}

Tenant administrators can configure a policy for compaction at the namespace level. The policy specifies how large the topic backlog can grow before compaction is triggered.

For example, to trigger compaction when the backlog reaches 100MB:

```bash
$ bin/pulsar-admin namespaces set-compaction-threshold \
  --threshold 100M my-tenant/my-namespace
```

Configuring the compaction threshold on a namespace will apply to all topics within that namespace.

## Triggering compaction manually {#trigger}

In order to run compaction on a topic, you need to use the [`topics compact`](../../CliTools#pulsar-admin-topics-compact) command for the [`pulsar-admin`](../../CliTools#pulsar-admin) CLI tool. Here's an example:

```bash
$ bin/pulsar-admin topics compact \
  persistent://my-tenant/my-namespace/my-topic
```

The `pulsar-admin` tool runs compaction via the Pulsar [REST API](../../reference/RestApi). To run compaction in its own dedicated process, i.e. *not* through the REST API, you can use the [`pulsar compact-topic`](../../CliTools#pulsar-compact-topic) command. Here's an example:

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

#### When should I trigger compaction?

How often you [trigger compaction](#trigger) will vary widely based on the use case. If you want a compacted topic to be extremely speedy on read, then you should run compaction fairly frequently.

## Consumer configuration {#config}

Pulsar consumers and readers need to be configured to read from compacted topics. The sections below show you how to enable compacted topic reads for Pulsar's language clients. If the

{% include admonition.html type="warning" title="Java only" content="Currently, only [Java](#java) clients can consume messages from compacted topics." %}

### Java

In order to read from a compacted topic using a Java consumer, the `readCompacted` parameter must be set to `true`. Here's an example consumer for a compacted topic:

```java
Consumer<byte[]> compactedTopicConsumer = client.newConsumer()
        .topic("some-compacted-topic")
        .readCompacted(true)
        .subscribe();
```

As mentioned above, topic compaction in Pulsar works on a *per-key basis*. That means that messages that you produce on compacted topics need to have keys (the content of the key will depend on your use case). Messages that don't have keys will be ignored by the compaction process. Here's an example Pulsar message with a key:

```java
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageBuilder;

Message<byte[]> msg = MessageBuilder.create()
        .setContent(someByteArray)
        .setKey("some-key")
        .build();
```

The example below shows a message with a key being produced on a compacted Pulsar topic:

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

compactedTopicProducer.send(msg);
```