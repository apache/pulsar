---
id: cookbooks-compaction
title: Topic compaction
sidebar_label: "Topic compaction"
original_id: cookbooks-compaction
---

Pulsar's [topic compaction](concepts-topic-compaction.md#compaction) feature enables you to create **compacted** topics in which older, "obscured" entries are pruned from the topic, allowing for faster reads through the topic's history (which messages are deemed obscured/outdated/irrelevant will depend on your use case).

To use compaction:

* You need to give messages keys, as topic compaction in Pulsar takes place on a *per-key basis* (i.e. messages are compacted based on their key). For a stock ticker use case, the stock symbol---e.g. `AAPL` or `GOOG`---could serve as the key (more on this [below](#when)). Messages without keys will be left alone by the compaction process.
* Compaction can be configured to run [automatically](#automatic), or you can manually [trigger](#trigger) compaction using the Pulsar administrative API.
* Your consumers must be [configured](#config) to read from compacted topics ([Java consumers](#java), for example, have a `readCompacted` setting that must be set to `true`). If this configuration is not set, consumers will still be able to read from the non-compacted topic.


> Compaction only works on messages that have keys (as in the stock ticker example the stock symbol serves as the key for each message). Keys can thus be thought of as the axis along which compaction is applied. Messages that don't have keys are simply ignored by compaction.

## When should I use compacted topics? {#when}

The classic example of a topic that could benefit from compaction would be a stock ticker topic through which consumers can access up-to-date values for specific stocks. Imagine a scenario in which messages carrying stock value data use the stock symbol as the key (`GOOG`, `AAPL`, `TWTR`, etc.). Compacting this topic would give consumers on the topic two options:

* They can read from the "original," non-compacted topic in case they need access to "historical" values, i.e. the entirety of the topic's messages.
* They can read from the compacted topic if they only want to see the most up-to-date messages.

Thus, if you're using a Pulsar topic called `stock-values`, some consumers could have access to all messages in the topic (perhaps because they're performing some kind of number crunching of all values in the last hour) while the consumers used to power the real-time stock ticker only see the compacted topic (and thus aren't forced to process outdated messages). Which variant of the topic any given consumer pulls messages from is determined by the consumer's [configuration](#config).

> One of the benefits of compaction in Pulsar is that you aren't forced to choose between compacted and non-compacted topics, as the compaction process leaves the original topic as-is and essentially adds an alternate topic. In other words, you can run compaction on a topic and consumers that need access to the non-compacted version of the topic will not be adversely affected.


## Configuring compaction to run automatically {#automatic}

Tenant administrators can configure a policy for compaction at the namespace level. The policy specifies how large the topic backlog can grow before compaction is triggered.

For example, to trigger compaction when the backlog reaches 100MB:

```bash

$ bin/pulsar-admin namespaces set-compaction-threshold \
  --threshold 100M my-tenant/my-namespace

```

Configuring the compaction threshold on a namespace will apply to all topics within that namespace.

## Triggering compaction manually {#trigger}

In order to run compaction on a topic, you need to use the [`topics compact`](reference-pulsar-admin.md#topics-compact) command for the [`pulsar-admin`](reference-pulsar-admin) CLI tool. Here's an example:

```bash

$ bin/pulsar-admin topics compact \
  persistent://my-tenant/my-namespace/my-topic

```

The `pulsar-admin` tool runs compaction via the Pulsar {@inject: rest:REST:/} API. To run compaction in its own dedicated process, i.e. *not* through the REST API, you can use the [`pulsar compact-topic`](reference-cli-tools.md#pulsar-compact-topic) command. Here's an example:

```bash

$ bin/pulsar compact-topic \
  --topic persistent://my-tenant-namespace/my-topic

```

> Running compaction in its own process is recommended when you want to avoid interfering with the broker's performance. Broker performance should only be affected, however, when running compaction on topics with a large keyspace (i.e when there are many keys on the topic). The first phase of the compaction process keeps a copy of each key in the topic, which can create memory pressure as the number of keys grows. Using the `pulsar-admin topics compact` command to run compaction through the REST API should present no issues in the overwhelming majority of cases; using `pulsar compact-topic` should correspondingly be considered an edge case.

The `pulsar compact-topic` command communicates with [ZooKeeper](https://zookeeper.apache.org) directly. In order to establish communication with ZooKeeper, though, the `pulsar` CLI tool will need to have a valid [broker configuration](reference-configuration.md#broker). You can either supply a proper configuration in `conf/broker.conf` or specify a non-default location for the configuration:

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


> #### Java only
> Currently, only [Java](#java) clients can consume messages from compacted topics.


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

