---
id: cookbooks-non-persistent
title: Non-persistent messaging
sidebar_label: "Non-persistent messaging"
---

**Non-persistent topics** are Pulsar topics in which message data is *never* [persistently stored](concepts-architecture-overview.md#persistent-storage) and kept only in memory. This cookbook provides:

* A basic [conceptual overview](#overview) of non-persistent topics
* Information about [configurable parameters](#configuration) related to non-persistent topics
* A guide to the [CLI interface](#cli) for managing non-persistent topics

## Overview

By default, Pulsar persistently stores *all* unacknowledged messages on multiple [BookKeeper](#persistent-storage) bookies (storage nodes). Data for messages on persistent topics can thus survive broker restarts and subscriber failover.

Pulsar also, however, supports **non-persistent topics**, which are topics on which messages are *never* persisted to disk and live only in memory. When using non-persistent delivery, killing a Pulsar [broker](reference-terminology.md#broker) or disconnecting a subscriber to a topic means that all in-transit messages are lost on that (non-persistent) topic, meaning that clients may see message loss.

Non-persistent topics have names of this form (note the `non-persistent` in the name):

```http

non-persistent://tenant/namespace/topic

```

> For more high-level information about non-persistent topics, see the [Concepts and Architecture](concepts-messaging.md#non-persistent-topics) documentation.

## Using

> In order to use non-persistent topics, they must be [enabled](#enabling) in your Pulsar broker configuration.

In order to use non-persistent topics, you only need to differentiate them by name when interacting with them. This [`pulsar-client produce`](reference-cli-tools.md#pulsar-client-produce) command, for example, would produce one message on a non-persistent topic in a standalone cluster:

```bash

$ bin/pulsar-client produce non-persistent://public/default/example-np-topic \
  --num-produce 1 \
  --messages "This message will be stored only in memory"

```

> For a more thorough guide to non-persistent topics from an administrative perspective, see the [Non-persistent topics](admin-api-topics) guide.

## Enabling

In order to enable non-persistent topics in a Pulsar broker, the [`enableNonPersistentTopics`](reference-configuration.md#broker-enableNonPersistentTopics) must be set to `true`. This is the default, and so you won't need to take any action to enable non-persistent messaging.


> #### Configuration for standalone mode
> If you're running Pulsar in standalone mode, the same configurable parameters are available but in the [`standalone.conf`](reference-configuration.md#standalone) configuration file. 

If you'd like to enable *only* non-persistent topics in a broker, you can set the [`enablePersistentTopics`](reference-configuration.md#broker-enablePersistentTopics) parameter to `false` and the `enableNonPersistentTopics` parameter to `true`.

## Managing with cli

Non-persistent topics can be managed using the [`pulsar-admin non-persistent`](reference-pulsar-admin.md#non-persistent) command-line interface. With that interface you can perform actions like [create a partitioned non-persistent topic](reference-pulsar-admin.md#non-persistent-create-partitioned-topic), get [stats](reference-pulsar-admin.md#non-persistent-stats) for a non-persistent topic, [list](reference-pulsar-admin) non-persistent topics under a namespace, and more.

## Using with Pulsar clients

You shouldn't need to make any changes to your Pulsar clients to use non-persistent messaging beyond making sure that you use proper [topic names](#using) with `non-persistent` as the topic type.

