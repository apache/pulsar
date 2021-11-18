---
id: reference-terminology
title: Pulsar Terminology
sidebar_label: "Terminology"
original_id: reference-terminology
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


Here is a glossary of terms related to Apache Pulsar:

### Concepts

#### Pulsar

Pulsar is a distributed messaging system originally created by Yahoo but now under the stewardship of the Apache Software Foundation.

#### Message

Messages are the basic unit of Pulsar. They're what [producers](#producer) publish to [topics](#topic)
and what [consumers](#consumer) then consume from topics.

#### Topic

A named channel used to pass messages published by [producers](#producer) to [consumers](#consumer) who
process those [messages](#message).

#### Partitioned Topic

A topic that is served by multiple Pulsar [brokers](#broker), which enables higher throughput.

#### Namespace

A grouping mechanism for related [topics](#topic).

#### Namespace Bundle

A virtual group of [topics](#topic) that belong to the same [namespace](#namespace). A namespace bundle
is defined as a range between two 32-bit hashes, such as 0x00000000 and 0xffffffff.

#### Tenant

An administrative unit for allocating capacity and enforcing an authentication/authorization scheme.

#### Subscription

A lease on a [topic](#topic) established by a group of [consumers](#consumer). Pulsar has four subscription
modes (exclusive, shared, failover and key_shared).

#### Pub-Sub

A messaging pattern in which [producer](#producer) processes publish messages on [topics](#topic) that
are then consumed (processed) by [consumer](#consumer) processes.

#### Producer

A process that publishes [messages](#message) to a Pulsar [topic](#topic).

#### Consumer

A process that establishes a subscription to a Pulsar [topic](#topic) and processes messages published
to that topic by [producers](#producer).

#### Reader

Pulsar readers are message processors much like Pulsar [consumers](#consumer) but with two crucial differences:

- you can specify *where* on a topic readers begin processing messages (consumers always begin with the latest
  available unacked message);
- readers don't retain data or acknowledge messages.

#### Cursor

The subscription position for a [consumer](#consumer).

#### Acknowledgment (ack)

A message sent to a Pulsar broker by a [consumer](#consumer) that a message has been successfully processed.
An acknowledgement (ack) is Pulsar's way of knowing that the message can be deleted from the system;
if no acknowledgement, then the message will be retained until it's processed.

#### Negative Acknowledgment (nack)

When an application fails to process a particular message, it can send a "negative ack" to Pulsar
to signal that the message should be replayed at a later timer. (By default, failed messages are
replayed after a 1 minute delay). Be aware that negative acknowledgment on ordered subscription types,
such as Exclusive, Failover and Key_Shared, can cause failed messages to arrive consumers out of the original order.

#### Unacknowledged

A message that has been delivered to a consumer for processing but not yet confirmed as processed by the consumer.

#### Retention Policy

Size and time limits that you can set on a [namespace](#namespace) to configure retention of [messages](#message)
that have already been [acknowledged](#acknowledgement-ack).

#### Multi-Tenancy

The ability to isolate [namespaces](#namespace), specify quotas, and configure authentication and authorization
on a per-[tenant](#tenant) basis.

### Architecture

#### Standalone

A lightweight Pulsar broker in which all components run in a single Java Virtual Machine (JVM) process. Standalone
clusters can be run on a single machine and are useful for development purposes.

#### Cluster

A set of Pulsar [brokers](#broker) and [BookKeeper](#bookkeeper) servers (aka [bookies](#bookie)).
Clusters can reside in different geographical regions and replicate messages to one another
in a process called [geo-replication](#geo-replication).

#### Instance

A group of Pulsar [clusters](#cluster) that act together as a single unit.

#### Geo-Replication

Replication of messages across Pulsar [clusters](#cluster), potentially in different datacenters
or geographical regions.

#### Configuration Store

Pulsar's configuration store (previously known as configuration store) is a ZooKeeper quorum that
is used for configuration-specific tasks. A multi-cluster Pulsar installation requires just one
configuration store across all [clusters](#cluster).

#### Topic Lookup

A service provided by Pulsar [brokers](#broker) that enables connecting clients to automatically determine
which Pulsar [cluster](#cluster) is responsible for a [topic](#topic) (and thus where message traffic for
the topic needs to be routed).

#### Service Discovery

A mechanism provided by Pulsar that enables connecting clients to use just a single URL to interact
with all the [brokers](#broker) in a [cluster](#cluster).

#### Broker

A stateless component of Pulsar [clusters](#cluster) that runs two other components: an HTTP server
exposing a REST interface for administration and topic lookup and a [dispatcher](#dispatcher) that
handles all message transfers. Pulsar clusters typically consist of multiple brokers.

#### Dispatcher

An asynchronous TCP server used for all data transfers in-and-out a Pulsar [broker](#broker). The Pulsar
dispatcher uses a custom binary protocol for all communications.

### Storage

#### BookKeeper

[Apache BookKeeper](http://bookkeeper.apache.org/) is a scalable, low-latency persistent log storage
service that Pulsar uses to store data.

#### Bookie

Bookie is the name of an individual BookKeeper server. It is effectively the storage server of Pulsar.

#### Ledger

An append-only data structure in [BookKeeper](#bookkeeper) that is used to persistently store messages in Pulsar [topics](#topic).

### Functions

Pulsar Functions are lightweight functions that can consume messages from Pulsar topics, apply custom processing logic, and, if desired, publish results to topics.
