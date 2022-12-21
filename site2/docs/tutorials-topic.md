---
Id: tutorials-topic
title: How to create a topic
sidebar_label: "Tutorials"
---


Apache Pulsar is a distributed messaging system that supports high performance and low latency. Topics are the primary way to structure data in Apache Pulsar. A Pulsar topic is a unit of storage that organizes messages into a stream. Each message in a topic has an offset, which uniquely identifies the message within the topic. 

## Prerequisites
[Publish to partitioned topics](admin-api-topics.md#publish-to-partitioned-topics)

## Create a topic

1. Create `test-topic` with 4 partitions in the namespace `apache/pulsar`.

   ```bash
   bin/pulsar-admin topics create-partitioned-topic apache/pulsar/test-topic -p 4
   ```

2. List all the partitioned topics in the namespace `apache/pulsar`.

   ```bash
   bin/pulsar-admin topics list-partitioned-topics apache/pulsar
   ```

#### Related Topics

- [How to set up a tenant](tutorials-tenant.md)
- [How to create a namespace](tutorials-namespace.md)
- [How to produce and consume messages](tutorial-produce-consume.md)








