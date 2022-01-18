---
author: Sijie Guo
authorURL: https://twitter.com/sijieg
title: Apache Pulsar 2.1.0-incubating
---

We are glad to present the new 2.1.0-incubating release of Pulsar.
This release is the culmination of 2 months of work that have
brought multiple new features and improvements to Pulsar. 

In Pulsar 2.1 you'll see:

- [Pulsar IO](/docs/io-overview) connector framework and a list of [builtin connectors](/docs/io-connectors)
- [PIP-17](https://github.com/apache/incubator-pulsar/wiki/PIP-17:-Tiered-storage-for-Pulsar-topics): [Tiered Storage](/docs/concepts-tiered-storage)
- Pulsar [Stateful Functions](/docs/functions-state)
- [Go Client](/docs/client-libraries-go)
- [Avro](https://github.com/apache/incubator-pulsar/blob/v2.1.0-incubating/pulsar-client-schema/src/main/java/org/apache/pulsar/client/impl/schema/AvroSchema.java)
  and [Protobuf](https://github.com/apache/incubator-pulsar/blob/v2.1.0-incubating/pulsar-client-schema/src/main/java/org/apache/pulsar/client/impl/schema/ProtobufSchema.java) Schemas

For details information please check the detailed [release notes](/release-notes/#2.1.0-incubating) and [2.1.0 documentation](/versions).

<!--truncate-->

We'll provide a brief summary of these features in the section below.

## Pulsar IO

Since Pulsar 2.0, we introduced a serverless inspired lightweight computing framework [Pulsar Functions](/docs/functions-overview),
providing the easiest possible way to implement application-specific in-stream processing logic of any complexity. A lot of developers
love Pulsar Functions because they require minimal boilerplate and are easy to reason about.

In Pulsar 2.1, we continued following this "simplicity first" principle on developing Pulsar. We developed this IO (input/output) connector
framework on top of Pulsar Functions, to simplify getting data in and out of Apache Pulsar. You don't need to write any single line of code.
All you need is prepare a configuration file of the system your want to connect to, and use Pulsar admin
CLI to submit a connector to Pulsar. Pulsar will take care of all the other stuffs, such as fault-tolerance, rebalancing and etc.

There are 6 built-in connectors released in 2.1 release. They are:

- [Aerospike Connector](/docs/io-aerospike/)
- [Cassandra Connector](/docs/io-cassandra/)
- [Kafka Connector](/docs/io-kafka/)
- [Kinesis Connector](/docs/io-kinesis/)
- [RabbitMQ Connector](/docs/io-rabbitmq/)
- [Twitter Firehose Connector](/docs/io-twitter/) 

You can follow [the tutorial](/docs/io-quickstart) to try out Pulsar IO on connecting Pulsar with [Apache Cassandra](http://cassandra.apache.org/).

More connectors will be coming in future releases. If you are interested in contributing a connector to Pulsar, checkout the guide on [Developing Connectors](/docs/io-develop).
It is as simple as writing a Pulsar function.

## Tiered Storage

One of the advantages of Apache Pulsar is [its segment storage](https://streaml.io/blog/pulsar-segment-based-architecture) using [Apache BookKeeper](https://bookkeeper.apache.org/). You can store a topic backlog as large as you want.
When the cluster starts to run out of space, you just add another storage node, and the system will automatically
pickup the new storage nodes and start using them without rebalancing partitions. However, this can start to get expensive after a while.

Pulsar mitigates this cost/size trade-off by providing Tiered Storage. Tiered Storage turns your Pulsar topics into real *infinite* streams,
by offloading older segments into a long term storage, such as AWS S3, GCS and HDFS, which is designed for storing cold data. To the end user,
there is no perceivable difference between consuming streams whose data is stored in BookKeeper or in long term storage. All the underlying
offloading mechanisms and metadata management are transparent to applications.

Currently [S3](https://aws.amazon.com/s3/) is supported in 2.1. More offloaders (such as Google GCS, Azure Blobstore, and HDFS) are coming
in future releases.

If you are interested in this feature, you can checkout more details [here](/docs/cookbooks-tiered-storage).

## Stateful Function

The greatest challenge that stream processing engines face is managing *state*. So does Pulsar Functions. As the goal for Pulsar Functions
is to simplify developing stream native processing logic, we also want to provide an easier way for Pulsar Functions to manage their state.
We introduced a set of [State API](/docs/functions-state/#api) for Pulsar Functions to store their state. It integrates with the table service
in Apache BookKeeper for storing the state.

It is released as a developer preview feature in Pulsar Functions Java SDK. We would like to collect feedback to improve it in future releases.

## Schemas

Pulsar 2.0 introduces native support for schemas in Pulsar. It means you can declare how message data looks and have Pulsar enforce that
producers can only publish valid data on the topics. In 2.0, Pulsar only supports `String`, `bytes` and `JSON` schemas. We introduced the
support for [Avro](https://avro.apache.org/) and [Protobuf](https://developers.google.com/protocol-buffers/) in this release. 

## Clients

We have introduced a new [Go](/docs/client-libraries-go) client in 2.1 release. The Pulsar Go client library is based on the [C++](/docs/client-libraries-cpp/) client library.

Follow [the instructions](/docs/client-libraries-go/#installing-go-package) to try it out in your Go applications!
