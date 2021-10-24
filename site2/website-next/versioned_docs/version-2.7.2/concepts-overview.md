---
id: concepts-overview
title: Pulsar Overview
sidebar_label: Overview
original_id: concepts-overview
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


Pulsar is a multi-tenant, high-performance solution for server-to-server messaging. Pulsar was originally developed by Yahoo, it is under the stewardship of the [Apache Software Foundation](https://www.apache.org/).

Key features of Pulsar are listed below:

* Native support for multiple clusters in a Pulsar instance, with seamless [geo-replication](administration-geo) of messages across clusters.
* Very low publish and end-to-end latency.
* Seamless scalability to over a million topics.
* A simple [client API](concepts-clients.md) with bindings for [Java](client-libraries-java.md), [Go](client-libraries-go.md), [Python](client-libraries-python.md) and [C++](client-libraries-cpp).
* Multiple [subscription modes](concepts-messaging.md#subscription-modes) ([exclusive](concepts-messaging.md#exclusive), [shared](concepts-messaging.md#shared), and [failover](concepts-messaging.md#failover)) for topics.
* Guaranteed message delivery with [persistent message storage](concepts-architecture-overview.md#persistent-storage) provided by [Apache BookKeeper](http://bookkeeper.apache.org/).
* A serverless light-weight computing framework [Pulsar Functions](functions-overview) offers the capability for stream-native data processing.
* A serverless connector framework [Pulsar IO](io-overview), which is built on Pulsar Functions, makes it easier to move data in and out of Apache Pulsar.
* [Tiered Storage](concepts-tiered-storage) offloads data from hot/warm storage to cold/longterm storage (such as S3 and GCS) when the data is aging out.

## Contents

- [Messaging Concepts](concepts-messaging)
- [Architecture Overview](concepts-architecture-overview)
- [Pulsar Clients](concepts-clients)
- [Geo Replication](concepts-replication)
- [Multi Tenancy](concepts-multi-tenancy)
- [Authentication and Authorization](concepts-authentication)
- [Topic Compaction](concepts-topic-compaction)
- [Tiered Storage](concepts-tiered-storage)
