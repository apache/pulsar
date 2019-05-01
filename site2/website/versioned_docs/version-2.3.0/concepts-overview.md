---
id: version-2.3.0-concepts-overview
title: Pulsar Overview
sidebar_label: Overview
original_id: concepts-overview
---

Pulsar is a multi-tenant, high-performance solution for server-to-server messaging originally developed by [Yahoo](http://yahoo.github.io/) and now under the stewardship of the [Apache Software Foundation](https://www.apache.org/).

Pulsar's key features include:

* Native support for multiple clusters in a Pulsar instance, with seamless [geo-replication](administration-geo.md) of messages across clusters
* Very low publish and end-to-end latency
* Seamless scalability out to over a million topics
* A simple [client API](concepts-clients.md) with bindings for [Java](client-libraries-java.md), [Go](client-libraries-go.md), [Python](client-libraries-python.md) and [C++](client-libraries-cpp.md)
* Multiple [subscription modes](concepts-messaging.md#subscription-modes) for topics ([exclusive](concepts-messaging.md#exclusive), [shared](concepts-messaging.md#shared), and [failover](concepts-messaging.md#failover))
* Guaranteed message delivery with [persistent message storage](concepts-architecture-overview.md#persistent-storage) provided by [Apache BookKeeper](http://bookkeeper.apache.org/)
* A serverless lightweight computing framework [Pulsar Functions](functions-overview.md) offers stream native data processing.
* A serverless connector framework [Pulsar IO](io-overview.md) built on-top-of Pulsar Functions to make moving data in and out Apache Pulsar easier.
* [Tiered Storage](concepts-tiered-storage.md) offloads data from hot/warm storage to cold/longterm storage (such as S3 and GCS) when the data is aging out.

## Contents

- [Messaging Concepts](concepts-messaging.md)
- [Architecture Overview](concepts-architecture-overview.md)
- [Pulsar Clients](concepts-clients.md)
- [Geo Replication](concepts-replication.md)
- [Multi Tenancy](concepts-multi-tenancy.md)
- [Authentication and Authorization](concepts-authentication.md)
- [Topic Compaction](concepts-topic-compaction.md)
- [Tiered Storage](concepts-tiered-storage.md)
- [Schema Registry](concepts-schema-registry.md)
