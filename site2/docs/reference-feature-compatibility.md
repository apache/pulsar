---
id: reference-feature-compatibility
title: Feature Compatibility Matrices
sidebar_label: Feature Compatibility
---

<style type="text/css">
  table{
    font-size: 80%;
  }
</style>

Most Pulsar features can be freely mixed and matched, but some features cannot be used in combination with some others. The tables below shed light on the compatibility of Pulsar features.

## By Topic Type

|                                                                                        | persistent | [non-persistent](concepts-messaging.md#non-persistent-topics) | [partitioned](concepts-messaging.md#partitioned-topics) |
|-------------------------------------------------------------------------------------- |---------- |------------------------------------------------------------- |------------------------------------------------------- |
| [partitioned topic](concepts-messaging.md#partitioned-topics)                          | ✅         | ✅                                                            |                                                         |
| cumulative [acknowledgement](concepts-messaging.md#acknowledgement)                    | ✅         |                                                               |                                                         |
| [multi-topic/regex subscription](concepts-messaging.md#multi-topic-subscriptions)      | ✅         |                                                               |                                                         |
| [reader](concepts-clients.md#reader-interface)                                         | ✅         | ✅                                                            | ❌\*1                                                   |
| [deduplication](concepts-messaging.md#message-deduplication)                           | ✅         |                                                               |                                                         |
| [delayed delivery](concepts-messaging.md#delayed-message-delivery)                     | ✅         |                                                               |                                                         |
| [exclusive subscription](concepts-messaging.md#exclusive)                              | ✅         |                                                               |                                                         |
| [failover subscription](concepts-messaging.md#failover)                                | ✅         |                                                               |                                                         |
| [shared subscription](concepts-messaging.md#shared)                                    | ✅         |                                                               |                                                         |
| [key<sub>shared</sub> subscription](concepts-messaging.md#key<sub>shared</sub>) (beta) | ✅         |                                                               |                                                         |

\*1 Readers cannot be used to subscribe to a partitioned topic directly, but as partitioned topics are internally implemented as multiple topics, one reader per partition can be created to listen on the internal partition topics readers can s cannot

## By Subscription Type

|                                                                                   | [exclusive subscription](concepts-messaging.md#exclusive) | [failover subscription](concepts-messaging.md#failover) | [shared subscription](concepts-messaging.md#shared) | [key<sub>shared</sub> subscription](concepts-messaging.md#key<sub>shared</sub>) (beta) |
|--------------------------------------------------------------------------------- |--------------------------------------------------------- |------------------------------------------------------- |--------------------------------------------------- |-------------------------------------------------------------------------------------- |
| persistent                                                                        | ✅                                                        | ✅                                                      | ✅                                                  | ✅                                                                                     |
| [non-persistent](concepts-messaging.md#non-persistent-topics)                     | ✅                                                        |                                                         |                                                     |                                                                                        |
| [partitioned](concepts-messaging.md#partitioned-topics)                           | ✅                                                        |                                                         |                                                     |                                                                                        |
| cumulative [acknowledgement](concepts-messaging.md#acknowledgement)               | ✅                                                        | ✅                                                      | ✅                                                  | ❌                                                                                     |
| [multi-topic/regex subscription](concepts-messaging.md#multi-topic-subscriptions) | ✅                                                        |                                                         |                                                     |                                                                                        |
| [reader](concepts-clients.md#reader-interface)                                    | ✅                                                        |                                                         |                                                     |                                                                                        |
| [deduplication](concepts-messaging.md#message-deduplication)                      | ✅                                                        |                                                         |                                                     |                                                                                        |
| [delayed delivery](concepts-messaging.md#delayed-message-delivery)                | ✅                                                        |                                                         |                                                     |                                                                                        |
