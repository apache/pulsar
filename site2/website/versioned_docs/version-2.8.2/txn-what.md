---
id: version-2.8.2-txn-what
title: What are transactions?
sidebar_label: What are transactions?
original_id: txn-what
---

Transactions strengthen the message delivery semantics of Apache Pulsar and [processing guarantees of Pulsar Functions](https://pulsar.apache.org/docs/en/next/functions-overview/#processing-guarantees). The Pulsar Transaction API supports atomic writes and acknowledgments across multiple topics. 

Transactions allow:

- A producer to send a batch of messages to multiple topics where all messages in the batch are eventually visible to any consumer, or none are ever visible to consumers. 

- End-to-end exactly-once semantics (execute a `consume-process-produce` operation exactly once).

## Transaction semantics

Pulsar transactions have the following semantics: 

* All operations within a transaction are committed as a single unit.
   
  * Either all messages are committed, or none of them are. 

  * Each message is written or processed exactly once, without data loss or duplicates (even in the event of failures). 

  * If a transaction is aborted, all the writes and acknowledgments in this transaction rollback.
  
* A group of messages in a transaction can be received from, produced to, and acknowledged by multiple partitions.
  
  * Consumers are only allowed to read committed (acked) messages. In other words, the broker does not deliver transactional messages which are part of an open transaction or messages which are part of an aborted transaction.
    
  * Message writes across multiple partitions are atomic.
    
  * Message acks across multiple subscriptions are atomic. A message is acked successfully only once by a consumer under the subscription when acknowledging the message with the transaction ID.

## Transactions and stream processing

Stream processing on Pulsar is a `consume-process-produce` operation on Pulsar topics:

* `Consume`: a source operator that runs a Pulsar consumer reads messages from one or multiple Pulsar topics.
  
* `Process`: a processing operator transforms the messages. 
  
* `Produce`: a sink operator that runs a Pulsar producer writes the resulting messages to one or multiple Pulsar topics.

![](assets/txn-2.png)

Pulsar transactions support end-to-end exactly-once stream processing, which means messages are not lost from a source operator and messages are not duplicated to a sink operator.

## Use case

Prior to Pulsar 2.8.0, there was no easy way to build stream processing applications with Pulsar to achieve exactly-once processing guarantees. With the transaction introduced in Pulsar 2.8.0, the following services support exactly-once semantics:

* [Pulsar Flink connector](https://flink.apache.org/2021/01/07/pulsar-flink-connector-270.html)

    Prior to Pulsar 2.8.0, if you want to build stream applications using Pulsar and Flink, the Pulsar Flink connector only supported exactly-once source connector and at-least-once sink connector, which means the highest processing guarantee for end-to-end was at-least-once, there was possibility that the resulting messages from streaming applications produce duplicated messages to the resulting topics in Pulsar.

    With the transaction introduced in Pulsar 2.8.0, the Pulsar Flink sink connector can support exactly-once semantics by implementing the designated `TwoPhaseCommitSinkFunction` and hooking up the Flink sink message lifecycle with Pulsar transaction API. 

* Support for Pulsar Functions and other connectors will be added in the future releases.
