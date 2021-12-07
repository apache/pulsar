---
id: txn-why
title: Why transactions?
sidebar_label: "Why transactions?"
---

Pulsar transactions (txn) enable event streaming applications to consume, process, and produce messages in one atomic operation. The reason for developing this feature can be summarized as below.

## Demand of stream processing

The demand for stream processing applications with stronger processing guarantees has grown along with the rise of stream processing. For example, in the financial industry, financial institutions use stream processing engines to process debits and credits for users. This type of use case requires that every message is processed exactly once, without exception.

In other words, if a stream processing application consumes message A and
produces the result as a message B (B = f(A)), then exactly-once processing
guarantee means that A can only be marked as consumed if and only if B is
successfully produced, and vice versa.

![](/assets/txn-1.png)

The Pulsar transactions API strengthens the message delivery semantics and the processing guarantees for stream processing. It enables stream processing applications to consume, process, and produce messages in one atomic operation. That means, a batch of messages in a transaction can be received from, produced to and acknowledged by many topic partitions. All the operations involved in a transaction succeed or fail as one single until.

## Limitation of idempotent producer

Avoiding data loss or duplication can be achieved by using the Pulsar idempotent producer, but it does not provide guarantees for writes across multiple partitions. 

In Pulsar, the highest level of message delivery guarantee is using an [idempotent producer](https://pulsar.apache.org/docs/en/next/concepts-messaging/#producer-idempotency) with the exactly once semantic at one single partition, that is, each message is persisted exactly once without data loss and duplication. However, there are some limitations in this solution:

- Due to the monotonic increasing sequence ID, this solution only works on a single partition and within a single producer session (that is, for producing one message), so there is no atomicity when producing multiple messages to one or multiple partitions. 
  
  In this case, if there are some failures  (for example, client / broker / bookie crashes, network failure, and more) in the process of producing and receiving messages, messages are re-processed and re-delivered, which may cause data loss or data duplication: 

  - For the producer: if the producer retry sending messages, some messages are persisted multiple times; if the producer does not retry sending messages, some messages are persisted once and other messages are lost. 
  
  - For the consumer: since the consumer does not know whether the broker has received messages or not, the consumer may not retry sending acks, which causes it to receive duplicate messages.  

- Similarly, for Pulsar Function, it only guarantees exactly once semantics for an idempotent function on a single event rather than processing multiple events or producing multiple results that can happen exactly. 

  For example, if a function accepts multiple events and produces one result (for example, window function), the function may fail between producing the result and acknowledging the incoming messages, or even between acknowledging individual events, which causes all (or some) incoming messages to be re-delivered and reprocessed, and a new result is generated.

  However, many scenarios need atomic guarantees across multiple partitions and sessions.

- Consumers need to rely on more mechanisms to acknowledge (ack) messages once. 
  
  For example, consumers are required to store the MessgeID along with its acked state. After the topic is unloaded, the subscription can recover the acked state of this MessgeID in memory when the topic is loaded again.