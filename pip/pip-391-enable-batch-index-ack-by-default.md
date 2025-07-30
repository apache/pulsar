## Background

### Default Approach to Acknowledge Batched Messages

A batched message is part of a batch stored in a BookKeeper entry. For instance, given a batch containing N messages, these N messages are stored in the same entry. The message IDs of these messages share the same ledger ID and entry ID, differing only in their batch indexes.

Consider the following code example:

```java
for (int i = 0; i < 3; i++) {
    producer.newMessage().value("msg-" + i).sendAsync();
}
producer.flush();
final var ids = new ArrayList<MessageId>();
for (int i = 0; i < 3; i++) {
    ids.add(consumer.receive().getMessageId());
}
consumer.acknowledge(ids.get(0));
```

Here, `ids` are the message IDs of the 3 messages in the same batch, with batch indexes 0, 1, and 2. These message IDs share the same bit set object with 3 bits set (represented as "111"). The side effect of `acknowledge(ids.get(0))` is to set the bit of batch index 0, changing the bit set to "011". Only when all bits are cleared will the consumer send an ACK request to the broker, indicating that the entry of this batch has been acknowledged.

If the `consumer` restarts after acknowledging `ids.get(0)`, the broker will dispatch this entry again, causing the `consumer` to receive duplicate messages:

```java
consumer.acknowledge(ids.get(0)); // the batch index of ids.get(0) is 0
consumer.close();
consumer = pulsarClient.newConsumer(Schema.STRING).topic(topic).subscriptionName("sub")
        .enableBatchIndexAcknowledgment(true).subscribe();
final var msg = consumer.receive(); // msg.getValue() will be "msg-0" and the batch index is 0
```

This behavior can be confusing. Users might assume that the message has been acknowledged and will not be received again.

Generally, if every received message is acknowledged, this issue does not arise because a consumer must be able to receive a whole batch. However, in some common use cases, this behavior can cause issues.

#### Case 1: Message ID Comes from Deserialization

Pulsar supports serializing the message ID into bytes via the `toByteArray()` method and recovering the message ID from the raw bytes via `fromByteArrayWithTopic()`. This is useful when storing the message ID in a database or other storage.

However, it is impossible to recover the bit set for message IDs in the same batch because they must share the same bit set object. The existing implementation creates a bit set for each batched message ID, which is meaningless. After acknowledging all of them, each message ID will have a separate bit set with only 1 bit cleared, e.g.:
- Bit set of `id0`: 011
- Bit set of `id1` 101
- Bit set of `id2`: 110

To solve this issue, a stateful message ID deserializer can be provided, which remembers if all message IDs in the same batch have been deserialized and only creates an ack set for the first message ID in the batch. However, this API would be complicated and not user-friendly as well. It would still not work for cases where serialized message IDs in the same batch are stored in different places.

#### Case 2: Negative Acknowledgment

When a consumer fails to process a message, Pulsar provides a `negativeAcknowledge` method to trigger the redelivery of that message after some time, determined by the `negativeAckRedeliveryDelay` config.

```java
consumer.acknowledge(ids.get(0));
consumer.acknowledge(ids.get(2));
consumer.negativeAcknowledge(ids.get(1));
final var msg = consumer.receive();
```

In this example, it is expected that `msg`s batch index is 1.

Unfortunately, with the default config, `msg`'s batch index will be 0 because Pulsar will dispatch the whole batch while the consumer is not able to filter out the acknowledged single messages whose message ID is `ids.get(0)` or `ids.get(2)`.

#### Summary

The default behavior of acknowledging batched messages is not user-friendly and might cause unexpected issues. When these issues arise, it is hard to understand the root cause without knowing the details of the implementation.

## Motivation

[PIP-54](https://github.com/apache/pulsar/wiki/PIP-54:-Support-acknowledgment-at-batch-index-level) introduces the ability to acknowledge messages at the batch index level. Two configurations control this behavior:
- Client side: `ConsumerBuilder#enableBatchIndexAcknowledgment`: false by default. When true, the client sends the batch index ACK request to the broker.
- Broker side: `acknowledgmentAtBatchIndexLevelEnabled`: false by default. When true, the broker handles the batch index ACK request from the client.

When both are enabled, the issues with the default ACK behaviors are resolved. However, these configurations are not enabled by default due to concerns about compatibility or performance:
1. The client with batch index ACK enabled cannot work with brokers before version 2.6.0.
2. The broker needs to maintain the ack set for each batched message ID in memory and persist it somewhere.
3. When `isAckReceiptEnabled` is true, each `acknowledge` call triggers a request to the broker and waits for the response.

For issue 1, it is not a significant concern because version 2.6.0 is very old. Even for old brokers, the behavior remains the same with batch index ACK disabled.

For issue 2, no performance issues have been reported with batch index ACK enabled. Generally, correctness should have higher priority than performance. It is more worthwhile to improve the performance of an API with correct semantics than to fear performance issues and provide a confusing API by default.

For issue 3, the number of RPCs can be reduced by leveraging the `acknowledge` API that accepts a list of message IDs:

```java
void acknowledge(List<MessageId> messageIdList) throws PulsarClientException;
```

Additionally, the `isAckReceiptEnabled` config is disabled by default, meaning even the synchronous `acknowledge` API does not require an ACK request to be sent for this message ID. The acknowledgments are cached and grouped into a single ACK request after some time, determined by the `acknowledgmentGroupTime` and `maxAcknowledgmentGroupSize` configs.

## Goals

Enable the batch index ACK feature by default for both client and broker.

## High-Level Design

For Pulsar 4.x, enable `acknowledgmentAtBatchIndexLevelEnabled` and `ConsumerBuilder#enableBatchIndexAcknowledgment` by default.

For Pulsar 5.x, deprecate the `ConsumerBuilder#enableBatchIndexAcknowledgment` config and enforce batch index ACK regardless of its setting.

## Backward & Forward Compatibility

- Batch index ACK does not work on Pulsar brokers earlier than version 2.6.0.
- For older versions that do not implement this PIP, `acknowledgmentAtBatchIndexLevelEnabled` must be configured to true manually.

## General Notes

## Links

* Mailing List discussion thread: https://lists.apache.org/thread/y19s4tn1xtbh8lxbgcf0pc8yy6xs2k25
* Mailing List voting thread: https://lists.apache.org/thread/6l2th8fwvx9gtljcdov6c1mz7d7oopt9
