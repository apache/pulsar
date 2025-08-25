# PIP-22: Pulsar Dead Letter Topic

- **Status**: Done
- **Author**: [Penghui Li](https://github.com/codelipenghui)
- **Pull Request**: [#2508](https://github.com/apache/incubator-pulsar/pull/2508)
- **Mailing List discussion**: 
- **Release**: 2.2.0

## Motivation

When consumer got messages from pulsar, It's difficult to ensure every message can be consume success. Pulsar support message redelivery feature by set acknowledge timeout when create a new consumer. This is a good feature guarantee consumer will not lost messages. 

But however, some message will redelivery so many times possible, even to the extent that it can be never stop.

So, It's necessary to support a feature to control it by pulsar. Users can use this feature and customize this feature to control the message redelivery behavior. The feature named Dead Letter Topic.

## Requirements

The Pulsar Dead Letter Topic should have the following capabilities:
- Consumer can set maximum number of redeliveries.
- Consumer can set the name of Dead Letter Topic, It’s not necessary.
- Message exceeding the maximum number of redeliveries should send to Dead Letter Topic and acknowledged automatic.

## Design

This design tries to do most of the work on broker side, This could make the client and broker side implementation easier, reduce the proto command executing between client and broker, and avoid the catching up work for multiple language clients. 
 
The main work is in `Subscription` or `Dispatcher`(e.g. `PersistentSubscription` or `PersistentDispatcherMultipleConsumers`), in which place we could define  a Redelivery Tracker to track every un-acked messages send to from this subscription. Redelivery Tracker record the number of deliver times in memory, it will increment at each time of message redelivery.

The subscription contains user defined maximum times of redeliveries and the the name of Dead Letter Topic for enable this feature. Usually the name of Dead Letter Topic is not necessary.If the the name of Dead Letter Topic is absent, pulsar broker need generate with topic name and subscription name and suffix with -DLQ.

If the delivery times, which was tracked in Redelivery Tracker, reached user defined threshold, we re-deliver the message to Dead Letter Topic, then mark the message acked in original subscription. If not, we keep the original redelivery logic.

 Keeping the number of deliver times in memory is enough for most of the use case, and make is simple. If we want to make it persistent in the future, we could use the existing cursor mechanism.

## Precautions
One subscription have more than one consumer, so the configuration of consumer is may not same. Pulsar broker will use the last created consumer in a subscription.

## Alternatives

PS: During the design discussing, we have following alternative suggestions from community for this feature:

### Solution 1

Use existing cursor mechanism. Currently cursor is tracking what messages are acked and what messages are not acked. We can extend the cursor to have `redelivery-count` for the message. So when broker detects an ack-timeout on a message, it first update the cursor to increase the `redelivery-count` for that message, once that is done, it will then redeliver the message. Since the cursor is backed by a bookkeeper ledger, so we are able to leverage the write throughput of bookkeeper for recording counters for redeliveries.
When a message exceed the `maxDeliverCount` defined in `DeliveryPolicy`, it will then write the message to `deadLetterTargetTopic` and then mark the message as acked in the original subscription.
In this solution, the message id will be retained during redeliveries. When it is moved from original topic to `deadLetterTopic`, we can put the original message id as a property in the new message published to `deadLetterTopic`.

### Solution 2

Use an intermediate topic as a `retry-topic` for each subscription. When broker detects an ack timeout and wants to redeliver the message, it first publish the message to this `retry-topic` and then mark the message as acked in the original subscription.
When broker dispatches messages, it will pull messages from both original topic and `retry-topic` for that subscription.
When a message is moved from original topic to retry topic, we will put the original message id in the property of new message. So when broker dispatch a message from retry topic, it can use the original message id as the message id. This ensures the message id retained during redelivers.
The message will contain a redelivery count in the property when redelivering. This can be used for tracking redeliveries. After a message has been redelivered more than `maxDeliverCount` defined in the delivery policy, the message will be moved to the deadLetterTargetTopic.

### Solution 3

I’d prefer to have this as a client side implementation (as much as possible) to avoid more complexity in broker.
The only support we would need from broker is the “delivery-count” and even that could also be kept just in memory.
With the delivery counter, in client library we can have internal implementation that re-publishes messages on the DLQ.
Client could configure that in consumer builder:
```
client.newConsumer()
    .topic(“my-topic”)
     .subscriptionName(“test”)
     .deadLetterQueue(15, “my-dlq-topic”)
     .subscribe();
```
Internally, when it receives a message with counter >= 15, the library will re-publish on dlq-topic and ack on source topic.
