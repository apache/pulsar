---
id: message-dispatch-throttling
title: Message dispatch throttling
sidebar_label: "Message dispatch throttling"
---

## Message Dispatch throttling
Message dispatch throttling is a mechanism that limits the speed at which messages are delivered to the client.

### When should I use message dispatch throttling?
* Messages are persisted on disk, `Storage` is the component for this part of the work. If a large number of read
  requests fail to match the cache, the Storage becomes too busy and cannot work properly. Use message dispatch
  throttling makes Pulsar work steadily by keeping Storage's read request load stable.
* An instance of 'Broker' serves multiple topics at the same time, and if a topic is too busy, it will occupy
  almost all of the IO resources, other topics will not work well. Use message dispatch throttling can balance
  the allocation of resources to agents across topics.
* There have large backlog of messages to consume, clients may receive a large amount of data in a short period of time,
  thus monopolizing the client computer resources. Message dispatch throttling can also be used to balance the resource
  allocation of the client computer.

### Concepts of message dispatch throttling
- `ratePeriodInSecond` Usually the rate limiter is defined as how many times per second, how many times per minute, and
   so on. In each of these definitions, there is the concept of a time period, such as one second, one minute, and the
   counter is reset at the end of the time period. In Pulsar, the user can customize this time period, it is
   `ratePeriodInSecond`, default is 1s. For example: if we want limit dispatch to 10,000 numbers of messages per minute,
   we should set `ratePeriodInSecond` to 60 and set `dispatchThrottlingRateInMsg` to 10,000.
- `dispatchThrottlingRateInMsg` Specifies the maximum number of messages to be delivered in each rate limiting period. 
   The default is' -1 ', which means no limit.
- `dispatchThrottlingRateInByte` The maximum number of bytes of messages delivered per rate-limiting period. The default
   is' -1 ', which means no limit.

> `dispatchThrottlingRateInMsg` and `dispatchThrottlingRateInByte` are AND relations.

### How it works
Message dispatch throttling works divided into these steps:
1. Calculate the number of messages or bytes to be delivered.
2. Estimate the amount of data to be read from the Storage (This estimate logic is not accurate, as described below).
3. Update the counter of message dispatch throttling.
4. Actually deliver the message to the client.

> If the quota in the current rate limiting period is not used up, the quota will not be used in the next rate limiting
> period. However, if the quota in the current rate limiting period is exceeded, the quota in the next rate limiting
> period is reduced. For example, if the rate-limiting rule is set to `10 /s`, `11` messages are delivered to the client
> in the first rate-limiting period,`9` messages will be delivered to the client in the next rate-limiting period.

#### Why are messages over-delivered?
- Cause-1: Messages is stored in blocks. If batch-send is not enabled, each message is packaged into an independent data
block. And if batch-send is enabled, a batch of messages are packaged into one data block, then we can not determine the
true count of messages in one data block. So Pulsar counts batch as one message to calculates how many batch need to
read, after read data-block from storage, we can exactly known how many messages in these data-blocks, so the counter
of dispatch throttling can be corrected calculate. But it has also led to over-delivered(enable the feature
`preciseDispatcherFlowControl` and `dispatchThrottlingOnBatchMessageEnabled` will improve this situation,see [Features](#features)).
- Cause-2: The logic of the dispatch throttling is: `1.get remaining amount` -> `2.load data` -> `3.deduct amount`, If
there are two process "dispatch replay messages(we call it `a`)" and "dispatch non-replay messages(we call it `b`)" in
the same subscription are executed in parallel, it is possible to execute in this order:
`process-a: 1.get remaining amount` -> `process-a: 2.load data` -> `process-b: 1.get remaining amount` ->
`process-b: 2.load data` -> `process-a: 3.deduct amount` -> `process-b: 3.deduct amount`, both `process-a` and
`process-b` dispatch enough messages, and the total number exceeds the threshold.

#### Estimate the bytes size of data to be read from the Storage
We cannot determine the true size of this data block until we have actually read the it from Storage.So in order to
satisfy the constraints `dispatchThrottlingRateInMsg` and `dispatchThrottlingRateInByte`, requires a mechanism to
estimate how much need to read a data block.

When Pulsar receives a request to send messages, Pulsar maintains the total count and total bytes size of data blocks,
then Pulsar known the avg bytes size per data block: `total bytes size / total count of blocks`. What if the broker has
just started and has not received any new send message request? Pulsar also records the total count and the total bytes
size of data previously delivered to clients, so can calculate the average of the messages previously delivered. What if
the broker has just started and has not redelivered any messages? The current workaround is that just delivers one data
block to clients if it is the first delivery of a new topic.

### Features
| feature-name                                  | description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | example case                                                                                                                                                                                                                                                                                                                                                | default value |
|-----------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| preciseDispatcherFlowControl                  | When delivering a message to the client, the Broker looks at the size and number of messages per data block, calculates an average and caches it in memory, using this average to estimate how many data blocks to be read. What if there is no estimated value in memory when the first reading of a new topic? The current workaround is that just delivers one data block to clients if it is the first delivery of a new topic. Since yesterday's batch size and today's batch size may be different, so we will not take a strict average, instead the newer data will have more weight, the algorithm is 'avg = (history-avg * 0.9 + new-avg * 0.1)' is used.                  | We set rate limiter to 10/s, if the average number of messages per batch currently recorded is 6, then we should read 2 data blocks, and if there have 7 and 8 messages each blocks, we will eventually exceed the limit. But if we disabled feature `preciseDispatcherFlowControl`, 10 data blocks will be read, which will make the excess problem worse. | false         |
| dispatchThrottlingOnBatchMessageEnabled       | Support rate-limiting dispatching on the batch messages rather than individual messages within batch messages. Since one batch is one data block, this is sufficient to constrain the concurrency of read requests on storage. This makes the count of the number of messages inaccurate,but also maximizes pulsar's throughput while keeping storage read requests stable.                                                                                                                                                                                                                                                                                                          | We set rate limiter to 10/s, after we enabled `dispatchThrottlingOnBatchMessageEnabled`, Pulsar do not care about how many messages per batch, 10 data blocks will be read and delivered, and the counter of rate limiter mark delivered 10 messages.                                                                                                       | false         |
| dispatchThrottlingOnNonBacklogConsumerEnabled | After we turn off `dispatchThrottlingOnNonBacklogConsumerEnabled`(default is enabled), if all consumers in one subscription have no backlog(it is clear that almost all read requests can hit the cache, so Pulsar will not send read request to the Storage any more), then message dispatch throttling is turned off automatically(it means that even if we set `dispatchThrottlingRateInMsg` and `dispatchThrottlingRateInByte`, throttling won't work because there is no backlog)，and if any consumer has backlog,it will be turned on automatically. If we need only to prevent the excessive read requests results in Storage can not work well, this feature is very useful. |                                                                                                                                                                                                                                                                                                                                                             | true          |

> `preciseDispatcherFlowControl` and `dispatchThrottlingOnBatchMessageEnabled` are mutually exclusive. We should ensure
> that a maximum of one is enabled.

### Only three granularity supports
- Throttling limit per broker.
- Throttling limit per topic: The maximum number of messages that can be delivered to clients per unit of time for the
  same topic. If there are multiple partitions, this rule indicates the maximum number of messages each partition can
  deliver per unit time. Total limiting of multiple partitions on the same topic is not supported. For example, Topic
  `t2` has two partitions, and we set the limit of each topic to `10/s`, then the rate limit of `t2-partition-0` is
  `10/s`, and the rate limit of `t2-partition-1` is also `10/s`. The speed limit for the whole `t2` is `20/s`. There
  are three ways to set the rule of topics(Effective priority, from low to high):
  - set by [configuration of broker](https://pulsar.apache.org/docs/reference-configuration/#broker); 
  - set by [policies of namespace](https://pulsar.apache.org/docs/2.10.x/admin-api-namespaces/#configure-dispatch-throttling-for-topics);
  - set by `pulsar-admin topicPolicies set-dispatch-rate`.
- Throttling limit per subscription: The maximum number of messages that same subscription can deliver to clients per
  unit time. If the subscribed topic has multiple partitions, this rule indicates the maximum number of messages the
  subscription can deliver per partition per unit time. For example, topic `t2` has two partitions, we create a
  subscription `sub1` to `t2`, and set rate limit to `10/s`. Then the speed limit for `sub1` in `t2-partition-0` is
  `10/s`, and the speed limit for `sub1` in `t2-partition-1` is `10/s` too. So the speed limit for the whole `sub1` is
  `20/s`. There are also three ways to set the rule of subscriptions(Effective priority, from low to high):
  - set by [configuration of broker](https://pulsar.apache.org/docs/reference-configuration/#broker);
  - set by [policies of namespace](https://pulsar.apache.org/docs/2.10.x/admin-api-namespaces/#configure-dispatch-throttling-for-subscription);
  - set by `pulsar-admin topicPolicies set-subscription-dispatch-rate`.
- Throttling limit per resource group([PIP-82 Not yet implement](https://github.com/apache/pulsar/wiki/PIP-82%3A-Tenant-and-namespace-level-rate-limiting)):
  This feature is to allow users to configure a namespace or tenant wide limit for consumers and have that enforced
  irrespective of the number of topics in the namespace, with fair sharing of the quotas.