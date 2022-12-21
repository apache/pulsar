---
id: message-throttling
title: Message throttling
sidebar_label: "Message throttling"
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
- `ratePeriodInSecond` The unit of the rate limiting period is second. The default is 1s.
- `dispatchThrottlingRateInMsg` Specifies the maximum number of messages to be delivered in each rate limiting period. 
   The default is' -1 ', which means no limit.
- `dispatchThrottlingRateInByte` The maximum number of bytes of messages delivered per rate-limiting period. The default
   is' -1 ', which means no limit.

> If set ` dispatchThrottlingRateInMsg` and ` dispatchThrottlingRateInByte` both, then the message delivery is required 
> at the same time satisfy the rules of the two, in other words the two rules are not mutually exclusive.

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

#### Estimate the amount of data to be read from the Storage
Messages is stored in blocks. If batch-send is not enabled, each message is packaged into an independent data block.And
if batch-send is enabled, a batch of messages are packaged into one data block, then we can not determine the true count
of messages in one data block; We also cannot determine the true size of this data block until we have actually read the
it from Storage.So in order to satisfy the constraints `dispatchThrottlingRateInMsg` and `dispatchThrottlingRateInByte`,
requires a mechanism to estimate how much need to read a data block.

> If enabled the feature ` dispatchThrottlingOnBatchMessageEnabled`, instead of counting by message, we're counting
> by blocks of data.

When delivering a message to the client, the Broker looks at the size and number of messages per data block, calculates
an average and caches it in memory, using this average to estimate how many data blocks to be read. What if there is no
estimated value in memory when the first reading of a new topic? The current workaround is that just delivers one data
block to clients if it is the first delivery of a new topic. Of course, the broker does not store all the information
of data blocks for a long time to process the average accurately, which would waste memory. Instead, a compromise
algorithm 'avg = (history-avg * 0.9 + new-avg * 0.1)' is used.

### Only three granularity supports
- Throttling limit per broker: Often applied to overload protection of broker or storage. If only to prevent the
  excessive read requests results in Storage can not work well, you can disabled the feature
  `dispatchThrottlingOnNonBacklogConsumerEnabled` (default is enabled). After disabled this feature, if all consumers
  in one subscription have no backlog(it is clear that almost all read requests can hit the cache), then message
  dispatch throttling is turned off automatically，and if any consumer has backlog, it will be turned on automatically.
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
  `10/s`, and the speed limit for `sub1` in `t2-partition-1` is `10/s` too. So the speed limit for the whole `sub2` is
  `20/s`. There are also three ways to set the rule of subscriptions(Effective priority, from low to high):
  - set by [configuration of broker](https://pulsar.apache.org/docs/reference-configuration/#broker);
  - set by [policies of namespace](https://pulsar.apache.org/docs/2.10.x/admin-api-namespaces/#configure-dispatch-throttling-for-subscription);
  - set by `pulsar-admin topicPolicies set-subscription-dispatch-rate`.
- Throttling limit per resource group([PIP-82 Not yet implement](https://github.com/apache/pulsar/wiki/PIP-82%3A-Tenant-and-namespace-level-rate-limiting)):
  This feature is to allow users to configure a namespace or tenant wide limit for consumers and have that enforced
  irrespective of the number of topics in the namespace, with fair sharing of the quotas.