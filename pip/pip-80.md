# PIP-80: Unified namespace-level admin API

- Status: [Discuss]
- Authors: Penghui Li，Lin Lin
- Release: 2.8.0
- Proposal time: 2021/02/01

## Motivation

Now topic-level policies are unified but the namespace-level policies are not and have the following problems:

1. Some default values are equal to 0, which makes us unable to judge whether there is no setting value or whether it is disabled. The existing default values are as follows:

- null
- 0
- -1

There are instructions in `broker.conf`, equal to 0 means disabled,but these policies can not be disabled now.

E.g:
The comment of `maxConsumersPerSubscription` in broker.conf is :

Using a value of 0, is disabling maxConsumersPerSubscription-limit check.The disabling of all other parameters is similar, all 0 means disabled.

But in `Policies.java`, the default value of maxConsumersPerSubscription is:
`public int max_consumers_per_subscription = 0;`

Now the disabling does not take effect, and we cannot recognize whether the namespace-level is not set or whether it needs to be disabled.
Why don't we use -1 or 0 to mean it is not set？

Now -1 is rare, most of them are 0 or null. Change 0 and null to -1, or change -1 and null to 0, there will be compatibility issues as well.
All topic-level use null, which can be consistent.

2. When no value is set, the return value of the query interface has many types：

- throws an Exception
- return the value of the previous level
- return the value of current level

## Approach

1. To solve problem 1. We will unify all interfaces so that each state has the same meaning. The get interface only returns the value of current level.
- null: not set
- 0: disabled
- `>` 0: has been set
- `<` 0: not limited (Some need infinity, such as : retention)

2. To solve problem 2. We will add `applied` interfaces at the topic level:
`Integer getMessageTTL(String topic, boolean applied)`
When applied = true, if the topic-level has a policy, it will return, otherwise it will return the broker-level, if not, it will return the 

broker-level

When applied = false, it only return the value of current level

## Changes
### API Changes
1. The basic type will be changed to the packaging type, such as:
`int` becomes `Integer`, can return null. Now topic-level policies are all in this way, but there are several configuration items of namespace-level still use basic types, and we will make namespace-level also be like topic-level.
2. The remove method will be added to the namespace-level API, allowing us to remove the namespace-level policies.
3. Applied API will be added to the topic-level.
Protocol Changes
none
Configuration Changes
none

## Compatibility
This will cause some existing interfaces to not be fully compatible.
When the lower version of the SDK queries the new version of the broker with no value, NPE will appear.
Since the previous disabled did not take effect, the value stored in zk is 0. If the broker is upgraded, the disabled will take effect. This is not directly related to modifying the API, as long as the bug is fixed, this problem will occur.

Interfaces that are incompatible and `disabled` will take effect：

- max_producers_per_topic
- max_consumers_per_topic
- max_consumers_per_subscription
- max_unacked_messages_per_consumer
- max_unacked_messages_per_subscription
- compaction_threshold
- offload_threshold

## Test Plan
1. Test whether the API at each level is working properly
2. When policies of different levels exist at the same time, test whether the priority is correct
