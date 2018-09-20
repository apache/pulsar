---
id: version-2.1.0-incubating-administration-load-distribution
title: Pulsar load distribution
sidebar_label: Load distribution
original_id: administration-load-distribution
---

## Load distribution across Pulsar brokers

Pulsar is an horizontally scalable messaging system, so it is a core requirement that the traffic
in a logical cluster must be spread across all the available Pulsar brokers, as evenly as possible.

In most cases, this is true out of the box and one shouldn't worry about it. There are, though,
multiple settings and tools to control the traffic distribution and they require a bit of
context to understand how the traffic is managed in Pulsar.

## Pulsar load manager architecture

### Dynamic assignment of topics to brokers

Topics are dynamically assigned to brokers based on the load conditions of all brokers in the
cluster.

When a clients starts using new topics that are not assigned to any broker, it will trigger a
process that, given the load conditions, it will choose the best suited broker to acquire ownership
of such topic.

In case of partitioned topics, different partitions might be assigned to different brokers. We talk
about "topic" in this context to mean either a non-partitioned topic or one partition of a topic.

The assignment is "dynamic" because it can change very quickly. For example, if the broker owning
the topic crashes, the topic will be reassigned immediately to another broker. Another scenario is
that the broker owning the topic becomes overloaded. In this case too, the topic will be
reassigned to a less loaded broker.

The dynamic assignment is made possible by the stateless nature of brokers. This also ensure that
we can quickly expand or shrink the cluster based on usage.

### Assignment granularity

The assignment of topics/partitions to brokers is not done at the individual level. The reason for
it is to amortize the amount of information that we need to keep track (eg. which topics are
assigned to a particular broker, what's the load on topics for a broker and similar).

Instead of individual topic/partition assignment, each broker takes ownership of a subset of the
topics for a namespace. This subset is called a "*bundle*" and effectively it's a sharding
mechanism.

The namespace is the "administrative" unit: many config knobs or operations are done at the
namespace level.

For assignment, a namespaces is sharded into a list of "bundles", with each bundle comprising
a portion of overall hash range of the namespace.

Topics are assigned to a particular bundle by taking the hash of the topic name and seeing in which
bundle the hash falls into.

Each bundle is independent of the others and thus is independently assigned to different brokers.

### Creating namespaces and bundles

When creating a new namespace, it will set to use the default number of bundles. This is set in
`conf/broker.conf`:

```properties
# When a namespace is created without specifying the number of bundle, this
# value will be used as the default
defaultNumberOfNamespaceBundles=4
```

One can either change the system default, or override it when creating a new namespace:

```shell
$ bin/pulsar-admin namespaces create my-tenant/my-namespace --clusters us-west --bundles 16
```

With this command, we're creating a namespace with 16 initial bundles. Therefore the topics for
this namespaces can immediately be spread across up to 16 brokers.

In general, if the expected traffic and number of topics is known in advance, it's a good idea to
start with a reasonable number of bundles instead of waiting for the system to auto-correct the
distribution.

On a same note, it is normally beneficial to start with more bundles than number of brokers,
primarily because of the hashing nature of the distribution of topics into bundles. For example,
for a namespace with 1000 topics, using something like 64 bundles will achieve a good distribution
of traffic across 16 brokers.

### Unloading topics and bundles

In Pulsar there is an admin operation of "unloading" a topic. Unloading means to close the topics,
release ownership and reassign the topics to a new broker, based on current load.

When unload happens, the client will experience a small latency blip, typically in the order of
tens of milliseconds, while the topic is reassigned.

Unloading is the mechanism used by the load-manager to perform the load shedding, but it can
also be triggered manually, for example to correct the assignments and redistribute traffic
even before having any broker overloaded.

Unloading a topic has no effect on the assignment, but it will just close and reopen the
particular topic:

```shell
pulsar-admin topics unload persistent://tenant/namespace/topic
```

To unload all topics for a namespace and trigger reassignments:

```shell
pulsar-admin namespaces unload tenant/namespace
```

### Namespace bundles splitting

Since the load for the topics in a bundle might change over time, or could just be hard to predict
upfront, bundles can be split in 2 by brokers. The new smaller bundles can then be reassigned
to different brokers.

The splitting happens based on some tunable thresholds. Any existing bundle that exceeds any
of the threshold is a candidate to be split. By default the newly split bundles are also
immediately offloaded to other brokers, to facilitate the traffic distribution.

```properties
# enable/disable namespace bundle auto split
loadBalancerAutoBundleSplitEnabled=true

# enable/disable automatic unloading of split bundles
loadBalancerAutoUnloadSplitBundlesEnabled=true

# maximum topics in a bundle, otherwise bundle split will be triggered
loadBalancerNamespaceBundleMaxTopics=1000

# maximum sessions (producers + consumers) in a bundle, otherwise bundle split will be triggered
loadBalancerNamespaceBundleMaxSessions=1000

# maximum msgRate (in + out) in a bundle, otherwise bundle split will be triggered
loadBalancerNamespaceBundleMaxMsgRate=30000

# maximum bandwidth (in + out) in a bundle, otherwise bundle split will be triggered
loadBalancerNamespaceBundleMaxBandwidthMbytes=100

# maximum number of bundles in a namespace (for auto-split)
loadBalancerNamespaceMaximumBundles=128
```


### Automatic load shedding

In Pulsar's load manager there is support for automatic load shedding. This means that whenever
the system recognized a particular broker is overloaded, it will force some traffic to be
reassigned to less loaded brokers.

When a broker is identifies as overloaded, it will force to "unload" a subset of the bundles, the
ones with higher traffic, that make up for the overload percentage.

For example, the default threshold is 85% and if a broker is over quota at 95% CPU usage, then
it will unload the percent difference plus a 5% margin: `(95% - 85%) + 5% = 15%`.

Given the selection of bundles to offload is based on traffic (as a proxy measure for cpu, network
and memory), broker will unload bundles for at least 15% of traffic.

The automatic load shedding is enabled by default and can be disabled with this setting:

```properties
# Enable/disable automatic bundle unloading for load-shedding
loadBalancerSheddingEnabled=true
```

There are additional settings that apply to shedding:

```properties
# Load shedding interval. Broker periodically checks whether some traffic should be offload from
# some over-loaded broker to other under-loaded brokers
loadBalancerSheddingIntervalMinutes=1

# Prevent the same topics to be shed and moved to other brokers more that once within this timeframe
loadBalancerSheddingGracePeriodMinutes=30
```

#### Broker overload thresholds

The determinations of when a broker is overloaded is based on threshold of CPU, network and
memory usage. Whenever either of those metrics reaches the threshold, it will trigger the shedding
(if enabled).

By default, overload threshold is set at 85%:

```properties
# Usage threshold to determine a broker as over-loaded
loadBalancerBrokerOverloadedThresholdPercentage=85
```

The usage stats are gathered by Pulsar from the system metrics.

In case of network utilization, in some cases the network interface speed reported by Linux is
not correct and needs to be manually overridden. This is the case in AWS EC2 instances with 1Gbps
NIC speed for which the OS report 10Gbps speed.

Because of the incorrect max speed, the Pulsar load manager might think the broker has not
reached the NIC capacity, while in fact it's already using all the bandwidth and the traffic is
being slowed down.

There is a setting to correct the max NIC speed:

```properties
# Override the auto-detection of the network interfaces max speed.
# This option is useful in some environments (eg: EC2 VMs) where the max speed
# reported by Linux is not reflecting the real bandwidth available to the broker.
# Since the network usage is employed by the load manager to decide when a broker
# is overloaded, it is important to make sure the info is correct or override it
# with the right value here. The configured value can be a double (eg: 0.8) and that
# can be used to trigger load-shedding even before hitting on NIC limits.
loadBalancerOverrideBrokerNicSpeedGbps=
```

When the value is empty, Pulsar will use the value reported by the OS.

