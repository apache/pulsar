---
id: administration-load-balance
title: Load balance across brokers
sidebar_label: "Load balance"
original_id: administration-load-balance
---


Pulsar is a horizontally scalable messaging system, so the traffic in a logical cluster must be balanced across all the available Pulsar brokers as evenly as possible, which is a core requirement.

You can use multiple settings and tools to control the traffic distribution which requires a bit of context to understand how the traffic is managed in Pulsar. Though in most cases, the core requirement mentioned above is true out of the box and you should not worry about it. 

The following sections introduce how the load-balanced assignments work across Pulsar brokers and how you can leverage the framework to adjust.

## Dynamic assignments

Topics are dynamically assigned to brokers based on the load conditions of all brokers in the cluster. The assignment of topics to brokers is not done at the topic level but at the **bundle** level (a higher level). Instead of individual topic assignments, each broker takes ownership of a subset of the topics for a namespace. This subset is called a bundle and effectively this subset is a sharding mechanism. 

In other words, each namespace is an "administrative" unit and sharded into a list of bundles, with each bundle comprising a portion of the overall hash range of the namespace. Topics are assigned to a particular bundle by taking the hash of the topic name and checking in which bundle the hash falls. Each bundle is independent of the others and thus is independently assigned to different brokers.

The benefit of the assignment granularity is to amortize the amount of information that you need to keep track of. Based on CPU, memory, traffic load, and other indexes, topics are assigned to a particular broker dynamically. For example: 
* When a client starts using new topics that are not assigned to any broker, a process is triggered to choose the best-suited broker to acquire ownership of these topics according to the load conditions. 
* If the broker owning a topic becomes overloaded, the topic is reassigned to a less-loaded broker.
* If the broker owning a topic crashes, the topic is reassigned to another active broker.

:::tip

For partitioned topics, different partitions are assigned to different brokers. Here "topic" means either a non-partitioned topic or one partition of a topic.

:::

## Create namespaces with assigned bundles

When you create a new namespace, a number of bundles are assigned to the namespace. You can set this number in the `conf/broker.conf` file:

```conf

# When a namespace is created without specifying the number of bundles, this
# value will be used as the default
defaultNumberOfNamespaceBundles=4

```

Alternatively, you can override the value when you create a new namespace using [Pulsar admin](/tools/pulsar-admin/):

```shell

bin/pulsar-admin namespaces create my-tenant/my-namespace --clusters us-west --bundles 16

```

With the above command, you create a namespace with 16 initial bundles. Therefore the topics for this namespace can immediately be spread across up to 16 brokers.

In general, if you know the expected traffic and number of topics in advance, you had better start with a reasonable number of bundles instead of waiting for the system to auto-correct the distribution.

On the same note, it is beneficial to start with more bundles than the number of brokers, due to the hashing nature of the distribution of topics into bundles. For example, for a namespace with 1000 topics, using something like 64 bundles achieves a good distribution of traffic across 16 brokers.


## Split namespace bundles

Since the load for the topics in a bundle might change over time and predicting the load might be hard, bundle split is designed to resolve these challenges. The broker splits a bundle into two and the new smaller bundles can be reassigned to different brokers.

Pulsar supports the following two bundle split algorithms:
* `range_equally_divide`: split the bundle into two parts with the same hash range size.
* `topic_count_equally_divide`: split the bundle into two parts with the same number of topics.

To enable bundle split, you need to configure the following settings in the `broker.conf` file, and set `defaultNamespaceBundleSplitAlgorithm` based on your needs.

```conf

loadBalancerAutoBundleSplitEnabled=true
loadBalancerAutoUnloadSplitBundlesEnabled=true
defaultNamespaceBundleSplitAlgorithm=range_equally_divide

```

You can configure more parameters for splitting thresholds. Any existing bundle that exceeds any of the thresholds is a candidate to be split. By default, the newly split bundles are immediately reassigned to other brokers, to facilitate the traffic distribution. 

```conf

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

## Shed load automatically

The support for automatic load shedding is available in the load manager of Pulsar. This means that whenever the system recognizes a particular broker is overloaded, the system forces some traffic to be reassigned to less-loaded brokers.

When a broker is identified as overloaded, the broker forces to "unload" a subset of the bundles, the ones with higher traffic, that make up for the overload percentage.

For example, the default threshold is 85% and if a broker is over quota at 95% CPU usage, then the broker unloads the percent difference plus a 5% margin: `(95% - 85%) + 5% = 15%`. Given the selection of bundles to unload is based on traffic (as a proxy measure for CPU, network, and memory), the broker unloads bundles for at least 15% of traffic.

:::tip

* The automatic load shedding is enabled by default. To disable it, you can set `loadBalancerSheddingEnabled` to `false`.
* Besides the automatic load shedding, you can [manually unload bundles](#unload-topics-and-bundles).

:::

Additional settings that apply to shedding:

```conf

# Load shedding interval. Broker periodically checks whether some traffic should be offload from
# some over-loaded broker to other under-loaded brokers
loadBalancerSheddingIntervalMinutes=1

# Prevent the same topics to be shed and moved to other brokers more than once within this timeframe
loadBalancerSheddingGracePeriodMinutes=30

```

Pulsar supports the following types of automatic load shedding strategies. 
* [ThresholdShedder](#thresholdshedder)
* [OverloadShedder](#overloadshedder)
* [UniformLoadShedder](#uniformloadshedder)

:::note

* From Pulsar 2.10, the **default** shedding strategy is `ThresholdShedder`.
* You need to restart brokers if the shedding strategy is [dynamically updated](admin-api-brokers.md/#dynamic-broker-configuration). 

:::

### ThresholdShedder
This strategy tends to shed the bundles if any broker's usage is above the configured threshold. It does this by first computing the average resource usage per broker for the whole cluster. The resource usage for each broker is calculated using the following method `LocalBrokerData#getMaxResourceUsageWithWeight`. Historical observations are included in the running average based on the broker's setting for `loadBalancerHistoryResourcePercentage`. Once the average resource usage is calculated, a broker's current/historical usage is compared to the average broker usage. If a broker's usage is greater than the average usage per broker plus the `loadBalancerBrokerThresholdShedderPercentage`, this load shedder proposes removing enough bundles to bring the unloaded broker 5% below the current average broker usage. Note that recently unloaded bundles are not unloaded again. 

![Shedding strategy - ThresholdShedder](/assets/shedding-strategy-thresholdshedder.svg)

For example, assume you have three brokers, the average broker usage of broker1 is 40%, the average broker usage of broker2 and broker3 is 10%, then the cluster average usage is 20% ((40% + 10% + 10%) / 3). If you set `loadBalancerBrokerThresholdShedderPercentage` to `10`, then only broker1's certain bundles get unloaded, because the average usage of broker1 is greater than the sum of the cluster average usage (20%) plus `loadBalancerBrokerThresholdShedderPercentage`(10%).

To use the `ThresholdShedder` strategy, configure brokers with this value.
`loadBalancerLoadSheddingStrategy=org.apache.pulsar.broker.loadbalance.impl.ThresholdShedder`

You can configure the weights for each resource per broker in the `conf/broker.conf` file. 

```conf

# The BandWithIn usage weight when calculating new resource usage. The range is between 0 and 1.0.
loadBalancerBandwithInResourceWeight=1.0

# The BandWithOut usage weight when calculating new resource usage. The range is between 0 and 1.0.
loadBalancerBandwithOutResourceWeight=1.0

# The CPU usage weight when calculating new resource usage. The range is between 0 and 1.0.
loadBalancerCPUResourceWeight=1.0

# The heap memory usage weight when calculating new resource usage. The range is between 0 and 1.0.
loadBalancerMemoryResourceWeight=1.0

# The direct memory usage weight when calculating new resource usage. The range is between 0 and 1.0.
loadBalancerDirectMemoryResourceWeight=1.0

```

### OverloadShedder
This strategy attempts to shed exactly one bundle on brokers which are overloaded, that is, whose maximum system resource usage exceeds [`loadBalancerBrokerOverloadedThresholdPercentage`](#broker-overload-thresholds). To see which resources are considered when determining the maximum system resource. A bundle is recommended for unloading off that broker if and only if the following conditions hold: The broker has at least two bundles assigned and the broker has at least one bundle that has not been unloaded recently according to `LoadBalancerSheddingGracePeriodMinutes`. The unloaded bundle will be the most expensive bundle in terms of message rate that has not been recently unloaded. Note that this strategy does not take into account "underloaded" brokers when determining which bundles to unload. If you are looking for a strategy that spreads load evenly across all brokers, see [ThresholdShedder](#thresholdshedder). 

![Shedding strategy - OverloadShedder](/assets/shedding-strategy-overloadshedder.svg)

To use the `OverloadShedder` strategy, configure brokers with this value.
`loadBalancerLoadSheddingStrategy=org.apache.pulsar.broker.loadbalance.impl.OverloadShedder`

#### Broker overload thresholds

The determination of when a broker is overloaded is based on the threshold of CPU, network, and memory usage. Whenever either of those metrics reaches the threshold, the system triggers the shedding (if enabled).

:::note

The overload threshold `loadBalancerBrokerOverloadedThresholdPercentage` only applies to the [`OverloadShedder`](#overloadshedder) shedding strategy. By default, it is set to 85%.

:::

Pulsar gathers the CPU, network, and memory usage stats from the system metrics. In some cases of network utilization, the network interface speed that Linux reports is not correct and needs to be manually overridden. This is the case in AWS EC2 instances with 1Gbps NIC speed for which the OS reports 10Gbps speed.

Because of the incorrect max speed, the load manager might think the broker has not reached the NIC capacity, while in fact the broker already uses all the bandwidth and the traffic is slowed down.

You can set `loadBalancerOverrideBrokerNicSpeedGbps` in the `conf/broker.conf` file to correct the max NIC speed. When the value is empty, Pulsar uses the value that the OS reports.

### UniformLoadShedder
This strategy tends to distribute load uniformly across all brokers. This strategy checks the load difference between the broker with the highest load and the broker with the lowest load. If the difference is higher than configured thresholds `loadBalancerMsgRateDifferenceShedderThreshold` and `loadBalancerMsgThroughputMultiplierDifferenceShedderThreshold` then it finds out bundles that can be unloaded to distribute traffic evenly across all brokers. 

![Shedding strategy - UniformLoadShedder](/assets/shedding-strategy-uniformLoadshedder.svg)

To use the `UniformLoadShedder` strategy, configure brokers with this value.
`loadBalancerLoadSheddingStrategy=org.apache.pulsar.broker.loadbalance.impl.UniformLoadShedder`

## Unload topics and bundles

You can "unload" a topic in Pulsar manual admin operations. Unloading means closing topics, releasing ownership, and reassigning topics to a new broker, based on the current load.

When unloading happens, the client experiences a small latency blip, typically in the order of tens of milliseconds, while the topic is reassigned.

Unloading is the mechanism that the load manager uses to perform the load shedding, but you can also trigger the unloading manually, for example, to correct the assignments and redistribute traffic even before having any broker overloaded.

Unloading a topic has no effect on the assignment, but just closes and reopens the particular topic:

```shell

pulsar-admin topics unload persistent://tenant/namespace/topic

```

To unload all topics for a namespace and trigger reassignments:

```shell

pulsar-admin namespaces unload tenant/namespace

```

## Distribute anti-affinity namespaces across failure domains

When your application has multiple namespaces and you want one of them available all the time to avoid any downtime, you can group these namespaces and distribute them across different [failure domains](reference-terminology.md#failure-domain) and different brokers. Thus, if one of the failure domains is down (due to release rollout or brokers restart), it only disrupts namespaces owned by that specific failure domain and the rest of the namespaces owned by other domains remain available without any impact.

Such a group of namespaces has anti-affinity to each other, that is, all the namespaces in this group are [anti-affinity namespaces](reference-terminology.md#anti-affinity-namespaces) and are distributed to different failure domains in a load-balanced manner. 

As illustrated in the following figure, Pulsar has 2 failure domains (Domain1 and Domain2) and each domain has 2 brokers in it. You can create an anti-affinity namespace group that has 4 namespaces in it, and all the 4 namespaces have anti-affinity to each other. The load manager tries to distribute namespaces evenly across all the brokers in the same domain. Since each domain has 2 brokers, every broker owns one namespace from this anti-affinity namespace group, and you can see each domain owns 2 namespaces, and each broker owns 1 namespace.

![Distribute anti-affinity namespaces across failure domains](/assets/anti-affinity-namespaces-across-failure-domains.svg)

The load manager follows an even distribution policy across failure domains to assign anti-affinity namespaces. The following table outlines the even-distributed assignment sequence illustrated in the above figure.

| Assignment sequence | Namespace | Failure domain candidates | Broker candidates | Selected broker |
|:---|:------------|:------------------|:------------------------------------|:-----------------|
| 1 | Namespace1 | Domain1, Domain2 | Broker1, Broker2, Broker3, Broker4 | Domain1:Broker1 |
| 2 | Namespace2 | Domain2          | Broker3, Broker4                   | Domain2:Broker3 |
| 3 | Namespace3 | Domain1, Domain2 | Broker2, Broker4                   | Domain1:Broker2 |
| 4 | Namespace4 | Domain2          | Broker4                            | Domain2:Broker4 |
 
:::tip

* Each namespace belongs to only one anti-affinity group. If a namespace with an existing anti-affinity assignment is assigned to another anti-affinity group, the original assignment is dropped.

* If there are more anti-affinity namespaces than failure domains, the load manager distributes namespaces evenly across all the domains, and also every domain distributes namespaces evenly across all the brokers under that domain.

:::

### Create a failure domain and register brokers
 
:::note

One broker can only be registered to a single failure domain.

:::
 
To create a domain under a specific cluster and register brokers, run the following command:

```bash

pulsar-admin clusters create-failure-domain <cluster-name> --domain-name <domain-name> --broker-list <broker-list-comma-separated>

```

You can also view, update, and delete domains under a specific cluster. For more information, refer to [Pulsar admin doc](/tools/pulsar-admin/).

### Create an anti-affinity namespace group

An anti-affinity group is created automatically when the first namespace is assigned to the group. To assign a namespace to an anti-affinity group, run the following command. It sets an anti-affinity group name for a namespace.
 
```bash

pulsar-admin namespaces set-anti-affinity-group <namespace> --group <group-name>
 
```

For more information about `anti-affinity-group` related commands, refer to [Pulsar admin doc](/tools/pulsar-admin/).
