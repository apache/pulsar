---
id: administration-load-balance
title: Pulsar load balance
sidebar_label: "Load balance across Pulsar brokers"
---


Pulsar is a horizontally scalable messaging system, so the traffic in a logical cluster must be balanced across all the available Pulsar brokers as evenly as possible, which is a core requirement.

You can use multiple settings and tools to control the traffic distribution which requires a bit of context to understand how the traffic is managed in Pulsar. Though, in most cases, the core requirement mentioned above is true out of the box and you should not worry about it. 

The following sections introduce how the load manager works across Pulsar brokers and how you can leverage it to adjust.

## Assign topics to brokers dynamically

Topics are dynamically assigned to brokers based on the load conditions of all brokers in the cluster.

When a client starts using new topics that are not assigned to any broker, a process is triggered to choose the best-suited broker to acquire ownership of these topics according to the load conditions. 

In case of partitioned topics, different partitions are assigned to different brokers. Here "topic" means either a non-partitioned topic or one partition of a topic.

The assignment is "dynamic" because it changes quickly. For example, if the broker owning the topic crashes, the topic is reassigned immediately to another broker. Another scenario is that the broker owning the topic becomes overloaded. In this case, the topic is reassigned to a less-loaded broker.

The stateless nature of brokers makes the dynamic assignment possible, so you can quickly expand or shrink the cluster based on usage.

### Assignment granularity

The assignment of topics or partitions to brokers is not done at the topics or partitions level, but done at the Bundle level (a higher level). The reason is to amortize the amount of information that you need to keep track. Based on CPU, memory, traffic load and other indexes, topics are assigned to a particular broker dynamically. 

Instead of individual topic or partition assignment, each broker takes ownership of a subset of the topics for a namespace. This subset is called a "*bundle*" and effectively this subset is a sharding mechanism.

The namespace is the "administrative" unit: many config knobs or operations are done at the namespace level.

For assignment, a namespace is sharded into a list of "bundles", with each bundle comprising a portion of the overall hash range of the namespace.

Topics are assigned to a particular bundle by taking the hash of the topic name and checking in which bundle the hash falls into.

Each bundle is independent of the others and thus is independently assigned to different brokers.

### Create namespaces and bundles

When you create a new namespace, the new namespace sets to use the default number of bundles. You can set this in the `conf/broker.conf` file:

```properties

# When a namespace is created without specifying the number of bundle, this
# value will be used as the default
defaultNumberOfNamespaceBundles=4

```

Or, you can override the value when you create a new namespace using [Pulsar admin](/tools/pulsar-admin/):

```shell

bin/pulsar-admin namespaces create my-tenant/my-namespace --clusters us-west --bundles 16

```

With the above command, you create a namespace with 16 initial bundles. Therefore the topics for this namespace can immediately be spread across up to 16 brokers.

In general, if you know the expected traffic and number of topics in advance, you had better start with a reasonable number of bundles instead of waiting for the system to auto-correct the distribution.

On the same note, it is beneficial to start with more bundles than the number of brokers, due to the hashing nature of the distribution of topics into bundles. For example, for a namespace with 1000 topics, using something like 64 bundles achieves a good distribution of traffic across 16 brokers.

### Unload topics and bundles

You can "unload" a topic in Pulsar with admin operation. Unloading means closing topics, releasing ownership, and reassigning topics to a new broker, based on the current load.

When unloading happens, the client experiences a small latency blip, typically in the order of tens of milliseconds, while the topic is reassigned.

Unloading is the mechanism that the load-manager uses to perform the load shedding, but you can also trigger the unloading manually, for example, to correct the assignments and redistribute traffic even before having any broker overloaded.

Unloading a topic has no effect on the assignment, but just closes and reopens the particular topic:

```shell

pulsar-admin topics unload persistent://tenant/namespace/topic

```

To unload all topics for a namespace and trigger reassignments:

```shell

pulsar-admin namespaces unload tenant/namespace

```

### Split namespace bundles 

Since the load for the topics in a bundle might change over time and predicting the load might be hard, bundle split is designed to resolve these chanllenges. The broker splits a bundle into two and the new smaller bundles can be reassigned to different brokers.

The splitting is based on some tunable thresholds. Any existing bundle that exceeds any of the thresholds is a candidate to be split. By default the newly split bundles are also immediately offloaded to other brokers, to facilitate the traffic distribution. 

You can split namespace bundles by setting `supportedNamespaceBundleSplitAlgorithms` to the following values in the `broker.conf` file. 
* `range_equally_divide`: split the bundle into two parts with the same hash range size.
* `topic_count_equally_divide`: split the bundle into two parts with the same number of topics. 

You can also configure other parameters for namespace bundles.

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

### Shed load automatically

The support for automatic load shedding is available in the load manager of Pulsar. This means that whenever the system recognizes a particular broker is overloaded, the system forces some traffic to be reassigned to less-loaded brokers.

When a broker is identified as overloaded, the broker forces to "unload" a subset of the bundles, the ones with higher traffic, that make up for the overload percentage.

For example, the default threshold is 85% and if a broker is over quota at 95% CPU usage, then the broker unloads the percent difference plus a 5% margin: `(95% - 85%) + 5% = 15%`.

Given the selection of bundles to offload is based on traffic (as a proxy measure for CPU, network, and memory), broker unloads bundles for at least 15% of traffic.

The automatic load shedding is enabled by default and you can disable the automatic load shedding with this setting:

```properties

# Enable/disable automatic bundle unloading for load-shedding
loadBalancerSheddingEnabled=true

```

Additional settings that apply to shedding:

```properties

# Load shedding interval. Broker periodically checks whether some traffic should be offload from
# some over-loaded broker to other under-loaded brokers
loadBalancerSheddingIntervalMinutes=1

# Prevent the same topics to be shed and moved to other brokers more than once within this timeframe
loadBalancerSheddingGracePeriodMinutes=30

```

Pulsar supports the following types of shedding strategies. From Pulsar 2.10, the **default** shedding strategy is `ThresholdShedder`.

:::note

You need to restart brokers if the shedding strategy is [dynamically updated](admin-api-brokers.md/#dynamic-broker-configuration). 

:::

#### ThresholdShedder
This strategy tends to shed the bundles if any broker's usage is above the configured threshold. It does this by first computing the average resource usage per broker for the whole cluster. The resource usage for each broker is calculated using the following method: LocalBrokerData#getMaxResourceUsageWithWeight. The weights for each resource are configurable. Historical observations are included in the running average based on the broker's setting for loadBalancerHistoryResourcePercentage. Once the average resource usage is calculated, a broker's current/historical usage is compared to the average broker usage. If a broker's usage is greater than the average usage per broker plus the loadBalancerBrokerThresholdShedderPercentage, this load shedder proposes removing enough bundles to bring the unloaded broker 5% below the current average broker usage. Note that recently unloaded bundles are not unloaded again. Configure broker with below value to use this strategy.
`loadBalancerLoadSheddingStrategy=org.apache.pulsar.broker.loadbalance.impl.ThresholdShedder`

![Shedding strategy - ThresholdShedder](/assets/ThresholdShedder.png)

#### OverloadShedder
This strategy will attempt to shed exactly one bundle on brokers which are overloaded, that is, whose maximum system resource usage exceeds loadBalancerBrokerOverloadedThresholdPercentage. To see which resources are considered when determining the maximum system resource. A bundle is recommended for unloading off that broker if and only if the following conditions hold: The broker has at least two bundles assigned and the broker has at least one bundle that has not been unloaded recently according to LoadBalancerSheddingGracePeriodMinutes. The unloaded bundle will be the most expensive bundle in terms of message rate that has not been recently unloaded. Note that this strategy does not take into account "underloaded" brokers when determining which bundles to unload. If you are looking for a strategy that spreads load evenly across all brokers, see ThresholdShedder. Configure broker with below value to use this strategy.
`loadBalancerLoadSheddingStrategy=org.apache.pulsar.broker.loadbalance.impl.OverloadShedder`

![Shedding strategy - OverloadShedder](/assets/OverloadShedder.png)

#### UniformLoadShedder
This strategy tends to distribute load uniformly across all brokers. This strategy checks the load difference between the broker with the highest load and the broker with the lowest load. If the difference is higher than configured thresholds `loadBalancerMsgRateDifferenceShedderThreshold` and `loadBalancerMsgThroughputMultiplierDifferenceShedderThreshold` then it finds out bundles that can be unloaded to distribute traffic evenly across all brokers. Configure broker with below value to use this strategy.
`loadBalancerLoadSheddingStrategy=org.apache.pulsar.broker.loadbalance.impl.UniformLoadShedder`

![Shedding strategy - UniformLoadShedder](/assets/UniformLoadShedder.png)

#### Broker overload thresholds

The determination of when a broker is overloaded is based on the threshold of CPU, network, and memory usage. Whenever either of those metrics reaches the threshold, the system triggers the shedding (if enabled).

By default, the overload threshold is set at 85%:

```properties

# Usage threshold to determine a broker as over-loaded
loadBalancerBrokerOverloadedThresholdPercentage=85

```

Pulsar gathers the usage stats from the system metrics.

In case of network utilization, in some cases the network interface speed that Linux reports is not correct and needs to be manually overridden. This is the case in AWS EC2 instances with 1Gbps NIC speed for which the OS reports 10Gbps speed.

Because of the incorrect max speed, the Pulsar load manager might think the broker has not reached the NIC capacity, while in fact the broker already uses all the bandwidth and the traffic is slowed down.

You can use the following setting to correct the max NIC speed:

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

When the value is empty, Pulsar uses the value that the OS reports.


## Distribute anti-affinity namespaces across failure domains

When your application has multiple namespaces and you want one of them available all the time to avoid any downtime, you can group these namespaces and distribute them across different [failure domains](reference-terminology.md#failure-domain) and different brokers. Thus, if one of the failure domains is down (due to release rollout or brokers restart), it only disrupts namespaces owned by that specific failure domain and the rest of the namespaces owned by other domains remain available without any impact.

Such a group of namespaces has anti-affinity to each other, that is, all the namespaces in this group are [anti-affinity namespaces](reference-terminology.md#anti-affinity-namespaces) and are distributed to different failure domains in a load-balanced manner. 
As illustrated in the following figure, Pulsar has 2 failure domains (Domain-1 and Domain-2) and each domain has 2 brokers in it. You have one anti-affinity group which has 4 namespaces in it. All 4 namespaces have anti-affinity to each other. However, Pulsar has only 2 failure domains, so each domain owns 2 namespaces. Also, the load-balancer tries to distribute namespaces evenly across all the brokers in the same domain. Since each domain has 2 brokers, every broker owns one namespace from this anti-affinity namespace group, and you can see that domain-1 and domain-2 own 2 namespaces each, and all 4 brokers own 1 namespace each.

[Distribute anti-affinity namespaces across failure domains](/assets/anti-affinity-namespaces-across-failure-domains.svg)

Each namespace can belong to only one anti-affinity group. If a namespace with an existing anti-affinity assignment is assigned to another anti-affinity group, the original assignment will be dropped.
 
:::tip

If there are more anti-affinity namespaces than failure domains, the load-manager distributes namespaces evenly across all the domains, and also every domain distributes namespaces evenly across all the brokers under that domain.

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
