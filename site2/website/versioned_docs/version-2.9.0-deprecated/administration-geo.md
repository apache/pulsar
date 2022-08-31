---
id: administration-geo
title: Pulsar geo-replication
sidebar_label: "Geo-replication"
original_id: administration-geo
---

*Geo-replication* is the replication of persistently stored message data across multiple clusters of a Pulsar instance.

## How geo-replication works

The diagram below illustrates the process of geo-replication across Pulsar clusters:

![Replication Diagram](/assets/geo-replication.png)

In this diagram, whenever **P1**, **P2**, and **P3** producers publish messages to the **T1** topic on **Cluster-A**, **Cluster-B**, and **Cluster-C** clusters respectively, those messages are instantly replicated across clusters. Once the messages are replicated, **C1** and **C2** consumers can consume those messages from their respective clusters.

Without geo-replication, **C1** and **C2** consumers are not able to consume messages that **P3** producer publishes.

## Geo-replication and Pulsar properties

You must enable geo-replication on a per-tenant basis in Pulsar. You can enable geo-replication between clusters only when a tenant is created that allows access to both clusters.

Although geo-replication must be enabled between two clusters, actually geo-replication is managed at the namespace level. You must complete the following tasks to enable geo-replication for a namespace:

* [Enable geo-replication namespaces](#enable-geo-replication-namespaces)
* Configure that namespace to replicate across two or more provisioned clusters

Any message published on *any* topic in that namespace is replicated to all clusters in the specified set.

## Local persistence and forwarding

When messages are produced on a Pulsar topic, messages are first persisted in the local cluster, and then forwarded asynchronously to the remote clusters.

In normal cases, when connectivity issues are none, messages are replicated immediately, at the same time as they are dispatched to local consumers. Typically, the network [round-trip time](https://en.wikipedia.org/wiki/Round-trip_delay_time) (RTT) between the remote regions defines end-to-end delivery latency.

Applications can create producers and consumers in any of the clusters, even when the remote clusters are not reachable (like during a network partition).

Producers and consumers can publish messages to and consume messages from any cluster in a Pulsar instance. However, subscriptions cannot only be local to the cluster where the subscriptions are created but also can be transferred between clusters after replicated subscription is enabled. Once replicated subscription is enabled, you can keep subscription state in synchronization. Therefore, a topic can be asynchronously replicated across multiple geographical regions. In case of failover, a consumer can restart consuming messages from the failure point in a different cluster.

In the aforementioned example, the **T1** topic is replicated among three clusters, **Cluster-A**, **Cluster-B**, and **Cluster-C**.

All messages produced in any of the three clusters are delivered to all subscriptions in other clusters. In this case, **C1** and **C2** consumers receive all messages that **P1**, **P2**, and **P3** producers publish. Ordering is still guaranteed on a per-producer basis.

## Configure replication

As stated in [Geo-replication and Pulsar properties](#geo-replication-and-pulsar-properties) section, geo-replication in Pulsar is managed at the [tenant](reference-terminology.md#tenant) level.

The following example connects three clusters: **us-east**, **us-west**, and **us-cent**.

### Connect replication clusters

To replicate data among clusters, you need to configure each cluster to connect to the other. You can use the [`pulsar-admin`](https://pulsar.apache.org/tools/pulsar-admin/) tool to create a connection.

**Example**

Suppose that you have 3 replication clusters: `us-west`, `us-cent`, and `us-east`.

1. Configure the connection from `us-west` to `us-east`.

   Run the following command on `us-west`.

```shell

$ bin/pulsar-admin clusters create \
  --broker-url pulsar://<DNS-OF-US-EAST>:<PORT>	\
  --url http://<DNS-OF-US-EAST>:<PORT> \
  us-east

```

   :::tip

   - If you want to use a secure connection for a cluster, you can use the flags `--broker-url-secure` and `--url-secure`. For more information, see [pulsar-admin clusters create](https://pulsar.apache.org/tools/pulsar-admin/).
   - Different clusters may have different authentications. You can use the authentication flag `--auth-plugin` and `--auth-parameters` together to set cluster authentication, which overrides `brokerClientAuthenticationPlugin` and `brokerClientAuthenticationParameters` if `authenticationEnabled` sets to `true` in `broker.conf` and `standalone.conf`. For more information, see [authentication and authorization](concepts-authentication).

   :::

2. Configure the connection from `us-west` to `us-cent`.

   Run the following command on `us-west`.

```shell

$ bin/pulsar-admin clusters create \
  --broker-url pulsar://<DNS-OF-US-CENT>:<PORT>	\
  --url http://<DNS-OF-US-CENT>:<PORT> \
  us-cent

```

3. Run similar commands on `us-east` and `us-cent` to create connections among clusters.

### Grant permissions to properties

To replicate to a cluster, the tenant needs permission to use that cluster. You can grant permission to the tenant when you create the tenant or grant later.

Specify all the intended clusters when you create a tenant:

```shell

$ bin/pulsar-admin tenants create my-tenant \
  --admin-roles my-admin-role \
  --allowed-clusters us-west,us-east,us-cent

```

To update permissions of an existing tenant, use `update` instead of `create`.

### Enable geo-replication namespaces

You can create a namespace with the following command sample.

```shell

$ bin/pulsar-admin namespaces create my-tenant/my-namespace

```

Initially, the namespace is not assigned to any cluster. You can assign the namespace to clusters using the `set-clusters` subcommand:

```shell

$ bin/pulsar-admin namespaces set-clusters my-tenant/my-namespace \
  --clusters us-west,us-east,us-cent

```

You can change the replication clusters for a namespace at any time, without disruption to ongoing traffic. Replication channels are immediately set up or stopped in all clusters as soon as the configuration changes.

### Use topics with geo-replication

Once you create a geo-replication namespace, any topics that producers or consumers create within that namespace is replicated across clusters. Typically, each application uses the `serviceUrl` for the local cluster.

#### Selective replication

By default, messages are replicated to all clusters configured for the namespace. You can restrict replication selectively by specifying a replication list for a message, and then that message is replicated only to the subset in the replication list.

The following is an example for the [Java API](client-libraries-java). Note the use of the `setReplicationClusters` method when you construct the {@inject: javadoc:Message:/client/org/apache/pulsar/client/api/Message} object:

```java

List<String> restrictReplicationTo = Arrays.asList(
        "us-west",
        "us-east"
);

Producer producer = client.newProducer()
        .topic("some-topic")
        .create();

producer.newMessage()
        .value("my-payload".getBytes())
        .setReplicationClusters(restrictReplicationTo)
        .send();

```

#### Topic stats

Topic-specific statistics for geo-replication topics are available via the [`pulsar-admin`](reference-pulsar-admin) tool and {@inject: rest:REST:/} API:

```shell

$ bin/pulsar-admin persistent stats persistent://my-tenant/my-namespace/my-topic

```

Each cluster reports its own local stats, including the incoming and outgoing replication rates and backlogs.

#### Delete a geo-replication topic

Given that geo-replication topics exist in multiple regions, directly deleting a geo-replication topic is not possible. Instead, you should rely on automatic topic garbage collection.

In Pulsar, a topic is automatically deleted when the topic meets the following three conditions:
- no producers or consumers are connected to it;
- no subscriptions to it;
- no more messages are kept for retention. 
For geo-replication topics, each region uses a fault-tolerant mechanism to decide when deleting the topic locally is safe.

You can explicitly disable topic garbage collection by setting `brokerDeleteInactiveTopicsEnabled` to `false` in your [broker configuration](reference-configuration.md#broker).

To delete a geo-replication topic, close all producers and consumers on the topic, and delete all of its local subscriptions in every replication cluster. When Pulsar determines that no valid subscription for the topic remains across the system, it will garbage collect the topic.

## Replicated subscriptions

Pulsar supports replicated subscriptions, so you can keep subscription state in sync, within a sub-second timeframe, in the context of a topic that is being asynchronously replicated across multiple geographical regions.

In case of failover, a consumer can restart consuming from the failure point in a different cluster. 

### Enable replicated subscription

Replicated subscription is disabled by default. You can enable replicated subscription when creating a consumer. 

```java

Consumer<String> consumer = client.newConsumer(Schema.STRING)
            .topic("my-topic")
            .subscriptionName("my-subscription")
            .replicateSubscriptionState(true)
            .subscribe();

```

### Advantages

 * It is easy to implement the logic. 
 * You can choose to enable or disable replicated subscription.
 * When you enable it, the overhead is low, and it is easy to configure. 
 * When you disable it, the overhead is zero.

### Limitations

When you enable replicated subscription, you're creating a consistent distributed snapshot to establish an association between message ids from different clusters. The snapshots are taken periodically. The default value is `1 second`. It means that a consumer failing over to a different cluster can potentially receive 1 second of duplicates. You can also configure the frequency of the snapshot in the `broker.conf` file.
