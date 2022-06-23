---
id: administration-geo
title: Pulsar geo-replication
sidebar_label: "Geo-replication"
---

````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````


## Enable geo-replication for a namespace

You must enable geo-replication on a [per-tenant basis](#concepts-multi-tenancy) in Pulsar. For example, you can enable geo-replication between two specific clusters only when a tenant has access to both clusters.

Geo-replication is managed at the namespace level, which means you only need to create and configure a namespace to replicate messages between two or more provisioned clusters that a tenant can access.

Complete the following tasks to enable geo-replication for a namespace:

* [Enable a geo-replication namespace](#enable-geo-replication-at-namespace-level)
* [Configure that namespace to replicate across two or more provisioned clusters](admin-api-namespaces.md/#configure-replication-clusters)

Any message published on *any* topic in that namespace is replicated to all clusters in the specified set.

## Local persistence and forwarding

When messages are produced on a Pulsar topic, messages are first persisted in the local cluster, and then forwarded asynchronously to the remote clusters.

In normal cases, when connectivity issues are none, messages are replicated immediately, at the same time as they are dispatched to local consumers. Typically, the network [round-trip time](https://en.wikipedia.org/wiki/Round-trip_delay_time) (RTT) between the remote regions defines end-to-end delivery latency.

Applications can create producers and consumers in any of the clusters, even when the remote clusters are not reachable (like during a network partition).

Producers and consumers can publish messages to and consume messages from any cluster in a Pulsar instance. However, subscriptions cannot only be local to the cluster where the subscriptions are created but also can be transferred between clusters after replicated subscription is enabled. Once replicated subscription is enabled, you can keep subscription state in synchronization. Therefore, a topic can be asynchronously replicated across multiple geographical regions. In case of failover, a consumer can restart consuming messages from the failure point in a different cluster.

![A typical geo-replication example with full-mesh pattern](/assets/geo-replication.png)

In the aforementioned example, the **T1** topic is replicated among three clusters, **Cluster-A**, **Cluster-B**, and **Cluster-C**.

All messages produced in any of the three clusters are delivered to all subscriptions in other clusters. In this case, **C1** and **C2** consumers receive all messages that **P1**, **P2**, and **P3** producers publish. Ordering is still guaranteed on a per-producer basis.

## Configure replication

This section guides you through the steps to configure geo-replicated clusters.
1. [Connect replication clusters](#connect-replication-clusters)
2. [Grant permissions to properties](#grant-permissions-to-properties)
3. [Enable geo-replication](#enable-geo-replication)
4. [Use topics with geo-replication](#use-topics-with-geo-replication)

### Connect replication clusters

To replicate data among clusters, you need to configure each cluster to connect to the other. You can use the [`pulsar-admin`](/tools/pulsar-admin/) tool to create a connection.

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

   - If you want to use a secure connection for a cluster, you can use the flags `--broker-url-secure` and `--url-secure`. For more information, see [pulsar-admin clusters create](/tools/pulsar-admin/).
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

### Enable geo-replication 

You can enable geo-replication at **namespace** or **topic** level.

#### Enable geo-replication at namespace level

You can create a namespace with the following command sample.

```shell

$ bin/pulsar-admin namespaces create my-tenant/my-namespace

```

Initially, the namespace is not assigned to any cluster. You can assign the namespace to clusters using the `set-clusters` subcommand:

```shell

$ bin/pulsar-admin namespaces set-clusters my-tenant/my-namespace \
  --clusters us-west,us-east,us-cent

```

#### Enable geo-replication at topic level

You can set geo-replication at topic level using the command `pulsar-admin topics set-replication-clusters`. For the latest and complete information about `Pulsar admin`, including commands, flags, descriptions, and more information, see [Pulsar admin doc](/tools/pulsar-admin/).

```shell

$ bin/pulsar-admin topics set-replication-clusters --clusters us-west,us-east,us-cent my-tenant/my-namespace/my-topic

```

:::tip

- You can change the replication clusters for a namespace at any time, without disruption to ongoing traffic. Replication channels are immediately set up or stopped in all clusters as soon as the configuration changes.
- Once you create a geo-replication namespace, any topics that producers or consumers create within that namespace are replicated across clusters. Typically, each application uses the `serviceUrl` for the local cluster.

:::

### Use topics with geo-replication

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

You can check topic-specific statistics for geo-replication topics using one of the following methods.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"}]}>
<TabItem value="pulsar-admin">

Use the [`pulsar-admin topics stats`](/tools/pulsar-admin/) command.

```shell

$ bin/pulsar-admin topics stats persistent://my-tenant/my-namespace/my-topic

```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace/:topic/stats|operation/getStats?version=@pulsar:version_number@}

</TabItem>

</Tabs>
````

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

* When you enable replicated subscription, you're creating a consistent distributed snapshot to establish an association between message ids from different clusters. The snapshots are taken periodically. The default value is `1 second`. It means that a consumer failing over to a different cluster can potentially receive 1 second of duplicates. You can also configure the frequency of the snapshot in the `broker.conf` file.
* Only the base line cursor position is synced in replicated subscriptions while the individual acknowledgments are not synced. This means the messages acknowledged out-of-order could end up getting delivered again, in the case of a cluster failover.

## Migrate data between clusters using geo-replication

Using geo-replication to migrate data between clusters is a special use case of the [active-active replication pattern](concepts-replication.md/#active-active-replication) when you don't have a large amount of data.

1. Create your new cluster.
2. Add the new cluster to your old cluster.

```shell

  bin/pulsar-admin cluster create new-cluster

```

3. Add the new cluster to your tenant.

```shell

  bin/pulsar-admin tenants update my-tenant --cluster old-cluster,new-cluster

```

4. Set the clusters on your namespace.

```shell

  bin/pulsar-admin namespaces set-clusters my-tenant/my-ns --cluster old-cluster,new-cluster

```

5. Update your applications using [replicated subscriptions](#replicated-subscriptions).
6. Validate subscription replication is active.

```shell

  bin/pulsar-admin topics stats-internal public/default/t1

```

7. Move your consumers and producers to the new cluster by modifying the values of `serviceURL`.

:::note

* The replication starts from step 4, which means existing messages in your old cluster are not replicated. 
* If you have some older messages to migrate, you can pre-create the replication subscriptions for each topic and set it at the earliest position by using `pulsar-admin topics create-subscription -s pulsar.repl.new-cluster -m earliest <topic>`.

:::

