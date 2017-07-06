---
title: Pulsar geo-replication
---

*Geo-replication* is the replication of persistently stored message data across multiple {% popover clusters %} of a Pulsar {% popover instance %}.

## How it works

The diagram below illustrates the process of geo-replication across Pulsar clusters:

![Replication Diagram]({{ site.baseurl }}img/GeoReplication.png)

In this diagram, whenever producers **P1**, **P2**, and **P3** publish messages to the topic **T1** on clusters **Cluster-A**, **Cluster-B**, and **Cluster-C**, respectively, those messages are instantly replicated across clusters. Once replicated, consumers **C1** and **C2** can consume those messages from their respective clusters.

Without geo-replication, consumers **C1** and **C2** wouldn't be able to consume messages published by producer **P3**.

## Geo-replication and Pulsar properties

Geo-replication must be enabled on a per-{% popover property %} basis in Pulsar. Geo-replication can be enabled between clusters only when a property has been created that allows access to both clusters.

Although geo-replication must be enabled between two clusters, it's actually managed at the {% popover namespace %} level. You must do the following to enable geo-replication for a namespace:

* [Create a global namespace](#creating-a-global-namespace)
* Configure that namespace to replicate between two or more provisioned clusters

Any message published on *any* topic in that namespace will then be replicated to all clusters in the specified set.

## Local persistence and forwarding

When messages are produced on a Pulsar topic, they are first persisted in the local cluster and then forwarded asynchronously to the remote clusters.

In normal cases, when there are no connectivity issues, messages are replicated immediately, at the same time as they are dispatched to local consumers. Typically, end-to-end delivery latency is defined by the network [round-trip time](https://en.wikipedia.org/wiki/Round-trip_delay_time) (RTT) between the remote regions.

Applications can create producers and consumers in any of the clusters, even when the remote clusters are not reachable (like during a network partition).

{% include message.html id="subscriptions_local" %}

In the example in the image above, the topic **T1** is being replicated between 3 clusters, **Cluster-A**, **Cluster-B**, and **Cluster-C**.

All messages produced in any cluster will be delivered to all subscriptions in all the other clusters. In this case, consumers **C1** and **C2** will receive all messages published by producers **P1**, **P2**, and **P3**. Ordering is still guaranteed on a per-producer basis.

## Configuring replication

As stated [above](#geo-replication-and-pulsar-properties), geo-replication in Pulsar is managed at the {% popover property %} level.

### Granting permissions to properties

To establish replication to a cluster, the {% popover tenant %} needs permission to use that cluster. This permission can be granted when the property is created or later on.

At creation time, specify all the intended clusters:

```shell
$ bin/pulsar-admin properties create my-property \
  --admin-roles my-admin-role \
  --allowed-clusters us-west,us-east,us-cent
```

To update permissions of an existing property, use `update` instead of `create`.

### Creating global namespaces

Replication must be used with *global* topics, meaning topics that belong to a global {% popover namespace %} and are thus not tied to any particular cluster.

Global namespaces need to be created in the `global` virtual cluster. For example:

```shell
$ bin/pulsar-admin namespaces create my-property/global/my-namespace
```

Initially, the namespace is not assigned to any cluster. You can assign the namespace to clusters using the `set-clusters` subcommand:

```shell
$ bin/pulsar-admin namespaces set-clusters my-property/global/my-namespace \
  --clusters us-west,us-east,us-cent
```

The replication clusters for a namespace can be changed at any time, with no disruption to ongoing traffic. Replication channels will be immediately set up or stopped in all the clusters as soon as the configuration changes.

### Using global topics

Once you've created a global namespace, any topics that producers or consumers create within that namespace will be global. Typically, each application will use the `serviceUrl` for the local cluster.

#### Selective replication

By default, messages are replicated to all clusters configured for the namespace. You can restrict replication selectively by specifying a replication list for a message. That message will then be replicated only to the subset in the replication list.

Below is an example for the [Java API](../../applications/JavaClient). Note the use of the `setReplicationClusters` method when constructing the {% javadoc Message client com.yahoo.pulsar.client.api.Message %} object:

```java
List<String> restrictReplicationTo = new ArrayList<>;
restrictReplicationTo.add("us-west");
restrictReplicationTo.add("us-east");

Message message = MessageBuilder.create()
        .setContent("my-payload".getBytes())
        .setReplicationClusters(restrictReplicationTo)
        .build();

producer.send(message);
```

#### Topic stats

Topic-specific statistics for global topics are available via the [`pulsar-admin`](../../reference/CliTools#pulsar-admin) tool and [REST API](../../reference/RestApi):

```shell
$ bin/pulsar-admin persistent stats persistent://my-property/global/my-namespace/my-topic
```

Each cluster reports its own local stats, including incoming and outgoing replication rates and backlogs.

#### Deleting a global topic

Given that global topics exist in multiple regions, it's not possible to directly delete a global topic. Instead, you should rely on automatic topic garbage collection.

In Pulsar, a topic is automatically deleted when it's no longer used, that is to say, when no producers or consumers are connected *and* there are no subscriptions. For global topics, each region will use a fault-tolerant mechanism to decide when it's safe to delete the topic locally.

To delete a global topic, close all producers and consumers on the topic and delete all its local subscriptions in every replication cluster. When Pulsar determines that no valid subscription for the topic remains across the system, it will garbage collect the topic.
