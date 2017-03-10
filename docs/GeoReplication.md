
# Pulsar Geo-Replication

<!-- TOC depthFrom:2 depthTo:3 withLinks:1 updateOnSave:1 orderedList:0 -->

- [Replication model](#replication-model)
- [Configuring replication](#configuring-replication)
	- [Granting permissions to properties](#granting-permissions-to-properties)
	- [Creating global namespaces](#creating-global-namespaces)
	- [Using global topics](#using-global-topics)

<!-- /TOC -->

## Replication model

Pulsar properties provisioned on more than one cluster can enable replication
among those clusters.  Replication is managed directly by the users, at the
namespace level. Any topic in a global namespace is replicated. A user creates a
global namespace, and configures that namespace to replicate between two or more
of the provisioned clusters. Any message published on any topic in that namespace
will then be replicated to all clusters in that set.

Messages are first persisted in the local cluster, and then forwarded
asynchronously to the remote clusters.

In normal case, with no connectivity problems, messages are replicated immediately,
at the same time as they are dispatched to local consumers. Typically, end-to-end
delivery latency is defined by the network *RTT* between the remote regions.

Applications can create producers and consumers in any of the clusters,
even when the remote clusters are not reachable (eg: during a network
partition).

Subscriptions are local to the clusters in which they are created and they
cannot be transferred between clusters.

![Replication Diagram](img/GeoReplication.png)

In the above example, the topic is being replicated between 3 clusters,
***Cluster-A***, ***Cluster-B*** and ***Cluster-C***.

All messages produced in any cluster will be delivered to all subscriptions
in all the clusters. In this case, both ***C1*** and ***C2*** will receive all
messages published by ***P1***, ***P2*** and ***P3***. Ordering is still
guaranteed per-producer.

## Configuring replication

### Granting permissions to properties

To replicate to a cluster, the tenant needs permission to use that cluster.
This permission can be granted when the property is created, or it can be
updated later.

At creation time, specify all the intended clusters:
```shell
$ bin/pulsar-admin properties create my-property    \
        --admin-roles my-admin-role                 \
        --allowed-clusters us-west,us-east,us-cent
```

To update permissions of an existing property, use `update` instead of `create`.

### Creating global namespaces

To use replication, we need a *global* topic, that is to say, a topic
that belongs to a *global* namespace, not tied to any particular cluster.

Global namespaces need to be created in the ***global*** virtual cluster.
For example:

```shell
$ bin/pulsar-admin namespaces create my-property/global/my-namespace
```

Initially, the namespace is not assigned to any cluster.

```shell
$ bin/pulsar-admin namespaces set-clusters
                          my-property/global/my-namespace \
                          --clusters us-west,us-east,us-cent
```

The replication clusters for a namespace can be changed at any time, with no
disruption to ongoing traffic. Replication channels will be immediately
set up or stopped in all the clusters as soon as the configuration changes.

### Using global topics

At this point you can just create a producer or consumers and automatically
create a global topic.

Typically, each application will use the `serviceUrl` for the local cluster.

#### Selective replication

By default, messages are replicated to all configured clusters for the namespace.
You can restrict replication selectively, by specifying a replication list for a
message. The message will then be replicated only to the subset in the
replication list.

In the Java API, this is done on the message builder:
```java
...
List<String> restrictReplicationTo = new ArrayList<>();
restrictReplicationTo.add("us-west");
restrictReplicationTo.add("us-east");

Message message = MessageBuilder.create()
              .setContent("my-payload".getBytes())
              .setReplicationClusters(restrictReplicationTo)
              .build();

producer.send(message);
```

#### Topic stats

Topic stats for global topics are available through the same API and tools:

```shell
$ bin/pulsar-admin persistent stats persistent://my-property/global/my-namespace/my-topic
```

Each cluster will report its own local stats, including incoming and outgoing
replication rates and backlogs.

#### Deleting a global topic

Given that the topic exist in multiple regions, it's not possible to directly
delete a global topic. The procedure is to rely on the automatic topic garbage
collection.

In Pulsar, a topic is automatically deleted when it's not used anymore, that is
to say, when no producers or consumers are connected ***and*** there are no
subscriptions.

For global topics, each region will use a fault-tolerant mechanism to decide
when it's safe to delete the topic locally.

To delete a global topic, close all producers and consumers on the topic and
delete all its local subscriptions in every replication cluster. When Pulsar
determines that no valid subscription for the topic remains across the system,
it will garbage collect the topic.
