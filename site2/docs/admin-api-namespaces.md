---
id: admin-api-namespaces
title: Managing Namespaces
sidebar_label: "Namespaces"
---

````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````


:::tip

 This page only shows **some frequently used operations**.

 - For the latest and complete information about `Pulsar admin`, including commands, flags, descriptions, and more information, see [Pulsar admin doc](/tools/pulsar-admin/).
 
 - For the latest and complete information about `REST API`, including parameters, responses, samples, and more, see {@inject: rest:REST:/} API doc.
 
 - For the latest and complete information about `Java admin API`, including classes, methods, descriptions, and more, see [Java admin API doc](/api/admin/).
 
:::

Pulsar [namespaces](reference-terminology.md#namespace) are logical groupings of [topics](reference-terminology.md#topic).

Namespaces can be managed via:

* The `namespaces` command of the [`pulsar-admin`](/tools/pulsar-admin/) tool
* The `/admin/v2/namespaces` endpoint of the admin {@inject: rest:REST:/} API
* The `namespaces` method of the `PulsarAdmin` object in the [Java API](client-libraries-java.md)

## Namespaces resources

### Create namespaces

You can create new namespaces under a given [tenant](reference-terminology.md#tenant).

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

Use the [`create`](/tools/pulsar-admin/) subcommand and specify the namespace by name:

```shell
pulsar-admin namespaces create test-tenant/test-namespace
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|PUT|/admin/v2/namespaces/:tenant/:namespace|operation/createNamespace?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.namespaces().createNamespace(namespace);
```

</TabItem>

</Tabs>
````

### Get policies

You can fetch the current policies associated with a namespace at any time.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

Use the [`policies`](/tools/pulsar-admin/) subcommand and specify the namespace:

```shell
pulsar-admin namespaces policies test-tenant/test-namespace
```

Example output:

```json
{
  "auth_policies": {
    "namespace_auth": {},
    "destination_auth": {}
  },
  "replication_clusters": [],
  "bundles_activated": true,
  "bundles": {
    "boundaries": [
      "0x00000000",
      "0xffffffff"
    ],
    "numBundles": 1
  },
  "backlog_quota_map": {},
  "persistence": null,
  "latency_stats_sample_rate": {},
  "message_ttl_in_seconds": 0,
  "retention_policies": null,
  "deleted": false
}
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/namespaces/:tenant/:namespace|operation/getPolicies?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.namespaces().getPolicies(namespace);
```

</TabItem>

</Tabs>
````

### List namespaces

You can list all namespaces within a given Pulsar [tenant](reference-terminology.md#tenant).

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

Use the [`list`](/tools/pulsar-admin/) subcommand and specify the tenant:

```shell
pulsar-admin namespaces list test-tenant
```

Example output:

```
test-tenant/namespace1
test-tenant/namespace2
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/namespaces/:tenant|operation/getTenantNamespaces?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.namespaces().getNamespaces(tenant);
```

</TabItem>

</Tabs>
````

### Delete namespaces

You can delete existing namespaces from a tenant.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

Use the [`delete`](/tools/pulsar-admin/) subcommand and specify the namespace:

```shell
pulsar-admin namespaces delete test-tenant/namespace1
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|DELETE|/admin/v2/namespaces/:tenant/:namespace|operation/deleteNamespace?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.namespaces().deleteNamespace(namespace);
```

</TabItem>

</Tabs>
````

### Configure replication clusters

#### Set replication cluster

You can set replication clusters for a namespace to enable Pulsar to internally replicate the published messages from one colocation facility to another.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin namespaces set-clusters test-tenant/namespace1 --clusters cl1
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/replication|operation/setNamespaceReplicationClusters?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.namespaces().setNamespaceReplicationClusters(namespace, clusters);
```

</TabItem>

</Tabs>
````

#### Get replication cluster

You can get the list of replication clusters for a given namespace.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin namespaces get-clusters test-tenant/cluster1/namespace1
```

Example output:

```
cluster2
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/namespaces/:tenant/:namespace/replication|operation/getNamespaceReplicationClusters?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.namespaces().getNamespaceReplicationClusters(namespace)
```

</TabItem>

</Tabs>
````

### Configure backlog quota policies

#### Set backlog quota policies

Backlog quota helps the broker to restrict bandwidth/storage of a namespace once it reaches a certain threshold limit. Admin can set the limit and take the corresponding action after the limit is reached.

  1.  producer_request_hold: the producer holds the message and retries until client configuration `sendTimeoutMs` is exceeded

  2.  producer_exception: the producer throws an exception when trying to send a message

  3.  consumer_backlog_eviction: broker starts discarding backlog messages

Backlog quota restriction can be taken care of by defining the restriction of backlog-quota-type: destination_storage.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin namespaces set-backlog-quota --limit 10G \
    --limitTime 36000 \
    --policy producer_request_hold \
    test-tenant/namespace1
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/backlogQuota|operation/setBacklogQuota?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.namespaces().setBacklogQuota(namespace, new BacklogQuota(limit, limitTime, policy))
```

</TabItem>

</Tabs>
````

#### Get backlog quota policies

You can get a configured backlog quota for a given namespace.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin namespaces get-backlog-quotas test-tenant/namespace1
```

Example output:

```
destination_storage    BacklogQuotaImpl(limit=10737418240, limitSize=10737418240, limitTime=36000, policy=producer_request_hold)
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/namespaces/:tenant/:namespace/backlogQuotaMap|operation/getBacklogQuotaMap?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.namespaces().getBacklogQuotaMap(namespace);
```

</TabItem>

</Tabs>
````

#### Remove backlog quota policies

You can remove backlog quota policies for a given namespace.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin namespaces remove-backlog-quota test-tenant/namespace1
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|DELETE|/admin/v2/namespaces/:tenant/:namespace/backlogQuota|operation/removeBacklogQuota?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.namespaces().removeBacklogQuota(namespace, backlogQuotaType)
```

</TabItem>

</Tabs>
````

### Configure persistence policies

#### Set persistence policies

Persistence policies allow users to configure persistency-level for all topic messages under a given namespace.

  -   Bookkeeper-ack-quorum: Number of acks (guaranteed copies) to wait for each entry, default: 0

  -   Bookkeeper-ensemble: Number of bookies to use for a topic, default: 0

  -   Bookkeeper-write-quorum: How many writes to make of each entry, default: 0

  -   Ml-mark-delete-max-rate: Throttling rate of mark-delete operation (0 means no throttle), default: 0.0

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin namespaces set-persistence \
    --bookkeeper-ack-quorum 2 --bookkeeper-ensemble 3 \
    --bookkeeper-write-quorum 2 --ml-mark-delete-max-rate 0 \
    test-tenant/namespace1
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/persistence|operation/setPersistence?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.namespaces().setPersistence(namespace,new PersistencePolicies(bookkeeperEnsemble, bookkeeperWriteQuorum,bookkeeperAckQuorum,managedLedgerMaxMarkDeleteRate))
```

</TabItem>

</Tabs>
````

#### Get persistence policies

You can get the configured persistence policies of a given namespace.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin namespaces get-persistence test-tenant/namespace1
```

Example output:

```json
{
  "bookkeeperEnsemble": 3,
  "bookkeeperWriteQuorum": 2,
  "bookkeeperAckQuorum": 2,
  "managedLedgerMaxMarkDeleteRate": 0
}
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/namespaces/:tenant/:namespace/persistence|operation/getPersistence?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.namespaces().getPersistence(namespace)
```

</TabItem>

</Tabs>
````

### Configure namespace bundles

#### Unload namespace bundles

A namespace bundle is a virtual group of topics that belong to the same namespace. If the broker gets overloaded with the number of bundles, this command can help unload a bundle from that broker, so it can be served by some other less-loaded brokers. The namespace bundle ID ranges from 0x00000000 to 0xffffffff.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin namespaces unload --bundle 0x00000000_0xffffffff test-tenant/namespace1
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|PUT|/admin/v2/namespaces/:tenant/:namespace/:bundle/unload|operation/unloadNamespaceBundle?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.namespaces().unloadNamespaceBundle(namespace, bundle)
```

</TabItem>

</Tabs>
````

#### Split namespace bundles

One namespace bundle can contain multiple topics but can be served by only one broker. If a single bundle is creating an excessive load on a broker, an admin can split the bundle using the command below, permitting one or more of the new bundles to be unloaded, thus balancing the load across the brokers.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin namespaces split-bundle --bundle 0x00000000_0xffffffff test-tenant/namespace1
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|PUT|/admin/v2/namespaces/:tenant/:namespace/:bundle/split|operation/splitNamespaceBundle?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.namespaces().splitNamespaceBundle(namespace, bundle)
```

</TabItem>

</Tabs>
````

### Configure message TTL

#### Set message-ttl

You can configure the time to live (in seconds) duration for messages. In the example below, the message-ttl is set as 100s.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin namespaces set-message-ttl --messageTTL 100 test-tenant/namespace1
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/messageTTL|operation/setNamespaceMessageTTL?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.namespaces().setNamespaceMessageTTL(namespace, messageTTL)
```

</TabItem>

</Tabs>
````

#### Get message-ttl

When the message-ttl for a namespace is set, you can use the command below to get the configured value. This example comtinues the example of the command `set message-ttl`, so the returned value is 100(s).

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin namespaces get-message-ttl test-tenant/namespace1
```
Example output:

```
100
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/namespaces/:tenant/:namespace/messageTTL|operation/getNamespaceMessageTTL?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.namespaces().getNamespaceMessageTTL(namespace)
```

```
100
```

</TabItem>

</Tabs>
````

#### Remove message-ttl

Remove a message TTL of the configured namespace.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin namespaces remove-message-ttl test-tenant/namespace1
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|DELETE|/admin/v2/namespaces/:tenant/:namespace/messageTTL|operation/removeNamespaceMessageTTL?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.namespaces().removeNamespaceMessageTTL(namespace)
```

</TabItem>

</Tabs>
````


### Clear backlog

#### Clear namespace backlog

It clears all message backlogs for all the topics that belong to a specific namespace. You can also clear backlogs for a specific subscription as well.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin namespaces clear-backlog --sub my-subscription test-tenant/namespace1
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/clearBacklog|operation/clearNamespaceBacklogForSubscription?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.namespaces().clearNamespaceBacklogForSubscription(namespace, subscription)
```

</TabItem>

</Tabs>
````

#### Clear bundle backlog

It clears all message backlogs for all the topics that belong to a specific NamespaceBundle. You can also clear backlogs for a specific subscription as well.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin namespaces clear-backlog \
    --bundle 0x00000000_0xffffffff \
    --sub my-subscription \
    test-tenant/namespace1
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/:bundle/clearBacklog|operation/clearNamespaceBundleBacklogForSubscription?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java

admin.namespaces().clearNamespaceBundleBacklogForSubscription(namespace, bundle, subscription)

```

</TabItem>

</Tabs>
````

### Configure retention

#### Set retention

Each namespace contains multiple topics and the retention size (storage size) of each topic should not exceed a specific threshold or it should be stored for a certain period. This command helps configure the retention size and time of topics in a given namespace.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin namespaces set-retention --size 100M --time 10m test-tenant/namespace1
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/retention|operation/setRetention?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.namespaces().setRetention(namespace, new RetentionPolicies(retentionTimeInMin, retentionSizeInMB))
```

</TabItem>

</Tabs>
````

#### Get retention

It shows the retention information of a given namespace.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin namespaces get-retention test-tenant/namespace1
```

```json
{
  "retentionTimeInMinutes": 10,
  "retentionSizeInMB": 100
}
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/namespaces/:tenant/:namespace/retention|operation/getRetention?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.namespaces().getRetention(namespace)
```

</TabItem>

</Tabs>
````

### Configure dispatch throttling for topics

#### Set dispatch throttling for topics

It sets the message dispatch rate for all the topics under a given namespace. The dispatch rate can be restricted by the number of messages per X seconds (`msg-dispatch-rate`) or by the number of message-bytes per X second (`byte-dispatch-rate`). The dispatch rate is in second and it can be configured with `dispatch-rate-period`. The default value of `msg-dispatch-rate` and `byte-dispatch-rate` is -1 which
disables the throttling.

:::note

- If neither `clusterDispatchRate` nor `topicDispatchRate` is configured, dispatch throttling is disabled.
- If `topicDispatchRate` is not configured, `clusterDispatchRate` takes effect.
- If `topicDispatchRate` is configured, `topicDispatchRate` takes effect.

:::

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin namespaces set-dispatch-rate test-tenant/namespace1 \
    --msg-dispatch-rate 1000 \
    --byte-dispatch-rate 1048576 \
    --dispatch-rate-period 1
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/dispatchRate|operation/setDispatchRate?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.namespaces().setDispatchRate(namespace, new DispatchRate(1000, 1048576, 1))
```

</TabItem>

</Tabs>
````

#### Get configured message-rate for topics

It shows the configured message-rate for the namespace (topics under this namespace can dispatch this many messages per second)

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin namespaces get-dispatch-rate test-tenant/namespace1
```

Example output:

```json
{
  "dispatchThrottlingRateInMsg" : 1000,
  "dispatchThrottlingRateInByte" : 1048576,
  "relativeToPublishRate" : false,
  "ratePeriodInSecond" : 1
}
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/namespaces/:tenant/:namespace/dispatchRate|operation/getDispatchRate?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.namespaces().getDispatchRate(namespace)
```

</TabItem>

</Tabs>
````

### Configure dispatch throttling for subscription

#### Set dispatch throttling for subscription

It sets the message dispatch rate for all the subscriptions of topics under a given namespace. The dispatch rate can be restricted by the number of messages per X seconds (`msg-dispatch-rate`) or by the number of message-bytes per X second (`byte-dispatch-rate`). The dispatch rate is in second and it can be configured with `dispatch-rate-period`. The default value of `msg-dispatch-rate` and `byte-dispatch-rate` is -1 which
disables the throttling.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin namespaces set-subscription-dispatch-rate test-tenant/namespace1 \
    --msg-dispatch-rate 1000 \
    --byte-dispatch-rate 1048576 \
    --dispatch-rate-period 1
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/subscriptionDispatchRate|operation/setDispatchRate?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.namespaces().setSubscriptionDispatchRate(namespace, new DispatchRate(1000, 1048576, 1))
```

</TabItem>

</Tabs>
````

#### Get configured message-rate for subscription

It shows the configured message-rate for the namespace (topics under this namespace can dispatch this many messages per second).

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin namespaces get-subscription-dispatch-rate test-tenant/namespace1
```

Example output:

```json
{
  "dispatchThrottlingRateInMsg" : 1000,
  "dispatchThrottlingRateInByte" : 1048576,
  "relativeToPublishRate" : false,
  "ratePeriodInSecond" : 1
}
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/namespaces/:tenant/:namespace/subscriptionDispatchRate|operation/getDispatchRate?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.namespaces().getSubscriptionDispatchRate(namespace)
```

</TabItem>

</Tabs>
````

### Configure dispatch throttling for replicators

#### Set dispatch throttling for replicators

It sets the message dispatch rate for all the replicators between replication clusters under a given namespace. The dispatch rate can be restricted by the number of messages per X seconds (`msg-dispatch-rate`) or by the number of message-bytes per X second (`byte-dispatch-rate`). The dispatch rate is in second and it can be configured with `dispatch-rate-period`. The default value of `msg-dispatch-rate` and `byte-dispatch-rate` is -1 which disables the throttling.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin namespaces set-replicator-dispatch-rate test-tenant/namespace1 \
    --msg-dispatch-rate 1000 \
    --byte-dispatch-rate 1048576 \
    --dispatch-rate-period 1
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/replicatorDispatchRate|operation/setDispatchRate?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.namespaces().setReplicatorDispatchRate(namespace, new DispatchRate(1000, 1048576, 1))
```

</TabItem>

</Tabs>
````

#### Get configured message-rate for replicators

It shows the configured message-rate for the namespace (topics under this namespace can dispatch this many messages per second)

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin namespaces get-replicator-dispatch-rate test-tenant/namespace1
```

```json
{
  "dispatchThrottlingRatePerTopicInMsg" : 1000,
  "dispatchThrottlingRatePerTopicInByte" : 1048576,
  "ratePeriodInSecond" : 1
}
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/namespaces/:tenant/:namespace/replicatorDispatchRate|operation/getDispatchRate?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.namespaces().getReplicatorDispatchRate(namespace)
```

</TabItem>

</Tabs>
````

### Configure deduplication snapshot interval

#### Get deduplication snapshot interval

It shows the configured `deduplicationSnapshotInterval` for a namespace (Each topic under the namespace will take a deduplication snapshot according to this interval)

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin namespaces get-deduplication-snapshot-interval test-tenant/namespace1
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/namespaces/:tenant/:namespace/deduplicationSnapshotInterval|operation/getDeduplicationSnapshotInterval?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.namespaces().getDeduplicationSnapshotInterval(namespace)
```

</TabItem>

</Tabs>
````

#### Set deduplication snapshot interval

Set the configured `deduplicationSnapshotInterval` for a namespace. Each topic under the namespace will take a deduplication snapshot according to this interval. `brokerDeduplicationEnabled` must be set to `true` for this property to take effect.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin namespaces set-deduplication-snapshot-interval test-tenant/namespace1 --interval 1000
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/deduplicationSnapshotInterval|operation/setDeduplicationSnapshotInterval?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.namespaces().setDeduplicationSnapshotInterval(namespace, 1000)
```

</TabItem>

</Tabs>
````

#### Remove deduplication snapshot interval

Remove configured `deduplicationSnapshotInterval` of a namespace (Each topic under the namespace will take a deduplication snapshot according to this interval).

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin namespaces remove-deduplication-snapshot-interval test-tenant/namespace1
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|DELETE|/admin/v2/namespaces/:tenant/:namespace/deduplicationSnapshotInterval|operation/deleteDeduplicationSnapshotInterval?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.namespaces().removeDeduplicationSnapshotInterval(namespace)
```

</TabItem>

</Tabs>
````

### Namespace isolation

You can use the [Pulsar isolation policy](administration-isolation.md) to allocate resources (broker and bookie) for a namespace. 

### Unload namespaces from a broker

You can unload a namespace, or a [namespace bundle](reference-terminology.md#namespace-bundle), from the Pulsar [broker](reference-terminology.md#broker) that is currently responsible for it.

#### pulsar-admin

Use the [`unload`](/tools/pulsar-admin/) subcommand of the [`namespaces`](/tools/pulsar-admin/) command.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin namespaces unload my-tenant/my-ns
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|PUT|/admin/v2/namespaces/:tenant/:namespace/unload|operation/unloadNamespace?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.namespaces().unload(namespace)
```

</TabItem>

</Tabs>
````
### Configure entry filters policy

#### Set entry filters policy

Entry filter helps with filtering message on server side.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```
pulsar-admin namespaces set-entry-filters \
    --desc "The description of the entry filter to be used for user help." \
    --entry-filters-name "The class name for the entry filter." \
    --entry-filters-dir "The directory for all the entry filter implementations." \
    test-tenant/namespace1
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/entryFilters|operation/setEntryFilters?
version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.namespaces().setEntryFilters(namespace, new EntryFilters("desc", "classes name", "class files localtion"))
```

</TabItem>

</Tabs>
````

#### Get entry filters policies

You can get a configured entry filter for a given namespace.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin namespaces get-entry-filters test-tenant/namespace1
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/namespaces/:tenant/:namespace/entryFilters|operation/getEntryFilters?
version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.namespaces().getEntryFilters(namespace);
```

</TabItem>

</Tabs>
````

#### Remove entry filters policies

You can remove entry filters policies for a given namespace.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin namespaces remove-entry-filters test-tenant/namespace1
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|DELETE|/admin/v2/namespaces/:tenant/:namespace/entryFilters|operation/removeEntryFilters?
version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.namespaces().removeEntryFilters(namespace)
```

</TabItem>

</Tabs>
````
