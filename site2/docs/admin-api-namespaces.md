---
id: admin-api-namespaces
title: Managing Namespaces
sidebar_label: Namespaces
---

Pulsar [namespaces](reference-terminology.md#namespace) are logical groupings of [topics](reference-terminology.md#topic).

Namespaces can be managed via:

* The [`namespaces`](reference-pulsar-admin.md#clusters) command of the [`pulsar-admin`](reference-pulsar-admin.md) tool
* The `/admin/v2/namespaces` endpoint of the admin {@inject: rest:REST:/} API
* The `namespaces` method of the {@inject: javadoc:PulsarAdmin:/admin/org/apache/pulsar/client/admin/PulsarAdmin} object in the [Java API](client-libraries-java.md)

## Namespaces resources

### Create namespaces

You can create new namespaces under a given [tenant](reference-terminology.md#tenant).

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

Use the [`create`](reference-pulsar-admin.md#namespaces-create) subcommand and specify the namespace by name:

```shell
$ pulsar-admin namespaces create test-tenant/test-namespace
```

<!--REST API-->

{@inject: endpoint|PUT|/admin/v2/namespaces/:tenant/:namespace|operation/createNamespace?version=[[pulsar:version_number]]}

<!--Java-->

```java
admin.namespaces().createNamespace(namespace);
```
<!--END_DOCUSAURUS_CODE_TABS-->

### Get policies

You can fetch the current policies associated with a namespace at any time.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

Use the [`policies`](reference-pulsar-admin.md#namespaces-policies) subcommand and specify the namespace:

```shell
$ pulsar-admin namespaces policies test-tenant/test-namespace
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

<!--REST API-->

{@inject: endpoint|GET|/admin/v2/namespaces/:tenant/:namespace|operation/getPolicies?version=[[pulsar:version_number]]}

<!--Java-->

```java
admin.namespaces().getPolicies(namespace);
```
<!--END_DOCUSAURUS_CODE_TABS-->

### List namespaces

You can list all namespaces within a given Pulsar [tenant](reference-terminology.md#tenant).

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

Use the [`list`](reference-pulsar-admin.md#namespaces-list) subcommand and specify the tenant:

```shell
$ pulsar-admin namespaces list test-tenant
test-tenant/ns1
test-tenant/ns2
```

<!--REST API-->

{@inject: endpoint|GET|/admin/v2/namespaces/:tenant|operation/getTenantNamespaces?version=[[pulsar:version_number]]}

<!--Java-->

```java
admin.namespaces().getNamespaces(tenant);
```
<!--END_DOCUSAURUS_CODE_TABS-->

### Delete namespaces

You can delete existing namespaces from a tenant.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

Use the [`delete`](reference-pulsar-admin.md#namespaces-delete) subcommand and specify the namespace:

```shell
$ pulsar-admin namespaces delete test-tenant/ns1
```

<!--REST API-->

{@inject: endpoint|DELETE|/admin/v2/namespaces/:tenant/:namespace|operation/deleteNamespace?version=[[pulsar:version_number]]}

<!--Java-->

```java
admin.namespaces().deleteNamespace(namespace);
```
<!--END_DOCUSAURUS_CODE_TABS-->

### Configure replication clusters

#### Set replication cluster

It sets replication clusters for a namespace, so Pulsar can internally replicate publish message from one colo to another colo.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

```
$ pulsar-admin namespaces set-clusters test-tenant/ns1 \
  --clusters cl1
```

<!--REST API-->

```
{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/replication|operation/setNamespaceReplicationClusters?version=[[pulsar:version_number]]}
```

<!--Java-->

```java
admin.namespaces().setNamespaceReplicationClusters(namespace, clusters);
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Get replication cluster

It gives a list of replication clusters for a given namespace.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

```
$ pulsar-admin namespaces get-clusters test-tenant/cl1/ns1
```

```
cl2
```

<!--REST API-->

```
{@inject: endpoint|GET|/admin/v2/namespaces/:tenant/:namespace/replication|operation/getNamespaceReplicationClusters?version=[[pulsar:version_number]]}
```

<!--Java-->

```java
admin.namespaces().getNamespaceReplicationClusters(namespace)
```
<!--END_DOCUSAURUS_CODE_TABS-->

### Configure backlog quota policies

#### Set backlog quota policies

Backlog quota helps the broker to restrict bandwidth/storage of a namespace once it reaches a certain threshold limit. Admin can set the limit and take corresponding action after the limit is reached.

  1.  producer_request_hold: broker will hold and not persist produce request payload

  2.  producer_exception: broker disconnects with the client by giving an exception.

  3.  consumer_backlog_eviction: broker will start discarding backlog messages

  Backlog quota restriction can be taken care by defining restriction of backlog-quota-type: destination_storage

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

```
$ pulsar-admin namespaces set-backlog-quota --limit 10G --limitTime 36000 --policy producer_request_hold test-tenant/ns1
```

```
N/A
```

<!--REST API-->

```
{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/backlogQuota|operation/setBacklogQuota?version=[[pulsar:version_number]]}
```

<!--Java-->

```java
admin.namespaces().setBacklogQuota(namespace, new BacklogQuota(limit, limitTime, policy))
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Get backlog quota policies

It shows a configured backlog quota for a given namespace.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

```
$ pulsar-admin namespaces get-backlog-quotas test-tenant/ns1
```

```json
{
  "destination_storage": {
    "limit": 10,
    "policy": "producer_request_hold"
  }
}
```

<!--REST API-->

```
{@inject: endpoint|GET|/admin/v2/namespaces/:tenant/:namespace/backlogQuotaMap|operation/getBacklogQuotaMap?version=[[pulsar:version_number]]}
```

<!--Java-->

```java
admin.namespaces().getBacklogQuotaMap(namespace);
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Remove backlog quota policies

It removes backlog quota policies for a given namespace

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

```
$ pulsar-admin namespaces remove-backlog-quota test-tenant/ns1
```

```
N/A
```

<!--REST API-->

```
{@inject: endpoint|DELETE|/admin/v2/namespaces/:tenant/:namespace/backlogQuota|operation/removeBacklogQuota?version=[[pulsar:version_number]]}
```

<!--Java-->

```java
admin.namespaces().removeBacklogQuota(namespace, backlogQuotaType)
```
<!--END_DOCUSAURUS_CODE_TABS-->

### Configure persistence policies

#### Set persistence policies

Persistence policies allow to configure persistency-level for all topic messages under a given namespace.

  -   Bookkeeper-ack-quorum: Number of acks (guaranteed copies) to wait for each entry, default: 0

  -   Bookkeeper-ensemble: Number of bookies to use for a topic, default: 0

  -   Bookkeeper-write-quorum: How many writes to make of each entry, default: 0

  -   Ml-mark-delete-max-rate: Throttling rate of mark-delete operation (0 means no throttle), default: 0.0

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

```
$ pulsar-admin namespaces set-persistence --bookkeeper-ack-quorum 2 --bookkeeper-ensemble 3 --bookkeeper-write-quorum 2 --ml-mark-delete-max-rate 0 test-tenant/ns1
```

```
N/A
```

<!--REST API-->

```
{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/persistence|operation/setPersistence?version=[[pulsar:version_number]]}
```

<!--Java-->

```java
admin.namespaces().setPersistence(namespace,new PersistencePolicies(bookkeeperEnsemble, bookkeeperWriteQuorum,bookkeeperAckQuorum,managedLedgerMaxMarkDeleteRate))
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Get persistence policies

It shows the configured persistence policies of a given namespace.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

```
$ pulsar-admin namespaces get-persistence test-tenant/ns1
```

```json
{
  "bookkeeperEnsemble": 3,
  "bookkeeperWriteQuorum": 2,
  "bookkeeperAckQuorum": 2,
  "managedLedgerMaxMarkDeleteRate": 0
}
```

<!--REST API-->

```
{@inject: endpoint|GET|/admin/v2/namespaces/:tenant/:namespace/persistence|operation/getPersistence?version=[[pulsar:version_number]]}
```

<!--Java-->

```java
admin.namespaces().getPersistence(namespace)
```
<!--END_DOCUSAURUS_CODE_TABS-->

### Configure namespace bundles

#### Unload namespace bundles

The namespace bundle is a virtual group of topics which belong to the same namespace. If the broker gets overloaded with the number of bundles, this command can help unload a bundle from that broker, so it can be served by some other less-loaded brokers. The namespace bundle ID ranges from 0x00000000 to 0xffffffff.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

```
$ pulsar-admin namespaces unload --bundle 0x00000000_0xffffffff test-tenant/ns1
```

```
N/A
```

<!--REST API-->

```
{@inject: endpoint|PUT|/admin/v2/namespaces/:tenant/:namespace/{bundle}/unload|operation/unloadNamespaceBundle?version=[[pulsar:version_number]]}
```

<!--Java-->

```java
admin.namespaces().unloadNamespaceBundle(namespace, bundle)
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Split namespace bundles

Each namespace bundle can contain multiple topics and each bundle can be served by only one broker. 
If a single bundle is creating an excessive load on a broker, an admin splits the bundle using this command permitting one or more of the new bundles to be unloaded thus spreading the load across the brokers.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

```
$ pulsar-admin namespaces split-bundle --bundle 0x00000000_0xffffffff test-tenant/ns1
```

```
N/A
```

<!--REST API-->

```
{@inject: endpoint|PUT|/admin/v2/namespaces/:tenant/:namespace/{bundle}/split|operation/splitNamespaceBundle?version=[[pulsar:version_number]]}
```

<!--Java-->

```java
admin.namespaces().splitNamespaceBundle(namespace, bundle)
```
<!--END_DOCUSAURUS_CODE_TABS-->

### Configure message TTL

#### Set message-ttl

It configures message’s time to live (in seconds) duration.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

```
$ pulsar-admin namespaces set-message-ttl --messageTTL 100 test-tenant/ns1
```

```
N/A
```

<!--REST API-->

```
{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/messageTTL|operation/setNamespaceMessageTTL?version=[[pulsar:version_number]]}
```

<!--Java-->

```java
admin.namespaces().setNamespaceMessageTTL(namespace, messageTTL)
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Get message-ttl

It gives a message ttl of configured namespace.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

```
$ pulsar-admin namespaces get-message-ttl test-tenant/ns1
```

```
100
```

<!--REST API-->

```
{@inject: endpoint|GET|/admin/v2/namespaces/:tenant/:namespace/messageTTL|operation/getNamespaceMessageTTL?version=[[pulsar:version_number]]}
```

<!--Java-->

```java
admin.namespaces().getNamespaceMessageTTL(namespace)
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Remove message-ttl

Remove a message TTL of the configured namespace.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

```
$ pulsar-admin namespaces remove-message-ttl test-tenant/ns1
```

```
100
```

<!--REST API-->

```
{@inject: endpoint|DELETE|/admin/v2/namespaces/:tenant/:namespace/messageTTL|operation/removeNamespaceMessageTTL?version=[[pulsar:version_number]]}
```

<!--Java-->

```java
admin.namespaces().removeNamespaceMessageTTL(namespace)
```
<!--END_DOCUSAURUS_CODE_TABS-->


### Clear backlog

#### Clear namespace backlog

It clears all message backlog for all the topics that belong to a specific namespace. You can also clear backlog for a specific subscription as well.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

```
$ pulsar-admin namespaces clear-backlog --sub my-subscription test-tenant/ns1
```

```
N/A
```

<!--REST API-->

```
{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/clearBacklog|operation/clearNamespaceBacklogForSubscription?version=[[pulsar:version_number]]}
```

<!--Java-->

```java
admin.namespaces().clearNamespaceBacklogForSubscription(namespace, subscription)
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Clear bundle backlog

It clears all message backlog for all the topics that belong to a specific NamespaceBundle. You can also clear backlog for a specific subscription as well.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

```
$ pulsar-admin namespaces clear-backlog  --bundle 0x00000000_0xffffffff  --sub my-subscription test-tenant/ns1
```

```
N/A
```

<!--REST API-->

```
{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/{bundle}/clearBacklog|operation?version=[[pulsar:version_number]]/clearNamespaceBundleBacklogForSubscription}
```

<!--Java-->

```java
admin.namespaces().clearNamespaceBundleBacklogForSubscription(namespace, bundle, subscription)
```
<!--END_DOCUSAURUS_CODE_TABS-->

### Configure retention

#### Set retention

Each namespace contains multiple topics and the retention size (storage size) of each topic should not exceed a specific threshold or it should be stored for a certain period. This command helps configure the retention size and time of topics in a given namespace.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

```
$ pulsar-admin set-retention --size 100 --time 10 test-tenant/ns1
```

```
N/A
```

<!--REST API-->

```
{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/retention|operation/setRetention?version=[[pulsar:version_number]]}
```

<!--Java-->

```java
admin.namespaces().setRetention(namespace, new RetentionPolicies(retentionTimeInMin, retentionSizeInMB))
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Get retention

It shows retention information of a given namespace.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

```
$ pulsar-admin namespaces get-retention test-tenant/ns1
```

```json
{
  "retentionTimeInMinutes": 10,
  "retentionSizeInMB": 100
}
```

<!--REST API-->

```
{@inject: endpoint|GET|/admin/v2/namespaces/:tenant/:namespace/retention|operation/getRetention?version=[[pulsar:version_number]]}
```

<!--Java-->

```java
admin.namespaces().getRetention(namespace)
```
<!--END_DOCUSAURUS_CODE_TABS-->

### Configure dispatch throttling for topics

#### Set dispatch throttling for topics

It sets message dispatch rate for all the topics under a given namespace. 
The dispatch rate can be restricted by the number of messages per X seconds (`msg-dispatch-rate`) or by the number of message-bytes per X second (`byte-dispatch-rate`).
dispatch rate is in second and it can be configured with `dispatch-rate-period`. Default value of `msg-dispatch-rate` and `byte-dispatch-rate` is -1 which
disables the throttling.

> **Note**
> - If neither `clusterDispatchRate` nor `topicDispatchRate` is configured, dispatch throttling is disabled.
> >
> - If `topicDispatchRate` is not configured, `clusterDispatchRate` takes effect.
> > 
> - If `topicDispatchRate` is configured, `topicDispatchRate` takes effect.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

```
$ pulsar-admin namespaces set-dispatch-rate test-tenant/ns1 \
  --msg-dispatch-rate 1000 \
  --byte-dispatch-rate 1048576 \
  --dispatch-rate-period 1
```

<!--REST API-->

```
{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/dispatchRate|operation/setDispatchRate?version=[[pulsar:version_number]]}
```

<!--Java-->

```java
admin.namespaces().setDispatchRate(namespace, new DispatchRate(1000, 1048576, 1))
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Get configured message-rate for topics

It shows configured message-rate for the namespace (topics under this namespace can dispatch this many messages per second)

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

```
$ pulsar-admin namespaces get-dispatch-rate test-tenant/ns1
```

```json
{
  "dispatchThrottlingRatePerTopicInMsg" : 1000,
  "dispatchThrottlingRatePerTopicInByte" : 1048576,
  "ratePeriodInSecond" : 1
}
```

<!--REST API-->

```
{@inject: endpoint|GET|/admin/v2/namespaces/:tenant/:namespace/dispatchRate|operation/getDispatchRate?version=[[pulsar:version_number]]}
```

<!--Java-->

```java
admin.namespaces().getDispatchRate(namespace)
```
<!--END_DOCUSAURUS_CODE_TABS-->

### Configure dispatch throttling for subscription

#### Set dispatch throttling for subscription

It sets message dispatch rate for all the subscription of topics under a given namespace.
The dispatch rate can be restricted by the number of messages per X seconds (`msg-dispatch-rate`) or by the number of message-bytes per X second (`byte-dispatch-rate`).
dispatch rate is in second and it can be configured with `dispatch-rate-period`. Default value of `msg-dispatch-rate` and `byte-dispatch-rate` is -1 which
disables the throttling.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

```
$ pulsar-admin namespaces set-subscription-dispatch-rate test-tenant/ns1 \
  --msg-dispatch-rate 1000 \
  --byte-dispatch-rate 1048576 \
  --dispatch-rate-period 1
```

<!--REST API-->

```
{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/subscriptionDispatchRate|operation/setDispatchRate?version=[[pulsar:version_number]]}
```

<!--Java-->

```java
admin.namespaces().setSubscriptionDispatchRate(namespace, new DispatchRate(1000, 1048576, 1))
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Get configured message-rate for subscription

It shows configured message-rate for the namespace (topics under this namespace can dispatch this many messages per second)

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

```
$ pulsar-admin namespaces get-subscription-dispatch-rate test-tenant/ns1
```

```json
{
  "dispatchThrottlingRatePerTopicInMsg" : 1000,
  "dispatchThrottlingRatePerTopicInByte" : 1048576,
  "ratePeriodInSecond" : 1
}
```

<!--REST API-->

```
{@inject: endpoint|GET|/admin/v2/namespaces/:tenant/:namespace/subscriptionDispatchRate|operation/getDispatchRate?version=[[pulsar:version_number]]}
```

<!--Java-->

```java
admin.namespaces().getSubscriptionDispatchRate(namespace)
```
<!--END_DOCUSAURUS_CODE_TABS-->

### Configure dispatch throttling for replicator

#### Set dispatch throttling for replicator

It sets message dispatch rate for all the replicator between replication clusters under a given namespace.
The dispatch rate can be restricted by the number of messages per X seconds (`msg-dispatch-rate`) or by the number of message-bytes per X second (`byte-dispatch-rate`).
dispatch rate is in second and it can be configured with `dispatch-rate-period`. Default value of `msg-dispatch-rate` and `byte-dispatch-rate` is -1 which
disables the throttling.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

```
$ pulsar-admin namespaces set-replicator-dispatch-rate test-tenant/ns1 \
  --msg-dispatch-rate 1000 \
  --byte-dispatch-rate 1048576 \
  --dispatch-rate-period 1
```

<!--REST API-->

```
{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/replicatorDispatchRate|operation/setDispatchRate?version=[[pulsar:version_number]]}
```

<!--Java-->

```java
admin.namespaces().setReplicatorDispatchRate(namespace, new DispatchRate(1000, 1048576, 1))
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Get configured message-rate for replicator

It shows configured message-rate for the namespace (topics under this namespace can dispatch this many messages per second)

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

```
$ pulsar-admin namespaces get-replicator-dispatch-rate test-tenant/ns1
```

```json
{
  "dispatchThrottlingRatePerTopicInMsg" : 1000,
  "dispatchThrottlingRatePerTopicInByte" : 1048576,
  "ratePeriodInSecond" : 1
}
```

<!--REST API-->

```
{@inject: endpoint|GET|/admin/v2/namespaces/:tenant/:namespace/replicatorDispatchRate|operation/getDispatchRate?version=[[pulsar:version_number]]}
```

<!--Java-->

```java
admin.namespaces().getReplicatorDispatchRate(namespace)
```
<!--END_DOCUSAURUS_CODE_TABS-->

### Configure deduplication snapshot interval

#### Get deduplication snapshot interval

It shows configured `deduplicationSnapshotInterval` for a namespace (Each topic under the namespace will take a deduplication snapshot according to this interval)

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

```
$ pulsar-admin namespaces get-deduplication-snapshot-interval test-tenant/ns1
```

<!--REST API-->

```
{@inject: endpoint|GET|/admin/v2/namespaces/:tenant/:namespace/deduplicationSnapshotInterval?version=[[pulsar:version_number]]}
```

<!--Java-->

```java
admin.namespaces().getDeduplicationSnapshotInterval(namespace)
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Set deduplication snapshot interval

Set configured `deduplicationSnapshotInterval` for a namespace. Each topic under the namespace will take a deduplication snapshot according to this interval.
`brokerDeduplicationEnabled` must be set to `true` for this property to take effect.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

```
$ pulsar-admin namespaces set-deduplication-snapshot-interval test-tenant/ns1 --interval 1000 
```

<!--REST API-->

```
{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/deduplicationSnapshotInterval?version=[[pulsar:version_number]]}
```

```json
{
  "interval": 1000
}
```

<!--Java-->

```java
admin.namespaces().setDeduplicationSnapshotInterval(namespace, 1000)
```
<!--END_DOCUSAURUS_CODE_TABS-->

#### Remove deduplication snapshot interval

Remove configured `deduplicationSnapshotInterval` of a namespace (Each topic under the namespace will take a deduplication snapshot according to this interval)

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

```
$ pulsar-admin namespaces remove-deduplication-snapshot-interval test-tenant/ns1
```

<!--REST API-->

```
{@inject: endpoint|POST|/admin/v2/namespaces/:tenant/:namespace/deduplicationSnapshotInterval?version=[[pulsar:version_number]]}
```

<!--Java-->

```java
admin.namespaces().removeDeduplicationSnapshotInterval(namespace)
```
<!--END_DOCUSAURUS_CODE_TABS-->

### Namespace isolation

Coming soon.

### Unload namespaces from a broker

You can unload a namespace, or a [namespace bundle](reference-terminology.md#namespace-bundle), from the Pulsar [broker](reference-terminology.md#broker) that is currently responsible for it.

#### pulsar-admin

Use the [`unload`](reference-pulsar-admin.md#unload) subcommand of the [`namespaces`](reference-pulsar-admin.md#namespaces) command.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

```shell
$ pulsar-admin namespaces unload my-tenant/my-ns
```

<!--REST-->

```
{@inject: endpoint|PUT|/admin/v2/namespaces/:tenant/:namespace/unload|operation/unloadNamespace?version=[[pulsar:version_number]]}
```

<!--Java-->

```java
admin.namespaces().unload(namespace)
```
<!--END_DOCUSAURUS_CODE_TABS-->
