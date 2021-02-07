---
id: version-2.1.0-incubating-admin-api-namespaces
title: Managing Namespaces
sidebar_label: Namespaces
original_id: admin-api-namespaces
---

Pulsar [namespaces](reference-terminology.md#namespace) are logical groupings of [topics](reference-terminology.md#topic).

Namespaces can be managed via:

* The [`namespaces`](reference-pulsar-admin.md#clusters) command of the [`pulsar-admin`](reference-pulsar-admin.md) tool
* The `/admin/v2/namespaces` endpoint of the admin {@inject: rest:REST:/} API
* The `namespaces` method of the {@inject: javadoc:PulsarAdmin:/admin/org/apache/pulsar/client/admin/PulsarAdmin} object in the [Java API](client-libraries-java.md)

## Namespaces resources

### Create

You can create new namespaces under a given [tenant](reference-terminology.md#tenant).

#### pulsar-admin

Use the [`create`](reference-pulsar-admin.md#namespaces-create) subcommand and specify the namespace by name:

```shell
$ pulsar-admin namespaces create test-tenant/test-namespace
```

#### REST API

{@inject: endpoint|PUT|/admin/v2/namespaces/:tenant/:namespace|operation/createNamespace}

#### Java

```java
admin.namespaces().createNamespace(namespace);
```

### Get policies

You can fetch the current policies associated with a namespace at any time.

#### pulsar-admin

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

#### REST API

{@inject: endpoint|GET|/admin/v2/namespaces/:tenant/:namespace|operation/getPolicies}

#### Java

```java
admin.namespaces().getPolicies(namespace);
```

### List namespaces within a tenant

You can list all namespaces within a given Pulsar [tenant](reference-terminology.md#tenant).

#### pulsar-admin

Use the [`list`](reference-pulsar-admin.md#namespaces-list) subcommand and specify the tenant:

```shell
$ pulsar-admin namespaces list test-tenant
test-tenant/ns1
test-tenant/ns2
```

#### REST API

{@inject: endpoint|GET|/admin/v2/namespaces/:tenant|operation/getTenantNamespaces}

#### Java

```java
admin.namespaces().getNamespaces(tenant);
```


#### Java

```java
admin.namespaces().getNamespaces(tenant);
```

### Delete

You can delete existing namespaces from a tenant.

#### pulsar-admin

Use the [`delete`](reference-pulsar-admin.md#namespaces-delete) subcommand and specify the namespace:

```shell
$ pulsar-admin namespaces delete test-tenant/ns1
```

#### REST

{@inject: endpoint|DELETE|/admin/v2/namespaces/:tenant/:namespace|operation/deleteNamespace}

#### Java

```java
admin.namespaces().deleteNamespace(namespace);
```


#### set replication cluster

It sets replication clusters for a namespace, so Pulsar can internally replicate publish message from one colo to another colo.

###### CLI

```
$ pulsar-admin namespaces set-clusters test-tenant/ns1 \
  --clusters cl1
```

###### REST

```
{@inject: endpoint POST|/admin/v2/namespaces/:tenant/:namespace/replication|operation/setNamespaceReplicationClusters}
```

###### Java

```java
admin.namespaces().setNamespaceReplicationClusters(namespace, clusters);
```

#### get replication cluster

It gives a list of replication clusters for a given namespace.

###### CLI

```
$ pulsar-admin namespaces get-clusters test-tenant/cl1/ns1
```

```
cl2
```

###### REST

```
GET /admin/v2/namespaces/:tenant/:namespace/replication
```

###### Java

```java
admin.namespaces().getNamespaceReplicationClusters(namespace)
```

#### set backlog quota policies

Backlog quota helps broker to restrict bandwidth/storage of a namespace once it reach certain threshold limit . Admin can set this limit and one of the following action after the limit is reached.

  1.  producer_request_hold: broker will hold and not persist produce request payload

  2.  producer_exception: broker will disconnects with client by giving exception

  3.  consumer_backlog_eviction: broker will start discarding backlog messages

  Backlog quota restriction can be taken care by defining restriction of backlog-quota-type: destination_storage

###### CLI

```
$ pulsar-admin namespaces set-backlog-quota --limit 10 --policy producer_request_hold test-tenant/ns1
```

```
N/A
```

###### REST

```
POST /admin/v2/namespaces/:tenant/:namespace/backlogQuota
```

###### Java

```java
admin.namespaces().setBacklogQuota(namespace, new BacklogQuota(limit, policy))
```

#### get backlog quota policies

It shows a configured backlog quota for a given namespace.

###### CLI

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

###### REST

```
GET /admin/v2/namespaces/:tenant/:namespace/backlogQuotaMap
```

###### Java

```java
admin.namespaces().getBacklogQuotaMap(namespace);
```

#### remove backlog quota policies

It removes backlog quota policies for a given namespace

###### CLI

```
$ pulsar-admin namespaces remove-backlog-quota test-tenant/ns1
```

```
N/A
```

###### REST

```
DELETE /admin/v2/namespaces/:tenant/:namespace/backlogQuota
```

###### Java

```java
admin.namespaces().removeBacklogQuota(namespace, backlogQuotaType)
```

#### set persistence policies

Persistence policies allow to configure persistency-level for all topic messages under a given namespace.

  -   Bookkeeper-ack-quorum: Number of acks (guaranteed copies) to wait for each entry, default: 0

  -   Bookkeeper-ensemble: Number of bookies to use for a topic, default: 0

  -   Bookkeeper-write-quorum: How many writes to make of each entry, default: 0

  -   Ml-mark-delete-max-rate: Throttling rate of mark-delete operation (0 means no throttle), default: 0.0

###### CLI

```
$ pulsar-admin namespaces set-persistence --bookkeeper-ack-quorum 2 --bookkeeper-ensemble 3 --bookkeeper-write-quorum 2 --ml-mark-delete-max-rate 0 test-tenant/ns1
```

```
N/A
```

###### REST

```
POST /admin/v2/namespaces/:tenant/:namespace/persistence
```

###### Java

```java
admin.namespaces().setPersistence(namespace,new PersistencePolicies(bookkeeperEnsemble, bookkeeperWriteQuorum,bookkeeperAckQuorum,managedLedgerMaxMarkDeleteRate))
```


#### get persistence policies

It shows configured persistence policies of a given namespace.

###### CLI

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

###### REST

```
GET /admin/v2/namespaces/:tenant/:namespace/persistence
```

###### Java

```java
admin.namespaces().getPersistence(namespace)
```


#### unload namespace bundle

Namespace bundle is a virtual group of topics which belong to same namespace. If broker gets overloaded with number of bundles then this command can help to unload heavy bundle from that broker, so it can be served by some other less loaded broker. Namespace bundle is defined with it’s start and end range such as 0x00000000 and 0xffffffff.

###### CLI

```
$ pulsar-admin namespaces unload --bundle 0x00000000_0xffffffff test-tenant/ns1
```

```
N/A
```

###### REST

```
PUT /admin/v2/namespaces/:tenant/:namespace/unload
```

###### Java

```java
admin.namespaces().unloadNamespaceBundle(namespace, bundle)
```


#### set message-ttl

It configures message’s time to live (in seconds) duration.

###### CLI

```
$ pulsar-admin namespaces set-message-ttl --messageTTL 100 test-tenant/ns1
```

```
N/A
```

###### REST

```
POST /admin/v2/namespaces/:tenant/:namespace/messageTTL
```

###### Java

```java
admin.namespaces().setNamespaceMessageTTL(namespace, messageTTL)
```

#### get message-ttl

It gives a message ttl of configured namespace.

###### CLI

```
$ pulsar-admin namespaces get-message-ttl test-tenant/ns1
```

```
100
```


###### REST

```
GET /admin/v2/namespaces/:tenant/:namespace/messageTTL
```

###### Java

```java
admin.namespaces().getNamespaceReplicationClusters(namespace)
```


#### split bundle

Each namespace bundle can contain multiple topics and each bundle can be served by only one broker. If bundle gets heavy with multiple live topics in it then it creates load on that broker and in order to resolve this issue, admin can split bundle using this command.

###### CLI

```
$ pulsar-admin namespaces split-bundle --bundle 0x00000000_0xffffffff test-tenant/ns1
```

```
N/A
```

###### REST

```
PUT /admin/v2/namespaces/:tenant/:namespace/{bundle}/split
```

###### Java

```java
admin.namespaces().splitNamespaceBundle(namespace, bundle)
```


#### clear backlog

It clears all message backlog for all the topics those belong to specific namespace. You can also clear backlog for a specific subscription as well.

###### CLI

```
$ pulsar-admin namespaces clear-backlog --sub my-subscription test-tenant/ns1
```

```
N/A
```

###### REST

```
POST /admin/v2/namespaces/:tenant/:namespace/clearBacklog
```

###### Java

```java
admin.namespaces().clearNamespaceBacklogForSubscription(namespace, subscription)
```


#### clear bundle backlog

It clears all message backlog for all the topics those belong to specific NamespaceBundle. You can also clear backlog for a specific subscription as well.

###### CLI

```
$ pulsar-admin namespaces clear-backlog  --bundle 0x00000000_0xffffffff  --sub my-subscription test-tenant/ns1
```

```
N/A
```

###### REST

```
POST /admin/v2/namespaces/:tenant/:namespace/{bundle}/clearBacklog
```

###### Java

```java
admin.namespaces().clearNamespaceBundleBacklogForSubscription(namespace, bundle, subscription)
```


#### set retention

Each namespace contains multiple topics and each topic’s retention size (storage size) should not exceed to a specific threshold or it should be stored till certain time duration. This command helps to configure retention size and time of topics in a given namespace.

###### CLI

```
$ pulsar-admin set-retention --size 10 --time 100 test-tenant/ns1
```

```
N/A
```

###### REST

```
POST /admin/v2/namespaces/:tenant/:namespace/retention
```

###### Java

```java
admin.namespaces().setRetention(namespace, new RetentionPolicies(retentionTimeInMin, retentionSizeInMB))
```


#### get retention

It shows retention information of a given namespace.

###### CLI

```
$ pulsar-admin namespaces get-retention test-tenant/ns1
```

```json
{
  "retentionTimeInMinutes": 10,
  "retentionSizeInMB": 100
}
```

###### REST

```
GET /admin/v2/namespaces/:tenant/:namespace/retention
```

###### Java

```java
admin.namespaces().getRetention(namespace)
```

#### set dispatch throttling

It sets message dispatch rate for all the topics under a given namespace.
Dispatch rate can be restricted by number of message per X seconds (`msg-dispatch-rate`) or by number of message-bytes per X second (`byte-dispatch-rate`).
dispatch rate is in second and it can be configured with `dispatch-rate-period`. Default value of `msg-dispatch-rate` and `byte-dispatch-rate` is -1 which
disables the throttling.

###### CLI

```
$ pulsar-admin namespaces set-dispatch-rate test-tenant/ns1 \
  --msg-dispatch-rate 1000 \
  --byte-dispatch-rate 1048576 \
  --dispatch-rate-period 1
```

###### REST

```
POST /admin/v2/namespaces/:tenant/:namespace/dispatchRate
```

###### Java

```java
admin.namespaces().setDispatchRate(namespace, 1000, 1048576, 1)
```

#### get configured message-rate

It shows configured message-rate for the namespace (topics under this namespace can dispatch this many messages per second)

###### CLI

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

###### REST

```
GET /admin/v2/namespaces/:tenant/:namespace/dispatchRate
```

###### Java

```java
admin.namespaces().getDispatchRate(namespace)
```


### Namespace isolation

Coming soon.

### Unloading from a broker

You can unload a namespace, or a [namespace bundle](reference-terminology.md#namespace-bundle), from the Pulsar [broker](reference-terminology.md#broker) that is currently responsible for it.

#### pulsar-admin

Use the [`unload`](reference-pulsar-admin.md#namespaces-unload) subcommand of the [`namespaces`](reference-pulsar-admin.md#namespaces) command.

##### Example

```shell
$ pulsar-admin namespaces unload my-tenant/my-ns
```

#### REST API

#### Java

