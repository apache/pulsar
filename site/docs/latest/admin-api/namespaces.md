---
title: Managing namespaces
---

Pulsar {% popover namespaces %} are logical groupings of {% popover topics %}.

Namespaces can be managed via:

* The [`namespaces`](../../reference/CliTools#pulsar-admin-clusters) command of the [`pulsar-admin`](../../reference/CliTools#pulsar-admin) tool
* The `/admin/namespaces` endpoint of the admin [REST API](../../reference/RestApi)
* The `namespaces` method of the {% javadoc PulsarAdmin admin org.apache.pulsar.client.admin.PulsarAdmin %} object in the [Java API](../../applications/JavaClient)

## Namespaces resources

### Create

You can create new namespaces under a given {% popover property %} and within a Pulsar {% popover cluster %}.

#### pulsar-admin

Use the [`create`](../../reference/CliTools#pulsar-admin-namespaces-create) subcommand and specify the namespace by name:

```shell
$ pulsar-admin namespaces create test-property/cl1/ns1
```

#### REST API

{% endpoint PUT /admin/namespaces/:property/:cluster/:namespace %}

[More info](../../reference/RestApi#/admin/namespaces/:property/:cluster/:namespace)

#### Java

```java
admin.namespaces().createNamespace(namespace);
```

### Get policies

You can fetch the current policies associated with a namespace at any time.

#### pulsar-admin

Use the [`policies`](../../reference/CliTools#pulsar-admin-namespaces-policies) subcommand and specify the namespace:

```shell
$ pulsar-admin namespaces policies test-property/cl1/ns1
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

{% endpoint GET /admin/namespaces/:property/:cluster/:namespace %}

[More info](../../reference/RestApi#/admin/namespaces/:property/:cluster/:namespace)

#### Java

```java
admin.namespaces().getPolicies(namespace);
```

### List namespaces within a property

You can list all namespaces within a given Pulsar {% popover property %}.

#### pulsar-admin

Use the [`list`](../../reference/CliTools#pulsar-admin-namespaces-list) subcommand and specify the property:

```shell
$ pulsar-admin namespaces list test-property
test-property/cl1/ns1
test-property/cl2/ns2
```

#### REST API

{% endpoint GET /admin/namespaces/:property %}

[More info](../../reference/RestApi#/admin/namespaces/:property)

#### Java

```java
admin.namespaces().getNamespaces(property);
```

### List namespaces within a cluster

You can list all namespaces within a given Pulsar {% popover cluster %}.

#### pulsar-admin

Use the [`list-cluster`](../../reference/CliTools#pulsar-admin-namespaces-list-cluster) subcommand and specify the cluster:

```shell
$ pulsar-admin namespaces list-cluster test-property/cl1
test-property/cl1/ns1
test-property/cl1/ns1
```

#### REST API

{% endpoint GET /admin/namespaces/:property/:cluster %}

[More info](../../reference/RestApi#/admin/namespaces/:property/:cluster)

#### Java

```java
admin.namespaces().getNamespaces(property, cluster);
```

### Delete

You can delete existing namespaces from a property/cluster.

#### pulsar-admin

Use the [`delete`](../../reference/CliTools#pulsar-admin-namespaces-delete) subcommand and specify the namespace:

```shell
$ pulsar-admin namespaces delete test-property/cl1/ns1
```

#### REST

{% endpoint DELETE /admin/namespaces/:property/:cluster/:namespace %}

[More info](../../reference/RestApi#/admin/namespaces/:property/:cluster/:namespace)

#### Java

```java
admin.namespaces().deleteNamespace(namespace);
```
