---
title: Message retention and expiry
lead: Manage how long messages remain stored in your Pulsar instance
tags: [admin, expiry, retention, backlog]
---

By default Pulsar {% popover brokers %} do two things:

* They immediately delete all messages that have been {% popover acknowledged %} by a {% popover consumer %}.
* They persistently store all unacknowledged messages in a [backlog](#backlog-quotas).

Pulsar also enables you to:

* persistently store acknowledged messages by setting [retention policies](#retention-policies)
* delete messages that have not been acknowledged within a specified timeframe using [time to live](#time-to-live-ttl) (TTL)

Pulsar's [admin interface](../../admin/AdminInterface) enables you to manage both retention policies and TTL at the {% popover namespace %} level (and thus within a specific {% popover property %} and on a specific {% popover cluster %}).

{% include admonition.html type="warning" title="Don't use retention and TTL at the same time" content="
Message retention policies and TTL fulfill similar purposes and should not be used in conjunction. For any given namespace, use one or the other.
" %}

## Retention policies

By default, when a Pulsar message arrives at a {% popover broker %} it will be stored until it has been {% popover acknowledged %} by a {% popover consumer %}, at which point it will be deleted. You can override this behavior and retain even messages that have already been acknowledged by setting a *retention policy* on all the topics in a given {% popover namespace %}. When you set a retention policy you can set either a *size limit* or a *time limit*.

When you set a size limit of, say, 10 gigabytes, then messages in all topics in the namespace, *even acknowledged messages*, will be retained until the size limit for the topic is reached; if you set a time limit of, say, 1 day, then messages for all topics in the namespace will be retained for 24 hours.

### Defaults

There are two configuration parameters that you can use to set {% popover instance %}-wide defaults for message retention: [`defaultRetentionTimeInMinutes`](../../reference/Configuration#broker-defaultRetentionTimeInMinutes) and [`defaultRetentionSizeInMB`](../../reference/Configuration#broker-defaultRetentionSizeInMB).

Both of these parameters are in the [`broker.conf`](../../reference/Configuration#broker) configuration file.

### Set retention policy

You can a retention policy for a namespace by specifying the namespace and either a size limit or a time limit *or both a size and time limit*.

#### pulsar-admin

Use the [`set-retention`](../../reference/CliTools#pulsar-admin-namespaces-set-retention) subcommand and specify a namespace and either a size limit using the `-s`/`--size` flag *or* a time limit using the `-t`/`--time` flag.

##### Examples

To set a size limit of 10 gigabytes for the `my-prop/my-cluster/my-ns` namespace:

```shell
$ pulsar-admin namespaces set-retention my-prop/my-cluster/my-ns \
  --size 10G
```

To set a time limit of 3 hours for the `my-prop/my-cluster/my-ns` namespace:

```shell
$ pulsar-admin namespaces set-retention my-prop/my-cluster/my-ns \
  --time 3h
```

To set a size *and* time limit:

```shell
$ pulsar-admin namespaces set-retention my-prop/my-cluster/my-ns \
  --time 3h \
```

#### REST API

{% endpoint POST /admin/namespaces/:property/:cluster/:namespace/retention %}

[More info](../../reference/RestApi#/admin/namespaces/:property/:cluster/:namespace/retention)

#### Java

```java
int retentionTime = 10; // 10 minutes
int retentionSize = 500; // 500 megabytes
RetentionPolicies policies = new RetentionPolicies(retentionTime, retentionSize);
admin.namespaces().setRetention(namespace, policies);
```

### Get retention policy

You can fetch the retention policy for a namespace by specifying the namespace. The output will be a JSON object with two keys: `retentionTimeInMinutes` and `retentionSizeInMB`.

#### pulsar-admin

Use the [`get-retention`](../../reference/CliTools#pulsar-admin-namespaces-get-retention) subcommand and specify the namespace.

##### Example

```shell
$ pulsar-admin namespaces get-retention my-prop/my-cluster/my-ns
{
  "retentionTimeInMinutes": 10,
  "retentionSizeInMB": 0
}
```

#### REST API

{% endpoint GET /admin/namespaces/:property/:cluster/:namespace/retention %}

[More info](../../reference/RestApi#/admin/namespaces/:property/:cluster/:namespace/retention)

#### Java

```java
admin.namespaces().getRetention(namespace);
```

## Backlog quotas

*Backlogs* are sets of unacknowledged messages for a topic that have been stored by {% popover bookies %}. Pulsar stores all unacknowledged messages in backlogs until they are processed and acknowledged.

You can control the allowable size of backlogs, at the {% popover namespace %} level, using *backlog quotas*. Setting a backlog quota involves setting:

* an allowable *size threshold* for each topic in the namespace
* a *retention policy* that determines which action the {% popover broker %} takes if the threshold is exceeded.

The following retention policies are available:

Policy | Action
:------|:------
`producer_request_hold` | The broker will hold and not persist produce request payload
`producer_exception` | The broker will disconnect from the client by throwing an exception
`consumer_backlog_eviction` | The broker will begin discarding backlog messages

{% include admonition.html type="warning" title="Beware the distinction between retention policy types" content='
As you may have noticed, there are two definitions of the term "retention policy" in Pulsar, one that applies to persistent storage of already-acknowledged messages and one that applies to backlogs.
' %}

Backlog quotas are handled at the {% popover namespace %} level. They can be managed via:

### Set size thresholds and backlog retention policies

You can set a size threshold and backlog retention policy for all of the topics in a {% popover namespace %} by specifying the namespace, a size limit, and a policy by name.

#### pulsar-admin

Use the [`set-backlog-quota`](../../reference/CliTools#pulsar-admin-namespaces-set-backlog-quota) subcommand and specify a namespace, a size limit using the `-l`/`--limit` flag, and a retention policy using the `-p`/`--policy` flag.

##### Example

```shell
$ pulsar-admin namespaces set-backlog-quota my-prop/my-cluster/my-ns \
  --limit 2G \
  --policy producer_request_hold
```

#### REST API

{% endpoint POST /admin/namespaces/:property/:cluster/:namespace/backlogQuota %}

[More info](../../reference/RestApi#/admin/namespaces/:property/:cluster/:namespace/backlogQuota)

#### Java

```java
long sizeLimit = 2147483648L;
BacklogQuota.RetentionPolicy policy =
  BacklogQuota.RetentionPolicy.producer_request_hold;
BacklogQuota quota = new BacklogQuota(sizeLimit, policy);
admin.namespaces().setBacklogQuota(namespace, quota);
```

### Get backlog threshold and backlog retention policy

You can see which size threshold and backlog retention policy has been applied to a namespace.

#### pulsar-admin

Use the [`get-backlog-quotas`](../../reference/CliTools#pulsar-admin-namespaces-get-backlog-quotas) subcommand and specify a namespace. Here's an example:

```shell
$ pulsar-admin namespaces get-backlog-quotas my-prop/my-cluster/my-ns
{
  "destination_storage": {
    "limit" : 2147483648,
    "policy" : "producer_request_hold"
  }
}
```

#### REST API

{% endpoint GET /admin/namespaces/:property/:cluster/:namespace/backlogQuotaMap %}

[More info](../../reference/RestApi#/admin/namespaces/:property/:cluster/:namespace/backlogQuota)

#### Java

```java
Map<BacklogQuota.BacklogQuotaType,BacklogQuota> quotas =
  admin.namespaces().getBacklogQuotas(namespace);
```

### Remove backlog quotas

#### pulsar-admin

Use the [`remove-backlog-quotas`](../../reference/CliTools#pulsar-admin-namespaces-remove-backlog-quotas) subcommand and specify a namespace. Here's an example:

```shell
$ pulsar-admin namespaces remove-backlog-quotas my-prop/my-cluster/my-ns
```

#### REST API

{% endpoint DELETE /admin/namespaces/:property/:cluster/:namespace/backlogQuota %}

[More info](../../reference/RestApi#/admin/namespaces/:property/:cluster/:namespace/backlogQuota)

#### Java

```java
admin.namespaces().removeBacklogQuota(namespace);
```

### Clear backlog

#### pulsar-admin

Use the [`clear-backlog`](../../reference/CliTools#pulsar-admin-namespaces-clear-backlog) subcommand.

##### Example

```shell
$ pulsar-admin namespaces clear-backlog my-prop/my-cluster/my-ns
```

By default, you will be prompted to ensure that you really want to clear the backlog for the namespace. You can override the prompt using the `-f`/`--force` flag.

## Time to live (TTL)

By default, Pulsar stores all unacknowledged messages forever. This can lead to heavy disk space usage in cases where a lot of messages are going unacknowledged. If disk space is a concern, you can set a time to live (TTL) that determines how long unacknowledged messages will 

### Set the TTL for a namespace

#### pulsar-admin

Use the [`set-message-ttl`](../../reference/CliTools#pulsar-admin-namespaces-set-message-ttl) subcommand and specify a namespace and a TTL (in seconds) using the `-ttl`/`--messageTTL` flag.

##### Example

```shell
$ pulsar-admin namespaces set-message-ttl my-prop/my-cluster/my-ns \
  --messageTTL 120 # TTL of 2 minutes
```

#### REST API

{% endpoint POST /admin/namespaces/:property/:cluster/:namespace/messageTTL %}

[More info](../../reference/RestApi#/admin/namespaces/:property/:cluster/:namespace/messageTTL)

#### Java

```java
admin.namespaces().setNamespaceMessageTTL(namespace, ttlInSeconds);
```

### Get the TTL configuration for a namespace

#### pulsar-admin

Use the [`get-message-ttl`](../../reference/CliTools#pulsar-admin-namespaces-get-message-ttl) subcommand and specify a namespace.

##### Example

```shell
$ pulsar-admin namespaces get-message-ttl my-prop/my-cluster/my-ns
60
```

#### REST API

{% endpoint GET /admin/namespaces/:property/:cluster/:namespace/messageTTL %}

[More info](../../reference/RestApi#/admin/namespaces/:property/:cluster/:namespace/messageTTL)

#### Java

```java
admin.namespaces().get
```
