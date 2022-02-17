---
id: version-2.1.0-incubating-admin-api-persistent-topics
title: Managing persistent topics
sidebar_label: Persistent topics
original_id: admin-api-persistent-topics
---

Persistent helps to access topic which is a logical endpoint for publishing and consuming messages. Producers publish messages to the topic and consumers subscribe to the topic, to consume messages published to the topic.

In all of the instructions and commands below, the topic name structure is:


```shell
persistent://tenant/namespace/topic
```

## Persistent topics resources

### List of topics

It provides a list of persistent topics exist under a given namespace.

#### pulsar-admin

List of topics can be fetched using [`list`](../../reference/CliTools#list) command.

```shell
$ pulsar-admin topics list \
  my-tenant/my-namespace
```

#### REST API

{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace|operation/getList}

#### Java

```java
String namespace = "my-tenant/my-namespace";
admin.persistentTopics().getList(namespace);
```

### Grant permission

It grants permissions on a client role to perform specific actions on a given topic.

#### pulsar-admin

Permission can be granted using [`grant-permission`](../../reference/CliTools#grant-permission) command.

```shell
$ pulsar-admin topics grant-permission \
  --actions produce,consume --role application1 \
  persistent://test-tenant/ns1/tp1 \

```

#### REST API

{@inject: endpoint|POST|/admin/v2/:schema/:tenant/:namespace/:topic/permissions/:role|operation/grantPermissionsOnTopic}

#### Java

```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
String role = "test-role";
Set<AuthAction> actions  = Sets.newHashSet(AuthAction.produce, AuthAction.consume);
admin.persistentTopics().grantPermission(topic, role, actions);
```

### Get permission

Permission can be fetched using [`permissions`](../../reference/CliTools#permissions) command.

#### pulsar-admin

```shell
$ pulsar-admin topics permissions \
  persistent://test-tenant/ns1/tp1 \

{
    "application1": [
        "consume",
        "produce"
    ]
}
```

#### REST API

{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace/:topic/permissions|operation/getPermissionsOnTopic}

#### Java

```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
admin.persistentTopics().getPermissions(topic);
```

### Revoke permission

It revokes a permission which was granted on a client role.

#### pulsar-admin

Permission can be revoked using [`revoke-permission`](../../reference/CliTools#revoke-permission) command.

```shell
$ pulsar-admin topics revoke-permission \
  --role application1 \
  persistent://test-tenant/ns1/tp1 \

{
  "application1": [
    "consume",
    "produce"
  ]
}
```

#### REST API

{@inject: endpoint|DELETE|/admin/v2/:schema/:tenant/:namespace/:topic/permissions/:role|operation/revokePermissionsOnTopic}

#### Java

```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
String role = "test-role";
admin.persistentTopics().revokePermissions(topic, role);
```

### Delete topic

It deletes a topic. The topic cannot be deleted if there's any active subscription or producers connected to it.

#### pulsar-admin

Topic can be deleted using [`delete`](../../reference/CliTools#delete) command.

```shell
$ pulsar-admin topics delete \
  persistent://test-tenant/ns1/tp1 \
```

#### REST API

{@inject: endpoint|DELETE|/admin/v2/:schema/:tenant/:namespace/:topic|operation/deleteTopic}

#### Java

```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
admin.persistentTopics().delete(topic);
```

### Unload topic

It unloads a topic.

#### pulsar-admin

Topic can be unloaded using [`unload`](../../reference/CliTools#unload) command.

```shell
$ pulsar-admin topics unload \
  persistent://test-tenant/ns1/tp1 \
```

#### REST API

{@inject: endpoint|PUT|/admin/v2/:schema/:tenant/:namespace/:topic/unload|operation/unloadTopic}

#### Java

```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
admin.persistentTopics().unload(topic);
```

### Get stats

It shows current statistics of a given non-partitioned topic.

  -   **msgRateIn**: The sum of all local and replication publishers' publish rates in messages per second

  -   **msgThroughputIn**: Same as above, but in bytes per second instead of messages per second

  -   **msgRateOut**: The sum of all local and replication consumers' dispatch rates in messages per second

  -   **msgThroughputOut**: Same as above, but in bytes per second instead of messages per second

  -   **averageMsgSize**: The average size in bytes of messages published within the last interval

  -   **storageSize**: The sum of the ledgers' storage size for this topic. See

  -   **publishers**: The list of all local publishers into the topic. There can be zero or thousands

  -   **averageMsgSize**: Average message size in bytes from this publisher within the last interval

  -   **producerId**: Internal identifier for this producer on this topic

  -   **producerName**: Internal identifier for this producer, generated by the client library

  -   **address**: IP address and source port for the connection of this producer

  -   **connectedSince**: Timestamp this producer was created or last reconnected

  -   **subscriptions**: The list of all local subscriptions to the topic

  -   **my-subscription**: The name of this subscription (client defined)

  -   **msgBacklog**: The count of messages in backlog for this subscription

  -   **type**: This subscription type

  -   **msgRateExpired**: The rate at which messages were discarded instead of dispatched from this subscription due to TTL

  -   **consumers**: The list of connected consumers for this subscription

  -   **consumerName**: Internal identifier for this consumer, generated by the client library

  -   **availablePermits**: The number of messages this consumer has space for in the client library's listen queue. A value of 0 means the client library's queue is full and receive() isn't being called. A nonzero value means this consumer is ready to be dispatched messages.

  -   **replication**: This section gives the stats for cross-colo replication of this topic

  -   **replicationBacklog**: The outbound replication backlog in messages

  -   **connected**: Whether the outbound replicator is connected

  -   **replicationDelayInSeconds**: How long the oldest message has been waiting to be sent through the connection, if connected is true

  -   **inboundConnection**: The IP and port of the broker in the remote cluster's publisher connection to this broker

  -   **inboundConnectedSince**: The TCP connection being used to publish messages to the remote cluster. If there are no local publishers connected, this connection is automatically closed after a minute.

```json
{
  "msgRateIn": 4641.528542257553,
  "msgThroughputIn": 44663039.74947473,
  "msgRateOut": 0,
  "msgThroughputOut": 0,
  "averageMsgSize": 1232439.816728665,
  "storageSize": 135532389160,
  "publishers": [
    {
      "msgRateIn": 57.855383881403576,
      "msgThroughputIn": 558994.7078932219,
      "averageMsgSize": 613135,
      "producerId": 0,
      "producerName": null,
      "address": null,
      "connectedSince": null
    }
  ],
  "subscriptions": {
    "my-topic_subscription": {
      "msgRateOut": 0,
      "msgThroughputOut": 0,
      "msgBacklog": 116632,
      "type": null,
      "msgRateExpired": 36.98245516804671,
      "consumers": []
    }
  },
  "replication": {}
}
```

#### pulsar-admin

Topic stats can be fetched using [`stats`](../../reference/CliTools#stats) command.

```shell
$ pulsar-admin topics stats \
  persistent://test-tenant/ns1/tp1 \
```

#### REST API

{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace/:topic/stats|operation/getStats}

#### Java

```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
admin.persistentTopics().getStats(topic);
```

### Get internal stats

It shows detailed statistics of a topic.

  -   **entriesAddedCounter**: Messages published since this broker loaded this topic

  -   **numberOfEntries**: Total number of messages being tracked

  -   **totalSize**: Total storage size in bytes of all messages

  -   **currentLedgerEntries**: Count of messages written to the ledger currently open for writing

  -   **currentLedgerSize**: Size in bytes of messages written to ledger currently open for writing

  -   **lastLedgerCreatedTimestamp**: time when last ledger was created

  -   **lastLedgerCreationFailureTimestamp:** time when last ledger was failed

  -   **waitingCursorsCount**: How many cursors are "caught up" and waiting for a new message to be published

  -   **pendingAddEntriesCount**: How many messages have (asynchronous) write requests we are waiting on completion

  -   **lastConfirmedEntry**: The ledgerid:entryid of the last message successfully written. If the entryid is -1, then the ledger has been opened or is currently being opened but has no entries written yet.

  -   **state**: The state of this ledger for writing. LedgerOpened means we have a ledger open for saving published messages.

  -   **ledgers**: The ordered list of all ledgers for this topic holding its messages

  -   **cursors**: The list of all cursors on this topic. There will be one for every subscription you saw in the topic stats.

  -   **markDeletePosition**: The ack position: the last message the subscriber acknowledged receiving

  -   **readPosition**: The latest position of subscriber for reading message

  -   **waitingReadOp**: This is true when the subscription has read the latest message published to the topic and is waiting on new messages to be published.

  -   **pendingReadOps**: The counter for how many outstanding read requests to the BookKeepers we have in progress

  -   **messagesConsumedCounter**: Number of messages this cursor has acked since this broker loaded this topic

  -   **cursorLedger**: The ledger being used to persistently store the current markDeletePosition

  -   **cursorLedgerLastEntry**: The last entryid used to persistently store the current markDeletePosition

  -   **individuallyDeletedMessages**: If Acks are being done out of order, shows the ranges of messages Acked between the markDeletePosition and the read-position

  -   **lastLedgerSwitchTimestamp**: The last time the cursor ledger was rolled over

  -   **state**: The state of the cursor ledger: Open means we have a cursor ledger for saving updates of the markDeletePosition.

```json
{
    "entriesAddedCounter": 20449518,
    "numberOfEntries": 3233,
    "totalSize": 331482,
    "currentLedgerEntries": 3233,
    "currentLedgerSize": 331482,
    "lastLedgerCreatedTimestamp": "2016-06-29 03:00:23.825",
    "lastLedgerCreationFailureTimestamp": null,
    "waitingCursorsCount": 1,
    "pendingAddEntriesCount": 0,
    "lastConfirmedEntry": "324711539:3232",
    "state": "LedgerOpened",
    "ledgers": [
        {
            "ledgerId": 324711539,
            "entries": 0,
            "size": 0
        }
    ],
    "cursors": {
        "my-subscription": {
            "markDeletePosition": "324711539:3133",
            "readPosition": "324711539:3233",
            "waitingReadOp": true,
            "pendingReadOps": 0,
            "messagesConsumedCounter": 20449501,
            "cursorLedger": 324702104,
            "cursorLedgerLastEntry": 21,
            "individuallyDeletedMessages": "[(324711539:3134‥324711539:3136], (324711539:3137‥324711539:3140], ]",
            "lastLedgerSwitchTimestamp": "2016-06-29 01:30:19.313",
            "state": "Open"
        }
    }
}
```


#### pulsar-admin

Topic internal-stats can be fetched using [`stats-internal`](../../reference/CliTools#stats-internal) command.

```shell
$ pulsar-admin topics stats-internal \
  persistent://test-tenant/ns1/tp1 \
```

#### REST API

{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace/:topic/internalStats|operation/getInternalStats}

#### Java

```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
admin.persistentTopics().getInternalStats(topic);
```

### Peek messages

It peeks N messages for a specific subscription of a given topic.

#### pulsar-admin


```shell
$ pulsar-admin topics peek-messages \
  --count 10 --subscription my-subscription \
  persistent://test-tenant/ns1/tp1 \

Message ID: 315674752:0
Properties:  {  "X-Pulsar-publish-time" : "2015-07-13 17:40:28.451"  }
msg-payload
```

#### REST API

{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace/:topic/subscription/:subName/position/:messagePosition|operation/peekNthMessage}

#### Java

```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
String subName = "my-subscription";
int numMessages = 1;
admin.persistentTopics().peekMessages(topic, subName, numMessages);
```

### Skip messages

It skips N messages for a specific subscription of a given topic.

#### pulsar-admin


```shell
$ pulsar-admin topics skip \
  --count 10 --subscription my-subscription \
  persistent://test-tenant/ns1/tp1 \
```

#### REST API

{@inject: endpoint|POST|/admin/v2/:schema/:tenant/:namespace/:topic/subscription/:subName/skip/:numMessages|operation/skipMessages}

#### Java

```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
String subName = "my-subscription";
int numMessages = 1;
admin.persistentTopics().skipMessages(topic, subName, numMessages);
```

### Skip all messages

It skips all old messages for a specific subscription of a given topic.

#### pulsar-admin


```shell
$ pulsar-admin topics skip-all \
  --subscription my-subscription \
  persistent://test-tenant/ns1/tp1 \
```

#### REST API

{@inject: endpoint|POST|/admin/v2/:schema/:tenant/:namespace/:topic/subscription/:subName/skip_all|operation/skipAllMessages}

[More info](../../reference/RestApi#/admin/persistent/:tenant/:namespace/:topic/subscription/:subName/skip_all)

#### Java

```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
String subName = "my-subscription";
admin.persistentTopics().skipAllMessages(topic, subName);
```

### Reset cursor

It resets a subscription’s cursor position back to the position which was recorded X minutes before. It essentially calculates time and position of cursor at X minutes before and resets it at that position.

#### pulsar-admin


```shell
$ pulsar-admin topics reset-cursor \
  --subscription my-subscription --time 10 \
  persistent://test-tenant/ns1/tp1 \
```

#### REST API

{@inject: endpoint|POST|/admin/v2/:schema/:tenant/:namespace/:topic/subscription/:subName/resetcursor/:timestamp|operation/resetCursor}

#### Java

```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
String subName = "my-subscription";
long timestamp = 2342343L;
admin.persistentTopics().skipAllMessages(topic, subName, timestamp);
```

### Lookup of topic

It locates broker url which is serving the given topic.

#### pulsar-admin


```shell
$ pulsar-admin topics lookup \
  persistent://test-tenant/ns1/tp1 \

 "pulsar://broker1.org.com:4480"
```

#### REST API

{@inject: endpoint|GET|/lookup/v2/topic/:schema/:tenant:namespace/:topic|/}

#### Java

```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
admin.lookup().lookupDestination(topic);
```

### Get bundle

It gives range of the bundle which contains given topic

#### pulsar-admin


```shell
$ pulsar-admin topics bundle-range \
  persistent://test-tenant/ns1/tp1 \

 "0x00000000_0xffffffff"
```

#### REST API

{@inject: endpoint|GET|/lookup/v2/topic/:topic_domain/:tenant/:namespace/:topic/bundle|/}

#### Java

```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
admin.lookup().getBundleRange(topic);
```


### Get subscriptions

It shows all subscription names for a given topic.

#### pulsar-admin

```shell
$ pulsar-admin topics subscriptions \
  persistent://test-tenant/ns1/tp1 \

 my-subscription
```

#### REST API

{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace/:topic/subscriptions|operation/getSubscriptions}

#### Java

```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
admin.persistentTopics().getSubscriptions(topic);
```

### Unsubscribe

It can also help to unsubscribe a subscription which is no more processing further messages.

#### pulsar-admin


```shell
$ pulsar-admin topics unsubscribe \
  --subscription my-subscription \
  persistent://test-tenant/ns1/tp1 \
```

#### REST API

{@inject: endpoint|DELETE|/admin/v2/namespaces/:tenant/:namespace/:topic/subscription/:subscription|operation/deleteSubscription}

#### Java

```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
String subscriptionName = "my-subscription";
admin.persistentTopics().deleteSubscription(topic, subscriptionName);
```
