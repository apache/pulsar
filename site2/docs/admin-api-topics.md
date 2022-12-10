---
id: admin-api-topics
title: Manage topics
sidebar_label: "Topics"
---

````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````


:::tip

This page only shows **some frequently used operations**.

- For the latest and complete information about `Pulsar admin`, including commands, flags, descriptions, and more, see [Pulsar admin doc](/tools/pulsar-admin/)

- For the latest and complete information about `REST API`, including parameters, responses, samples, and more, see {@inject: rest:REST:/} API doc.

- For the latest and complete information about `Java admin API`, including classes, methods, descriptions, and more, see [Java admin API doc](/api/admin/).

:::

Pulsar has persistent and non-persistent topics. A persistent topic is a logical endpoint for publishing and consuming messages. The topic name structure for persistent topics is:

```shell
persistent://tenant/namespace/topic
```

Non-persistent topics are used in applications that only consume real-time published messages and do not need persistent guarantees. In this way, it reduces message-publish latency by removing overhead of persisting messages. The topic name structure for non-persistent topics is:

```shell
non-persistent://tenant/namespace/topic
```

## Manage topic resources
Whether it is a persistent or non-persistent topic, you can obtain the topic resources through `pulsar-admin` tool, REST API and Java.

:::note

In REST API, `:schema` stands for persistent or non-persistent. `:tenant`, `:namespace`, `:x` are variables, replace them with the real tenant, namespace, and `x` names when using them.     
Take {@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace|operation/getList?version=@pulsar:version_number@} as an example, to get the list of persistent topics in REST API, use `https://pulsar.apache.org/admin/v2/persistent/my-tenant/my-namespace`. To get the list of non-persistent topics in REST API, use `https://pulsar.apache.org/admin/v2/non-persistent/my-tenant/my-namespace`.

:::

### List of topics

You can get the list of topics under a given namespace in the following ways.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin topics list my-tenant/my-namespace
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace|operation/getList?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
String namespace = "my-tenant/my-namespace";
admin.topics().getList(namespace);
```

</TabItem>

</Tabs>
````

### Grant permission

You can grant permissions on a client role to perform specific actions on a given topic in the following ways.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin topics grant-permission \
    --actions produce,consume \
    --role application1 \
    persistent://test-tenant/ns1/tp1
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|POST|/admin/v2/:schema/:tenant/:namespace/:topic/permissions/:role|operation/grantPermissionsOnTopic?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
String role = "test-role";
Set<AuthAction> actions  = Sets.newHashSet(AuthAction.produce, AuthAction.consume);
admin.topics().grantPermission(topic, role, actions);
```

</TabItem>

</Tabs>
````

### Get permission

You can fetch permission in the following ways.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin topics permissions persistent://test-tenant/ns1/tp1 
```

Example output:

```
application1    [consume, produce]
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace/:topic/permissions|operation/getPermissionsOnTopic?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
admin.topics().getPermissions(topic);
```

</TabItem>

</Tabs>
````

### Revoke permission

You can revoke permissions granted on a client role in the following ways.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin topics revoke-permission \
    --role application1 \
    persistent://test-tenant/ns1/tp1 
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|DELETE|/admin/v2/:schema/:tenant/:namespace/:topic/permissions/:role|operation/revokePermissionsOnTopic?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
String role = "test-role";
admin.topics().revokePermissions(topic, role);
```

</TabItem>

</Tabs>
````

### Delete topic

You can delete a topic in the following ways. You cannot delete a topic if any active subscription or producer is connected to the topic.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin topics delete persistent://test-tenant/ns1/tp1
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|DELETE|/admin/v2/:schema/:tenant/:namespace/:topic|operation/deleteTopic?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
admin.topics().delete(topic);
```

</TabItem>

</Tabs>
````

### Unload topic

You can unload a topic in the following ways.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin topics unload persistent://test-tenant/ns1/tp1
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|PUT|/admin/v2/:schema/:tenant/:namespace/:topic/unload|operation/unloadTopic?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
admin.topics().unload(topic);
```

</TabItem>

</Tabs>
````

### Get stats

For the detailed statistics of a topic, see [Pulsar statistics](administration-stats.md#topic-stats).

The following is an example of a topic status.

```json
{
  "msgRateIn" : 0.0,
  "msgThroughputIn" : 0.0,
  "msgRateOut" : 0.0,
  "msgThroughputOut" : 0.0,
  "bytesInCounter" : 504,
  "msgInCounter" : 9,
  "bytesOutCounter" : 2296,
  "msgOutCounter" : 41,
  "averageMsgSize" : 0.0,
  "msgChunkPublished" : false,
  "storageSize" : 504,
  "backlogSize" : 0,
  "filteredEntriesCount" : 100,
  "earliestMsgPublishTimeInBacklogs": 0,
  "offloadedStorageSize" : 0,
  "publishers" : [ {
    "accessMode" : "Shared",
    "msgRateIn" : 0.0,
    "msgThroughputIn" : 0.0,
    "averageMsgSize" : 0.0,
    "chunkedMessageRate" : 0.0,
    "producerId" : 0,
    "metadata" : { },
    "address" : "/127.0.0.1:65402",
    "connectedSince" : "2021-06-09T17:22:55.913+08:00",
    "clientVersion" : "2.9.0-SNAPSHOT",
    "producerName" : "standalone-1-0"
  } ],
  "waitingPublishers" : 0,
  "subscriptions" : {
    "sub-demo" : {
      "msgRateOut" : 0.0,
      "msgThroughputOut" : 0.0,
      "bytesOutCounter" : 2296,
      "msgOutCounter" : 41,
      "msgRateRedeliver" : 0.0,
      "chunkedMessageRate" : 0,
      "msgBacklog" : 0,
      "backlogSize" : 0,
      "earliestMsgPublishTimeInBacklog": 0,
      "msgBacklogNoDelayed" : 0,
      "blockedSubscriptionOnUnackedMsgs" : false,
      "msgDelayed" : 0,
      "unackedMessages" : 0,
      "type" : "Exclusive",
      "activeConsumerName" : "20b81",
      "msgRateExpired" : 0.0,
      "totalMsgExpired" : 0,
      "lastExpireTimestamp" : 0,
      "lastConsumedFlowTimestamp" : 1623230565356,
      "lastConsumedTimestamp" : 1623230583946,
      "lastAckedTimestamp" : 1623230584033,
      "lastMarkDeleteAdvancedTimestamp" : 1623230584033,
      "filterProcessedMsgCount": 100,
      "filterAcceptedMsgCount": 100,
      "filterRejectedMsgCount": 0,
      "filterRescheduledMsgCount": 0,
      "consumers" : [ {
        "msgRateOut" : 0.0,
        "msgThroughputOut" : 0.0,
        "bytesOutCounter" : 2296,
        "msgOutCounter" : 41,
        "msgRateRedeliver" : 0.0,
        "chunkedMessageRate" : 0.0,
        "consumerName" : "20b81",
        "availablePermits" : 959,
        "unackedMessages" : 0,
        "avgMessagesPerEntry" : 314,
        "blockedConsumerOnUnackedMsgs" : false,
        "lastAckedTimestamp" : 1623230584033,
        "lastConsumedTimestamp" : 1623230583946,
        "metadata" : { },
        "address" : "/127.0.0.1:65172",
        "connectedSince" : "2021-06-09T17:22:45.353+08:00",
        "clientVersion" : "2.9.0-SNAPSHOT"
      } ],
      "allowOutOfOrderDelivery": false,
      "consumersAfterMarkDeletePosition" : { },
      "nonContiguousDeletedMessagesRanges" : 0,
      "nonContiguousDeletedMessagesRangesSerializedSize" : 0,
      "durable" : true,
      "replicated" : false
    }
  },
  "replication" : { },
  "deduplicationStatus" : "Disabled",
  "nonContiguousDeletedMessagesRanges" : 0,
  "nonContiguousDeletedMessagesRangesSerializedSize" : 0,
  "ownerBroker" : "localhost:8080"
}
```

To get the status of a topic, you can use the following ways.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin topics stats persistent://test-tenant/ns1/tp1
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace/:topic/stats|operation/getStats?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
admin.topics().getStats(topic);
```

</TabItem>

</Tabs>
````

### Get internal stats

For the detailed internal statistics inside a topic, see [Pulsar statistics](administration-stats.md#topic-internal-stats).

The following is an example of the internal statistics of a topic.

```json
{
    "entriesAddedCounter":0,
    "numberOfEntries":0,
    "totalSize":0,
    "currentLedgerEntries":0,
    "currentLedgerSize":0,
    "lastLedgerCreatedTimestamp":"2021-01-22T21:12:14.868+08:00",
    "lastLedgerCreationFailureTimestamp":null,
    "waitingCursorsCount":0,
    "pendingAddEntriesCount":0,
    "lastConfirmedEntry":"3:-1",
    "state":"LedgerOpened",
    "ledgers":[
        {
            "ledgerId":3,
            "entries":0,
            "size":0,
            "offloaded":false,
            "metadata":null
        }
    ],
    "cursors":{
        "test":{
            "markDeletePosition":"3:-1",
            "readPosition":"3:-1",
            "waitingReadOp":false,
            "pendingReadOps":0,
            "messagesConsumedCounter":0,
            "cursorLedger":4,
            "cursorLedgerLastEntry":1,
            "individuallyDeletedMessages":"[]",
            "lastLedgerSwitchTimestamp":"2021-01-22T21:12:14.966+08:00",
            "state":"Open",
            "numberOfEntriesSinceFirstNotAckedMessage":0,
            "totalNonContiguousDeletedMessagesRange":0,
            "properties":{

            }
        }
    },
    "schemaLedgers":[
        {
            "ledgerId":1,
            "entries":11,
            "size":10,
            "offloaded":false,
            "metadata":null
        }
    ],
    "compactedLedger":{
        "ledgerId":-1,
        "entries":-1,
        "size":-1,
        "offloaded":false,
        "metadata":null
    }
}
```

To get the internal status of a topic, you can use the following ways.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin topics stats-internal persistent://test-tenant/ns1/tp1
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace/:topic/internalStats|operation/getInternalStats?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
admin.topics().getInternalStats(topic);
```

</TabItem>

</Tabs>
````

### Peek messages

You can peek a number of messages for a specific subscription of a given topic in the following ways.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin topics peek-messages \
    --count 10 --subscription my-subscription \
    persistent://test-tenant/ns1/tp1
```

Example output:

```
Message ID: 77:2
Publish time: 1668674963028
Event time: 0
         +-------------------------------------------------+
         |  0  1  2  3  4  5  6  7  8  9  a  b  c  d  e  f |
+--------+-------------------------------------------------+----------------+
|00000000| 68 65 6c 6c 6f 2d 31                            |hello-1         |
+--------+-------------------------------------------------+----------------+
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace/:topic/subscription/:subName/position/:messagePosition|operation/peekNthMessage?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
String subName = "my-subscription";
int numMessages = 1;
admin.topics().peekMessages(topic, subName, numMessages);
```

</TabItem>

</Tabs>
````

### Get message by ID

You can fetch the message with the given ledger ID and entry ID in the following ways.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin topics get-message-by-id \
    -l 10 -e 0 persistent://public/default/my-topic
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace/:topic/ledger/:ledgerId/entry/:entryId|operation/getMessageById?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
long ledgerId = 10;
long entryId = 10;
admin.topics().getMessageById(topic, ledgerId, entryId);
```

</TabItem>

</Tabs>
````

### Examine messages

You can examine a specific message on a topic by position relative to the earliest or the latest message.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin topics examine-messages \
    -i latest -m 1 persistent://public/default/my-topic
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace/:topic/examinemessage?initialPosition=:initialPosition&messagePosition=:messagePosition|operation/examineMessage?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
admin.topics().examineMessage(topic, "latest", 1);
```

</TabItem>

</Tabs>
````

### Get message ID

You can get message ID published at or just after the given datetime.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin topics get-message-id \
    persistent://public/default/my-topic \
    -d 2021-06-28T19:01:17Z
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace/:topic/messageid/:timestamp|operation/getMessageIdByTimestamp?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
long timestamp = System.currentTimeMillis()
admin.topics().getMessageIdByTimestamp(topic, timestamp);
```

</TabItem>

</Tabs>
````


### Skip messages

You can skip a number of messages for a specific subscription of a given topic in the following ways.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin topics skip \
    --count 10 --subscription my-subscription \
    persistent://test-tenant/ns1/tp1
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|POST|/admin/v2/:schema/:tenant/:namespace/:topic/subscription/:subName/skip/:numMessages|operation/skipMessages?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
String subName = "my-subscription";
int numMessages = 1;
admin.topics().skipMessages(topic, subName, numMessages);
```

</TabItem>

</Tabs>
````

### Skip all messages

You can skip all the old messages for a specific subscription of a given topic.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin topics clear-backlog \
    --subscription my-subscription \
    persistent://test-tenant/ns1/tp1
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|POST|/admin/v2/:schema/:tenant/:namespace/:topic/subscription/:subName/skip_all|operation/skipAllMessages?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
String subName = "my-subscription";
admin.topics().skipAllMessages(topic, subName);
```

</TabItem>

</Tabs>
````

### Reset cursor

You can reset a subscription cursor position back to the position which is recorded X minutes before. It essentially calculates the time and position of the cursor at X minutes before and resets it at that position. You can reset the cursor in the following ways.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin topics reset-cursor \
    --subscription my-subscription --time 10 \
    persistent://test-tenant/ns1/tp1
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|POST|/admin/v2/:schema/:tenant/:namespace/:topic/subscription/:subName/resetcursor/:timestamp|operation/resetCursor?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java

String topic = "persistent://my-tenant/my-namespace/my-topic";
String subName = "my-subscription";
long timestamp = 2342343L;
admin.topics().resetCursor(topic, subName, timestamp);

```

</TabItem>

</Tabs>
````

### Look up topic's owner broker

You can locate the owner broker of the given topic in the following ways.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin topics lookup persistent://test-tenant/ns1/tp1
```

Example output:

```
"pulsar://broker1.org.com:4480"
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/lookup/v2/topic/:topic-domain/:tenant/:namespace/:topic|operation/lookupTopicAsync?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
admin.lookups().lookupDestination(topic);
```

</TabItem>

</Tabs>
````

### Look up partitioned topic's owner broker

You can locate the owner broker of the given partitioned topic in the following ways.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin topics partitioned-lookup persistent://test-tenant/ns1/my-topic
```

Example output:

```
"persistent://test-tenant/ns1/my-topic-partition-0   pulsar://localhost:6650"
"persistent://test-tenant/ns1/my-topic-partition-1   pulsar://localhost:6650"
"persistent://test-tenant/ns1/my-topic-partition-2   pulsar://localhost:6650"
"persistent://test-tenant/ns1/my-topic-partition-3   pulsar://localhost:6650"
```

Lookup the partitioned topics sorted by broker URL

```shell
pulsar-admin topics partitioned-lookup \
    persistent://test-tenant/ns1/my-topic --sort-by-broker
```

Example output:

```
pulsar://localhost:6650   [persistent://test-tenant/ns1/my-topic-partition-0, persistent://test-tenant/ns1/my-topic-partition-1, persistent://test-tenant/ns1/my-topic-partition-2, persistent://test-tenant/ns1/my-topic-partition-3]
```

</TabItem>
<TabItem value="Java">

```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
admin.lookups().lookupPartitionedTopic(topic);
```

</TabItem>

</Tabs>
````

### Get bundle

You can get the range of the bundle that the given topic belongs to in the following ways.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin topics bundle-range persistent://test-tenant/ns1/tp1
```

Example output:

```
"0x00000000_0xffffffff"
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/lookup/v2/topic/:topic_domain/:tenant/:namespace/:topic/bundle|operation/getNamespaceBundle?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
admin.lookups().getBundleRange(topic);
```

</TabItem>

</Tabs>
````

### Get subscriptions

You can check all subscription names for a given topic in the following ways.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin topics subscriptions persistent://test-tenant/ns1/tp1
```

Example output:

```
my-subscription
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace/:topic/subscriptions|operation/getSubscriptions?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
admin.topics().getSubscriptions(topic);
```

</TabItem>

</Tabs>
````

### Last Message Id

You can get the last committed message ID for a persistent topic. It is available since 2.3.0 release.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin topics last-message-id topic-name
```

Example output:

```json
{
  "ledgerId" : 97,
  "entryId" : 9,
  "partitionIndex" : -1
}
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|Get|/admin/v2/:schema/:tenant/:namespace/:topic/lastMessageId|operation/getLastMessageId?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
admin.topics().getLastMessage(topic);
```

</TabItem>

</Tabs>
````

### Get backlog size

You can get the backlog size of a single partition topic or a non-partitioned topic with a given message ID (in bytes).

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin topics get-backlog-size \
  -m 1:1 \
  persistent://test-tenant/ns1/tp1-partition-0
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|PUT|/admin/v2/:schema/:tenant/:namespace/:topic/backlogSize|operation/getBacklogSizeByMessageId?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
MessageId messageId = MessageId.earliest;
admin.topics().getBacklogSizeByMessageId(topic, messageId);
```

</TabItem>

</Tabs>
````


### Configure deduplication snapshot interval

#### Get deduplication snapshot interval

To get the topic-level deduplication snapshot interval, use one of the following methods.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin topics get-deduplication-snapshot-interval my-topic
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/topics/:tenant/:namespace/:topic/deduplicationSnapshotInterval|operation/getDeduplicationSnapshotInterval?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.topics().getDeduplicationSnapshotInterval(topic)
```

</TabItem>

</Tabs>
````

#### Set deduplication snapshot interval

To set the topic-level deduplication snapshot interval, use one of the following methods.

> **Prerequisite** `brokerDeduplicationEnabled` must be set to `true`.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin topics set-deduplication-snapshot-interval my-topic -i 1000
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|POST|/admin/v2/topics/:tenant/:namespace/:topic/deduplicationSnapshotInterval|operation/setDeduplicationSnapshotInterval?version=@pulsar:version_number@}

```json
{
  "interval": 1000
}
```

</TabItem>
<TabItem value="Java">

```java
admin.topics().setDeduplicationSnapshotInterval(topic, 1000)
```

</TabItem>

</Tabs>
````

#### Remove deduplication snapshot interval

To remove the topic-level deduplication snapshot interval, use one of the following methods.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin topics remove-deduplication-snapshot-interval my-topic
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|DELETE|/admin/v2/topics/:tenant/:namespace/:topic/deduplicationSnapshotInterval|operation/deleteDeduplicationSnapshotInterval?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.topics().removeDeduplicationSnapshotInterval(topic)
```

</TabItem>

</Tabs>
````


### Configure inactive topic policies

#### Get inactive topic policies

To get the topic-level inactive topic policies, use one of the following methods.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin topics get-inactive-topic-policies my-topic
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/topics/:tenant/:namespace/:topic/inactiveTopicPolicies|operation/getInactiveTopicPolicies?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.topics().getInactiveTopicPolicies(topic)
```

</TabItem>

</Tabs>
````

#### Set inactive topic policies

To set the topic-level inactive topic policies, use one of the following methods.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin topics set-inactive-topic-policies my-topic
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|POST|/admin/v2/topics/:tenant/:namespace/:topic/inactiveTopicPolicies|operation/setInactiveTopicPolicies?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.topics().setInactiveTopicPolicies(topic, inactiveTopicPolicies)
```

</TabItem>

</Tabs>
````

#### Remove inactive topic policies

To remove the topic-level inactive topic policies, use one of the following methods.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin topics remove-inactive-topic-policies my-topic
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|DELETE|/admin/v2/topics/:tenant/:namespace/:topic/inactiveTopicPolicies|operation/removeInactiveTopicPolicies?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.topics().removeInactiveTopicPolicies(topic)
```

</TabItem>

</Tabs>
````


### Configure offload policies

#### Get offload policies

To get the topic-level offload policies, use one of the following methods.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin topics get-offload-policies my-topic
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/topics/:tenant/:namespace/:topic/offloadPolicies|operation/getOffloadPolicies?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.topics().getOffloadPolicies(topic)
```

</TabItem>

</Tabs>
````

#### Set offload policies

To set the topic-level offload policies, use one of the following methods.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin topics set-offload-policies my-topic
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|POST|/admin/v2/topics/:tenant/:namespace/:topic/offloadPolicies|operation/setOffloadPolicies?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.topics().setOffloadPolicies(topic, offloadPolicies)
```

</TabItem>

</Tabs>
````

#### Remove offload policies

To remove the topic-level offload policies, use one of the following methods.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin topics remove-offload-policies my-topic
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|DELETE|/admin/v2/topics/:tenant/:namespace/:topic/offloadPolicies|operation/removeOffloadPolicies?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.topics().removeOffloadPolicies(topic)
```

</TabItem>

</Tabs>
````


## Manage non-partitioned topics
You can use Pulsar [admin API](admin-api-overview.md) to create, delete and check the status of non-partitioned topics.

### Create
Non-partitioned topics must be explicitly created. When creating a new non-partitioned topic, you need to provide a name for the topic.

By default, 60 seconds after creation, topics are considered inactive and deleted automatically to avoid generating trash data. To disable this feature, set `brokerDeleteInactiveTopicsEnabled` to `false`. To change the frequency of checking inactive topics, set `brokerDeleteInactiveTopicsFrequencySeconds` to a specific value.

For more information about the two parameters, see [here](reference-configuration.md#broker).

You can create non-partitioned topics in the following ways.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

When you create non-partitioned topics with the [`create`](/tools/pulsar-admin/) command, you need to specify the topic name as an argument.

```shell
pulsar-admin topics create \
    persistent://my-tenant/my-namespace/my-topic
```

:::note

When you create a non-partitioned topic with the suffix '-partition-' followed by numeric value like 'xyz-topic-partition-x' for the topic name, if a partitioned topic with same suffix 'xyz-topic-partition-y' exists, then the numeric value(x) for the non-partitioned topic must be larger than the number of partitions(y) of the partitioned topic. Otherwise, you cannot create such a non-partitioned topic.

:::

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|PUT|/admin/v2/:schema/:tenant/:namespace/:topic|operation/createNonPartitionedTopic?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
String topicName = "persistent://my-tenant/my-namespace/my-topic";
admin.topics().createNonPartitionedTopic(topicName);
```

</TabItem>

</Tabs>
````

### Delete

You can delete non-partitioned topics in the following ways.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin topics delete \
    persistent://my-tenant/my-namespace/my-topic
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|DELETE|/admin/v2/:schema/:tenant/:namespace/:topic|operation/deleteTopic?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.topics().delete(topic);
```

</TabItem>

</Tabs>
````

### List

You can get the list of topics under a given namespace in the following ways.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin topics list tenant/namespace
```

Example output:

```
persistent://tenant/namespace/topic1
persistent://tenant/namespace/topic2
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace|operation/getList?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.topics().getList(namespace);
```

</TabItem>

</Tabs>
````

### Stats


You can check the current statistics of a given topic and its connected producers and consumers in the following ways.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin topics stats \
    persistent://test-tenant/namespace/topic \
    --get-precise-backlog
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace/:topic/stats|operation/getStats?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.topics().getStats(topic, false /* is precise backlog */);
```

</TabItem>

</Tabs>
````

The following is an example. For the description of topic stats, see [Pulsar statistics](administration-stats.md#topic-stats).

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

## Manage partitioned topics
You can use Pulsar [admin API](admin-api-overview.md) to create, update, delete and check the status of partitioned topics.

### Create

When creating a new partitioned topic, you need to provide a name and the number of partitions for the topic.

:::note

By default, if there are no messages 60 seconds after creation, topics are considered inactive and deleted automatically to avoid generating trash data. To disable this feature, set `brokerDeleteInactiveTopicsEnabled` to `false`. To change the frequency of checking inactive topics, set `brokerDeleteInactiveTopicsFrequencySeconds` to a specific value.

:::

For more information about the two parameters, see [here](reference-configuration.md#broker).

You can create partitioned topics in the following ways.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

When you create partitioned topics with the [`create-partitioned-topic`](/tools/pulsar-admin/)
command, you need to specify the topic name as an argument and the number of partitions using the `-p` or `--partitions` flag.

```shell
pulsar-admin topics create-partitioned-topic \
    persistent://my-tenant/my-namespace/my-topic \
    --partitions 4
```

:::note

If a non-partitioned topic with the suffix '-partition-' followed by a numeric value like 'xyz-topic-partition-10', you can not create a partitioned topic with name 'xyz-topic', because the partitions of the partitioned topic could override the existing non-partitioned topic. To create such partitioned topic, you have to delete that non-partitioned topic first.

:::

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|PUT|/admin/v2/:schema/:tenant/:namespace/:topic/partitions|operation/createPartitionedTopic?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
String topicName = "persistent://my-tenant/my-namespace/my-topic";
int numPartitions = 4;
admin.topics().createPartitionedTopic(topicName, numPartitions);
```

</TabItem>

</Tabs>
````

### Create missed partitions

When topic auto-creation is disabled, and you have a partitioned topic without any partitions, you can use the [`create-missed-partitions`](/tools/pulsar-admin/) command to create partitions for the topic.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

You can create missed partitions with the [`create-missed-partitions`](/tools/pulsar-admin/) command and specify the topic name as an argument.

```shell
pulsar-admin topics create-missed-partitions \
    persistent://my-tenant/my-namespace/my-topic
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|POST|/admin/v2/:schema/:tenant/:namespace/:topic|operation/createMissedPartitions?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
String topicName = "persistent://my-tenant/my-namespace/my-topic";
admin.topics().createMissedPartitions(topicName);
```

</TabItem>

</Tabs>
````

### Get metadata

Partitioned topics are associated with metadata, you can view it as a JSON object. The following metadata field is available.

Field | Description
:-----|:-------
`partitions` | The number of partitions into which the topic is divided.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

You can check the number of partitions in a partitioned topic with the [`get-partitioned-topic-metadata`](/tools/pulsar-admin/) subcommand. 

```shell
pulsar-admin topics get-partitioned-topic-metadata \
    persistent://my-tenant/my-namespace/my-topic
```

Example output:

```json
{
  "partitions" : 4,
  "deleted" : false
}
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace/:topic/partitions|operation/getPartitionedMetadata?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
String topicName = "persistent://my-tenant/my-namespace/my-topic";
admin.topics().getPartitionedTopicMetadata(topicName);
```

</TabItem>

</Tabs>
````

### Update

You can update the number of partitions for an existing partitioned topic *if* the topic is non-global. However, you can only add the partition number. Decrementing the number of partitions would delete the topic, which is not supported in Pulsar.

Producers and consumers can find the newly created partitions automatically.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

You can update partitioned topics with the [`update-partitioned-topic`](/tools/pulsar-admin/) command.

```shell
pulsar-admin topics update-partitioned-topic \
    persistent://my-tenant/my-namespace/my-topic \
    --partitions 8
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|POST|/admin/v2/:schema/:tenant/:cluster/:namespace/:destination/partitions|operation/updatePartitionedTopic?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.topics().updatePartitionedTopic(topic, numPartitions);
```

</TabItem>

</Tabs>
````

### Delete
You can delete partitioned topics with the [`delete-partitioned-topic`](/tools/pulsar-admin/) command, REST API and Java.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin topics delete-partitioned-topic \
    persistent://my-tenant/my-namespace/my-topic
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|DELETE|/admin/v2/:schema/:topic/:namespace/:destination/partitions|operation/deletePartitionedTopic?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.topics().delete(topic);
```

</TabItem>

</Tabs>
````

### List

You can get the list of partitioned topics under a given namespace in the following ways.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin topics list-partitioned-topics tenant/namespace
```

Example output:

```
persistent://tenant/namespace/topic1
persistent://tenant/namespace/topic2
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace|operation/getPartitionedTopicList?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.topics().getPartitionedTopicList(namespace);
```

</TabItem>

</Tabs>
````

### Stats


You can check the current statistics of a given partitioned topic and its connected producers and consumers in the following ways.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin topics partitioned-stats \
    persistent://test-tenant/namespace/topic \
    --per-partition
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace/:topic/partitioned-stats|operation/getPartitionedStats?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.topics().getPartitionedStats(topic, true /* per partition */, false /* is precise backlog */);
```

</TabItem>

</Tabs>
````

The following is an example. For the description of each topic stats, see [Pulsar statistics](administration-stats.md#topic-stats).

Note that in the subscription JSON object, `chuckedMessageRate` is deprecated. Please use `chunkedMessageRate`. Both will be sent in the JSON for now.

```json
{
  "msgRateIn" : 999.992947159793,
  "msgThroughputIn" : 1070918.4635439808,
  "msgRateOut" : 0.0,
  "msgThroughputOut" : 0.0,
  "bytesInCounter" : 270318763,
  "msgInCounter" : 252489,
  "bytesOutCounter" : 0,
  "msgOutCounter" : 0,
  "averageMsgSize" : 1070.926056966454,
  "msgChunkPublished" : false,
  "storageSize" : 270316646,
  "backlogSize" : 200921133,
  "publishers" : [ {
    "msgRateIn" : 999.992947159793,
    "msgThroughputIn" : 1070918.4635439808,
    "averageMsgSize" : 1070.3333333333333,
    "chunkedMessageRate" : 0.0,
    "producerId" : 0
  } ],
  "subscriptions" : {
    "test" : {
      "msgRateOut" : 0.0,
      "msgThroughputOut" : 0.0,
      "bytesOutCounter" : 0,
      "msgOutCounter" : 0,
      "msgRateRedeliver" : 0.0,
      "chuckedMessageRate" : 0,
      "chunkedMessageRate" : 0,
      "msgBacklog" : 144318,
      "msgBacklogNoDelayed" : 144318,
      "blockedSubscriptionOnUnackedMsgs" : false,
      "msgDelayed" : 0,
      "unackedMessages" : 0,
      "msgRateExpired" : 0.0,
      "lastExpireTimestamp" : 0,
      "lastConsumedFlowTimestamp" : 0,
      "lastConsumedTimestamp" : 0,
      "lastAckedTimestamp" : 0,
      "consumers" : [ ],
      "isDurable" : true,
      "isReplicated" : false
    }
  },
  "replication" : { },
  "metadata" : {
    "partitions" : 3
  },
  "partitions" : { }
}
```

### Internal stats

You can check the detailed statistics of a topic. The following is an example. For the description of each internal topic stats, see [Pulsar statistics](administration-stats.md#topic-internal-stats).

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

You can get the internal stats for the partitioned topic in the following ways.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>
<TabItem value="pulsar-admin">

```shell
pulsar-admin topics stats-internal \
persistent://test-tenant/namespace/topic
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace/:topic/internalStats|operation/getInternalStats?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
admin.topics().getInternalStats(topic);
```

</TabItem>

</Tabs>
````


## Manage subscriptions

You can use [Pulsar admin API](admin-api-overview.md) to create, check, and delete subscriptions.

### Create subscription

You can create a subscription for a topic using one of the following methods.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>

<TabItem value="pulsar-admin">

```shell
pulsar-admin topics create-subscription \
    --subscription my-subscription \
    persistent://test-tenant/ns1/tp1
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|PUT|/admin/v2/persistent/:tenant/:namespace/:topic/subscription/:subscription|operation/createSubscriptions?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
String subscriptionName = "my-subscription";
admin.topics().createSubscription(topic, subscriptionName, MessageId.latest);
```

</TabItem>

</Tabs>
````

### Get subscription

You can check all subscription names for a given topic using one of the following methods.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>

<TabItem value="pulsar-admin">

```shell
pulsar-admin topics subscriptions persistent://test-tenant/ns1/tp1
```

Example output:

```
my-subscription
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace/:topic/subscriptions|operation/getSubscriptions?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
admin.topics().getSubscriptions(topic);
```

</TabItem>

</Tabs>
````

### Unsubscribe subscription

When a subscription does not process messages anymore, you can unsubscribe it using one of the following methods.

````mdx-code-block
<Tabs groupId="api-choice"
  defaultValue="pulsar-admin"
  values={[{"label":"pulsar-admin","value":"pulsar-admin"},{"label":"REST API","value":"REST API"},{"label":"Java","value":"Java"}]}>

<TabItem value="pulsar-admin">

```shell
pulsar-admin topics unsubscribe \
    --subscription my-subscription \
    persistent://test-tenant/ns1/tp1
```

</TabItem>
<TabItem value="REST API">

{@inject: endpoint|DELETE|/admin/v2/namespaces/:tenant/:namespace/:topic/subscription/:subscription|operation/deleteSubscription?version=@pulsar:version_number@}

</TabItem>
<TabItem value="Java">

```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
String subscriptionName = "my-subscription";
admin.topics().deleteSubscription(topic, subscriptionName);
```

</TabItem>

</Tabs>
````
