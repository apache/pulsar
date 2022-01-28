---
id: version-2.8.1-admin-api-topics
title: Manage topics
sidebar_label: Topics
original_id: admin-api-topics
---

Pulsar has persistent and non-persistent topics. Persistent topic is a logical endpoint for publishing and consuming messages. The topic name structure for persistent topics is:

```shell
persistent://tenant/namespace/topic
```

Non-persistent topics are used in applications that only consume real-time published messages and do not need persistent guarantee. In this way, it reduces message-publish latency by removing overhead of persisting messages. The topic name structure for non-persistent topics is:

```shell
non-persistent://tenant/namespace/topic
```
## Manage topic resources
Whether it is persistent or non-persistent topic, you can obtain the topic resources through `pulsar-admin` tool, REST API and Java.

> **Note**    
> In REST API, `:schema` stands for persistent or non-persistent. `:tenant`, `:namespace`, `:x` are variables, replace them with the real tenant, namespace, and `x` names when using them.     
> Take {@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace|operation/getList?version=[[pulsar:version_number]]} as an example, to get the list of persistent topics in REST API, use `https://pulsar.apache.org/admin/v2/persistent/my-tenant/my-namespace`. To get the list of non-persistent topics in REST API, use `https://pulsar.apache.org/admin/v2/non-persistent/my-tenant/my-namespace`.

### List of topics

You can get the list of topics under a given namespace in the following ways.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->

```shell
$ pulsar-admin topics list \
  my-tenant/my-namespace
```

<!--REST API-->
{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace|operation/getList?version=[[pulsar:version_number]]}

<!--Java-->
```java
String namespace = "my-tenant/my-namespace";
admin.topics().getList(namespace);
```
<!--END_DOCUSAURUS_CODE_TABS-->

### Grant permission

You can grant permissions on a client role to perform specific actions on a given topic in the following ways.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
```shell
$ pulsar-admin topics grant-permission \
  --actions produce,consume --role application1 \
  persistent://test-tenant/ns1/tp1 \
```

<!--REST API-->
{@inject: endpoint|POST|/admin/v2/:schema/:tenant/:namespace/:topic/permissions/:role|operation/grantPermissionsOnTopic?version=[[pulsar:version_number]]}

<!--Java-->
```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
String role = "test-role";
Set<AuthAction> actions  = Sets.newHashSet(AuthAction.produce, AuthAction.consume);
admin.topics().grantPermission(topic, role, actions);
```

<!--END_DOCUSAURUS_CODE_TABS-->

### Get permission

You can fetch permission in the following ways.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
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

<!--REST API-->
{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace/:topic/permissions|operation/getPermissionsOnTopic?version=[[pulsar:version_number]]}

<!--Java-->
```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
admin.topics().getPermissions(topic);
```

<!--END_DOCUSAURUS_CODE_TABS-->

### Revoke permission

You can revoke a permission granted on a client role in the following ways.
<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
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

<!--REST API-->
{@inject: endpoint|DELETE|/admin/v2/:schema/:tenant/:namespace/:topic/permissions/:role|operation/revokePermissionsOnTopic?version=[[pulsar:version_number]]}

<!--Java-->
```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
String role = "test-role";
admin.topics().revokePermissions(topic, role);
```

<!--END_DOCUSAURUS_CODE_TABS-->

### Delete topic

You can delete a topic in the following ways. You cannot delete a topic if any active subscription or producers is connected to the topic.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
```shell
$ pulsar-admin topics delete \
  persistent://test-tenant/ns1/tp1 \
```

<!--REST API-->
{@inject: endpoint|DELETE|/admin/v2/:schema/:tenant/:namespace/:topic|operation/deleteTopic?version=[[pulsar:version_number]]}

<!--Java-->
```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
admin.topics().delete(topic);
```

<!--END_DOCUSAURUS_CODE_TABS-->

### Unload topic

You can unload a topic in the following ways.
<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
```shell
$ pulsar-admin topics unload \
  persistent://test-tenant/ns1/tp1 \
```

<!--REST API-->
{@inject: endpoint|PUT|/admin/v2/:schema/:tenant/:namespace/:topic/unload|operation/unloadTopic?version=[[pulsar:version_number]]}

<!--Java-->
```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
admin.topics().unload(topic);
```

<!--END_DOCUSAURUS_CODE_TABS-->

### Get stats

You can check the following statistics of a given non-partitioned topic.

  -   **msgRateIn**: The sum of all local and replication publishers' publish rates (msg/s).

  -   **msgThroughputIn**: The sum of all local and replication publishers' publish rates (bytes/s).

  -   **msgRateOut**: The sum of all local and replication consumers' dispatch rates(msg/s).

  -   **msgThroughputOut**: The sum of all local and replication consumers' dispatch rates (bytes/s).

  -   **averageMsgSize**: The average size (in bytes) of messages published within the last interval.

  -   **storageSize**: The sum of the ledgers' storage size for this topic. The space used to store the messages for the topic.

  -   **publishers**: The list of all local publishers into the topic. The list ranges from zero to thousands.

      -   **msgRateIn**: The total rate of messages (msg/s) published by this publisher.

      -   **msgThroughputIn**: The total throughput (bytes/s) of the messages published by this publisher.

      -   **averageMsgSize**: The average message size in bytes from this publisher within the last interval.

      -   **producerId**: The internal identifier for this producer on this topic.

      -   **producerName**: The internal identifier for this producer, generated by the client library.

      -   **address**: The IP address and source port for the connection of this producer.

      -   **connectedSince**: The timestamp when this producer is created or reconnected last time.

  -   **subscriptions**: The list of all local subscriptions to the topic.

      -   **my-subscription**: The name of this subscription. It is defined by the client.

          -   **msgRateOut**: The total rate of messages (msg/s) delivered on this subscription.

          -   **msgThroughputOut**: The total throughput (bytes/s) delivered on this subscription.

          -   **msgBacklog**: The number of messages in the subscription backlog.

          -   **type**: The subscription type.

          -   **msgRateExpired**: The rate at which messages were discarded instead of dispatched from this subscription due to TTL.
          
          -   **lastExpireTimestamp**: The timestamp of the last message expire execution.
          
          -   **lastConsumedFlowTimestamp**: The timestamp of the last flow command received. 
          
          -   **lastConsumedTimestamp**: The latest timestamp of all the consumed timestamp of the consumers.
          
          -   **lastAckedTimestamp**: The latest timestamp of all the acked timestamp of the consumers.

          -   **consumers**: The list of connected consumers for this subscription.

                -   **msgRateOut**: The total rate of messages (msg/s) delivered to the consumer.

                -   **msgThroughputOut**: The total throughput (bytes/s) delivered to the consumer.

                -   **consumerName**: The internal identifier for this consumer, generated by the client library.

                -   **availablePermits**: The number of messages that the consumer has space for in the client library's listen queue. `0` means the client library's queue is full and `receive()` isn't being called. A non-zero value means this consumer is ready for dispatched messages.

                -   **unackedMessages**: The number of unacknowledged messages for the consumer.

                -   **blockedConsumerOnUnackedMsgs**: The flag used to verify if the consumer is blocked due to reaching threshold of the unacknowledged messages.
                
                -   **lastConsumedTimestamp**: The timestamp when the consumer reads a message the last time. 
          
                -   **lastAckedTimestamp**: The timestamp when the consumer acknowledges a message the last time.

  -   **replication**: This section gives the stats for cross-colo replication of this topic

      -   **msgRateIn**: The total rate (msg/s) of messages received from the remote cluster. 

      -   **msgThroughputIn**: The total throughput (bytes/s) received from the remote cluster.

      -   **msgRateOut**: The total rate of messages (msg/s) delivered to the replication-subscriber.

      -   **msgThroughputOut**: The total throughput (bytes/s) delivered to the replication-subscriber.

      -   **msgRateExpired**: The total rate of messages (msg/s) expired.

      -   **replicationBacklog**: The number of messages pending to be replicated to remote cluster.

      -   **connected**: Whether the outbound replicator is connected.

      -   **replicationDelayInSeconds**: How long the oldest message has been waiting to be sent through the connection, if connected is `true`.

      -   **inboundConnection**: The IP and port of the broker in the remote cluster's publisher connection to this broker.

      -   **inboundConnectedSince**: The TCP connection being used to publish messages to the remote cluster. If there are no local publishers connected, this connection is automatically closed after a minute.

      -   **outboundConnection**: The address of the outbound replication connection.

      -   **outboundConnectedSince**: The timestamp of establishing outbound connection.

The following is an example of a topic status.

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
To get the status of a topic, you can use the following ways.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
```shell
$ pulsar-admin topics stats \
  persistent://test-tenant/ns1/tp1 \
```

<!--REST API-->
{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace/:topic/stats|operation/getStats?version=[[pulsar:version_number]]}

<!--Java-->
```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
admin.topics().getStats(topic);
```

<!--END_DOCUSAURUS_CODE_TABS-->

### Get internal stats

You can get the detailed statistics of a topic.

  -   **entriesAddedCounter**: Messages published since this broker loaded this topic.

  -   **numberOfEntries**: The total number of messages being tracked.

  -   **totalSize**: The total storage size in bytes of all messages.

  -   **currentLedgerEntries**: The count of messages written to the ledger that is currently open for writing.

  -   **currentLedgerSize**: The size in bytes of messages written to the ledger that is currently open for writing.

  -   **lastLedgerCreatedTimestamp**: The time when the last ledger is created.

  -   **lastLedgerCreationFailureTimestamp:** The time when the last ledger failed.

  -   **waitingCursorsCount**: The number of cursors that are "caught up" and waiting for a new message to be published.

  -   **pendingAddEntriesCount**: The number of messages that complete (asynchronous) write requests.

  -   **lastConfirmedEntry**: The ledgerid:entryid of the last message that is written successfully. If the entryid is `-1`, then the ledger is open, yet no entries are written.

  -   **state**: The state of this ledger for writing. The state `LedgerOpened` means that a ledger is open for saving published messages.

  -   **ledgers**: The ordered list of all ledgers for this topic holding messages.

      -   **ledgerId**: The ID of this ledger.

      -   **entries**: The total number of entries belong to this ledger.

      -   **size**: The size of messages written to this ledger (in bytes).

      -   **offloaded**: Whether this ledger is offloaded.
      
      -   **metadata**: The ledger metadata.

  -   **schemaLedgers**: The ordered list of all ledgers for this topic schema.
  
      -   **ledgerId**: The ID of this ledger.
  
      -   **entries**: The total number of entries belong to this ledger.
  
      -   **size**: The size of messages written to this ledger (in bytes).
  
      -   **offloaded**: Whether this ledger is offloaded.
      
      -   **metadata**: The ledger metadata.

  -   **compactedLedger**: The ledgers holding un-acked messages after topic compaction.
 
      -   **ledgerId**: The ID of this ledger.
     
      -   **entries**: The total number of entries belong to this ledger.
     
      -   **size**: The size of messages written to this ledger (in bytes).
     
      -   **offloaded**: Whether this ledger is offloaded. The value is `false` for the compacted topic ledger.
      
  -   **cursors**: The list of all cursors on this topic. Each subscription in the topic stats has a cursor.

      -   **markDeletePosition**: All messages before the markDeletePosition are acknowledged by the subscriber.

      -   **readPosition**: The latest position of subscriber for reading message.

      -   **waitingReadOp**: This is true when the subscription has read the latest message published to the topic and is waiting for new messages to be published.

      -   **pendingReadOps**: The counter for how many outstanding read requests to the BookKeepers in progress.

      -   **messagesConsumedCounter**: The number of messages this cursor has acked since this broker loaded this topic.

      -   **cursorLedger**: The ledger being used to persistently store the current markDeletePosition.

      -   **cursorLedgerLastEntry**: The last entryid used to persistently store the current markDeletePosition.

      -   **individuallyDeletedMessages**: If acknowledges are being done out of order, the ranges of messages acknowledged between the markDeletePosition and the read-position shows.

      -   **lastLedgerSwitchTimestamp**: The last time the cursor ledger is rolled over.

      -   **state**: The state of the cursor ledger: `Open` means you have a cursor ledger for saving updates of the markDeletePosition.

The following is an example of the detailed statistics of a topic.

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
<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
```shell
$ pulsar-admin topics stats-internal \
  persistent://test-tenant/ns1/tp1 \
```

<!--REST API-->
{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace/:topic/internalStats|operation/getInternalStats?version=[[pulsar:version_number]]}

<!--Java-->
```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
admin.topics().getInternalStats(topic);
```

<!--END_DOCUSAURUS_CODE_TABS-->

### Peek messages

You can peek a number of messages for a specific subscription of a given topic in the following ways.
<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
```shell
$ pulsar-admin topics peek-messages \
  --count 10 --subscription my-subscription \
  persistent://test-tenant/ns1/tp1 \

Message ID: 315674752:0
Properties:  {  "X-Pulsar-publish-time" : "2015-07-13 17:40:28.451"  }
msg-payload
```

<!--REST API-->
{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace/:topic/subscription/:subName/position/:messagePosition|operation?version=[[pulsar:version_number]]/peekNthMessage}

<!--Java-->
```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
String subName = "my-subscription";
int numMessages = 1;
admin.topics().peekMessages(topic, subName, numMessages);
```

<!--END_DOCUSAURUS_CODE_TABS-->

### Get message by ID

You can fetch the message with the given ledger ID and entry ID in the following ways.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
```shell
$ ./bin/pulsar-admin topics get-message-by-id \
  persistent://public/default/my-topic \
  -l 10 -e 0
```

<!--REST API-->
{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace/:topic/ledger/:ledgerId/entry/:entryId|operation/getMessageById?version=[[pulsar:version_number]]}

<!--Java-->
```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
long ledgerId = 10;
long entryId = 10;
admin.topics().getMessageById(topic, ledgerId, entryId);
```

<!--END_DOCUSAURUS_CODE_TABS-->

### Skip messages

You can skip a number of messages for a specific subscription of a given topic in the following ways.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
```shell
$ pulsar-admin topics skip \
  --count 10 --subscription my-subscription \
  persistent://test-tenant/ns1/tp1 \
```

<!--REST API-->
{@inject: endpoint|POST|/admin/v2/:schema/:tenant/:namespace/:topic/subscription/:subName/skip/:numMessages|operation/skipMessages?version=[[pulsar:version_number]]}

<!--Java-->
```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
String subName = "my-subscription";
int numMessages = 1;
admin.topics().skipMessages(topic, subName, numMessages);
```

<!--END_DOCUSAURUS_CODE_TABS-->

### Skip all messages

You can skip all the old messages for a specific subscription of a given topic.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
```shell
$ pulsar-admin topics skip-all \
  --subscription my-subscription \
  persistent://test-tenant/ns1/tp1 \
```

<!--REST API-->
{@inject: endpoint|POST|/admin/v2/:schema/:tenant/:namespace/:topic/subscription/:subName/skip_all|operation/skipAllMessages?version=[[pulsar:version_number]]}

<!--Java-->
```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
String subName = "my-subscription";
admin.topics().skipAllMessages(topic, subName);
```

<!--END_DOCUSAURUS_CODE_TABS-->

### Reset cursor

You can reset a subscription cursor position back to the position which is recorded X minutes before. It essentially calculates time and position of cursor at X minutes before and resets it at that position. You can reset the cursor in the following ways.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
```shell
$ pulsar-admin topics reset-cursor \
  --subscription my-subscription --time 10 \
  persistent://test-tenant/ns1/tp1 \
```

<!--REST API-->
{@inject: endpoint|POST|/admin/v2/:schema/:tenant/:namespace/:topic/subscription/:subName/resetcursor/:timestamp|operation/resetCursor?version=[[pulsar:version_number]]}

<!--Java-->
```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
String subName = "my-subscription";
long timestamp = 2342343L;
admin.topics().skipAllMessages(topic, subName, timestamp);
```

<!--END_DOCUSAURUS_CODE_TABS-->

### Lookup of topic

You can locate the broker URL which is serving the given topic in the following ways.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
```shell
$ pulsar-admin topics lookup \
  persistent://test-tenant/ns1/tp1 \

 "pulsar://broker1.org.com:4480"
```

<!--REST API-->
{@inject: endpoint|GET|/lookup/v2/topic/:schema/:tenant:namespace/:topic|/?version=[[pulsar:version_number]]}

<!--Java-->
```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
admin.lookup().lookupDestination(topic);
```

<!--END_DOCUSAURUS_CODE_TABS-->

### Get bundle

You can check the range of the bundle which contains given topic in the following ways.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
```shell
$ pulsar-admin topics bundle-range \
  persistent://test-tenant/ns1/tp1 \

 "0x00000000_0xffffffff"
```

<!--REST API-->
{@inject: endpoint|GET|/lookup/v2/topic/:topic_domain/:tenant/:namespace/:topic/bundle|/?version=[[pulsar:version_number]]}

<!--Java-->
```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
admin.lookup().getBundleRange(topic);
```

<!--END_DOCUSAURUS_CODE_TABS-->

### Get subscriptions

You can check all subscription names for a given topic in the following ways.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
```shell
$ pulsar-admin topics subscriptions \
  persistent://test-tenant/ns1/tp1 \

 my-subscription
```

<!--REST API-->
{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace/:topic/subscriptions|operation/getSubscriptions?version=[[pulsar:version_number]]}

<!--Java-->
```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
admin.topics().getSubscriptions(topic);
```

<!--END_DOCUSAURUS_CODE_TABS-->

### Last Message Id

You can get the last committed message ID for a persistent topic. It is available since 2.3.0 release.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
```shell
pulsar-admin topics last-message-id topic-name
```

<!--REST API-->
{@inject: endpoint|Get|/admin/v2/:schema/:tenant/:namespace/:topic/lastMessageId?version=[[pulsar:version_number]]}

<!--Java-->
```Java
String topic = "persistent://my-tenant/my-namespace/my-topic";
admin.topics().getLastMessage(topic);
```

<!--END_DOCUSAURUS_CODE_TABS-->

## Manage non-partitioned topics
You can use Pulsar [admin API](admin-api-overview.md) to create, delete and check status of non-partitioned topics.

### Create
Non-partitioned topics must be explicitly created. When creating a new non-partitioned topic, you need to provide a name for the topic.

By default, 60 seconds after creation, topics are considered inactive and deleted automatically to avoid generating trash data. To disable this feature, set `brokerDeleteInactiveTopicsEnabled` to `false`. To change the frequency of checking inactive topics, set `brokerDeleteInactiveTopicsFrequencySeconds` to a specific value.

For more information about the two parameters, see [here](reference-configuration.md#broker).

You can create non-partitioned topics in the following ways.
<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
When you create non-partitioned topics with the [`create`](reference-pulsar-admin.md#create-3) command, you need to specify the topic name as an argument.

```shell
$ bin/pulsar-admin topics create \
  persistent://my-tenant/my-namespace/my-topic
```
> **Note**    
> When you create a non-partitioned topic with the suffix '-partition-' followed by numeric value like 'xyz-topic-partition-x' for the topic name, if a partitioned topic with same suffix 'xyz-topic-partition-y' exists, then the numeric value(x) for the non-partitioned topic must be larger than the number of partitions(y) of the partitioned topic. Otherwise, you cannot create such a non-partitioned topic. 

<!--REST API-->
{@inject: endpoint|PUT|/admin/v2/:schema/:tenant/:namespace/:topic|operation/createNonPartitionedTopic?version=[[pulsar:version_number]]}

<!--Java-->
```java
String topicName = "persistent://my-tenant/my-namespace/my-topic";
admin.topics().createNonPartitionedTopic(topicName);
```

<!--END_DOCUSAURUS_CODE_TABS-->

### Delete
You can delete non-partitioned topics in the following ways.
<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
```shell
$ bin/pulsar-admin topics delete \
  persistent://my-tenant/my-namespace/my-topic
```

<!--REST API-->
{@inject: endpoint|DELETE|/admin/v2/:schema/:tenant/:namespace/:topic|operation/deleteTopic?version=[[pulsar:version_number]]}

<!--Java-->
```java
admin.topics().delete(topic);
```

<!--END_DOCUSAURUS_CODE_TABS-->

### List

You can get the list of topics under a given namespace in the following ways.  
<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
```shell
$ pulsar-admin topics list tenant/namespace
persistent://tenant/namespace/topic1
persistent://tenant/namespace/topic2
```

<!--REST API-->
{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace|operation/getList?version=[[pulsar:version_number]]}

<!--Java-->
```java
admin.topics().getList(namespace);
```

<!--END_DOCUSAURUS_CODE_TABS-->

### Stats

You can check the current statistics of a given topic. The following is an example. For description of each stats, refer to [get stats](#get-stats).

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
You can check the current statistics of a given topic and its connected producers and consumers in the following ways.
<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
```shell
$ pulsar-admin topics stats \
  persistent://test-tenant/namespace/topic \
  --get-precise-backlog
```

<!--REST API-->
{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace/:topic/stats|operation/getStats?version=[[pulsar:version_number]]}

<!--Java-->
```java
admin.topics().getStats(topic, false /* is precise backlog */);
```
<!--END_DOCUSAURUS_CODE_TABS-->

## Manage partitioned topics
You can use Pulsar [admin API](admin-api-overview.md) to create, update, delete and check status of partitioned topics.

### Create

Partitioned topics must be explicitly created. When creating a new partitioned topic, you need to provide a name and the number of partitions for the topic.

By default, 60 seconds after creation, topics are considered inactive and deleted automatically to avoid generating trash data. To disable this feature, set `brokerDeleteInactiveTopicsEnabled` to `false`. To change the frequency of checking inactive topics, set `brokerDeleteInactiveTopicsFrequencySeconds` to a specific value.

For more information about the two parameters, see [here](reference-configuration.md#broker).

You can create partitioned topics in the following ways.
<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
When you create partitioned topics with the [`create-partitioned-topic`](reference-pulsar-admin.md#create-partitioned-topic)
command, you need to specify the topic name as an argument and the number of partitions using the `-p` or `--partitions` flag.

```shell
$ bin/pulsar-admin topics create-partitioned-topic \
  persistent://my-tenant/my-namespace/my-topic \
  --partitions 4
```

> **Note**    
> If a non-partitioned topic with the suffix '-partition-' followed by a numeric value like 'xyz-topic-partition-10', you can not create a partitioned topic with name 'xyz-topic', because the partitions of the partitioned topic could override the existing non-partitioned topic. To create such partitioned topic, you have to delete that non-partitioned topic first.

<!--REST API-->
{@inject: endpoint|PUT|/admin/v2/:schema/:tenant/:namespace/:topic/partitions|operation/createPartitionedTopic?version=[[pulsar:version_number]]}

<!--Java-->
```java
String topicName = "persistent://my-tenant/my-namespace/my-topic";
int numPartitions = 4;
admin.topics().createPartitionedTopic(topicName, numPartitions);
```

<!--END_DOCUSAURUS_CODE_TABS-->

### Create missed partitions

When topic auto-creation is disabled, and you have a partitioned topic without any partitions, you can use the [`create-missed-partitions`](reference-pulsar-admin.md#create-missed-partitions) command to create partitions for the topic.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
You can create missed partitions with the [`create-missed-partitions`](reference-pulsar-admin.md#create-missed-partitions) command and specify the topic name as an argument.

```shell
$ bin/pulsar-admin topics create-missed-partitions \
  persistent://my-tenant/my-namespace/my-topic \
```

<!--REST API-->
{@inject: endpoint|POST|/admin/v2/:schema/:tenant/:namespace/:topic|operation/createMissedPartitions?version=[[pulsar:version_number]]}

<!--Java-->
```java
String topicName = "persistent://my-tenant/my-namespace/my-topic";
admin.topics().createMissedPartitions(topicName);
```

<!--END_DOCUSAURUS_CODE_TABS-->

### Get metadata

Partitioned topics are associated with metadata, you can view it as a JSON object. The following metadata field is available.

Field | Description
:-----|:-------
`partitions` | The number of partitions into which the topic is divided.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
You can check the number of partitions in a partitioned topic with the [`get-partitioned-topic-metadata`](reference-pulsar-admin.md#get-partitioned-topic-metadata) subcommand. 

```shell
$ pulsar-admin topics get-partitioned-topic-metadata \
  persistent://my-tenant/my-namespace/my-topic
{
  "partitions": 4
}
```

<!--REST API-->
{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace/:topic/partitions|operation/getPartitionedMetadata?version=[[pulsar:version_number]]}

<!--Java-->
```java
String topicName = "persistent://my-tenant/my-namespace/my-topic";
admin.topics().getPartitionedTopicMetadata(topicName);
```

<!--END_DOCUSAURUS_CODE_TABS-->

### Update

You can update the number of partitions for an existing partitioned topic *if* the topic is non-global. However, you can only add the partition number. Decrementing the number of partitions would delete the topic, which is not supported in Pulsar.

Producers and consumers can find the newly created partitions automatically.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
You can update partitioned topics with the [`update-partitioned-topic`](reference-pulsar-admin.md#update-partitioned-topic) command.

```shell
$ pulsar-admin topics update-partitioned-topic \
  persistent://my-tenant/my-namespace/my-topic \
  --partitions 8
```

<!--REST API-->
{@inject: endpoint|POST|/admin/v2/:schema/:tenant/:cluster/:namespace/:destination/partitions|operation/updatePartitionedTopic?version=[[pulsar:version_number]]}

<!--Java-->
```java
admin.topics().updatePartitionedTopic(topic, numPartitions);
```

<!--END_DOCUSAURUS_CODE_TABS-->

### Delete
You can delete partitioned topics with the [`delete-partitioned-topic`](reference-pulsar-admin.md#delete-partitioned-topic) command, REST API and Java. 

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
```shell
$ bin/pulsar-admin topics delete-partitioned-topic \
  persistent://my-tenant/my-namespace/my-topic
```

<!--REST API-->
{@inject: endpoint|DELETE|/admin/v2/:schema/:topic/:namespace/:destination/partitions|operation/deletePartitionedTopic?version=[[pulsar:version_number]]}

<!--Java-->
```java
admin.topics().delete(topic);
```

<!--END_DOCUSAURUS_CODE_TABS-->

### List
You can get the list of partitioned topics under a given namespace in the following ways.  
<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
```shell
$ pulsar-admin topics list-partitioned-topics tenant/namespace
persistent://tenant/namespace/topic1
persistent://tenant/namespace/topic2
```

<!--REST API-->
{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace|operation/getPartitionedTopicList?version=[[pulsar:version_number]]}

<!--Java-->
```java
admin.topics().getPartitionedTopicList(namespace);
```

<!--END_DOCUSAURUS_CODE_TABS-->

### Stats

You can check the current statistics of a given partitioned topic. The following is an example. For description of each stats, refer to [get stats](#get-stats).

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

You can check the current statistics of a given partitioned topic and its connected producers and consumers in the following ways. 

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
```shell
$ pulsar-admin topics partitioned-stats \
  persistent://test-tenant/namespace/topic \
  --per-partition
```

<!--REST API-->
{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace/:topic/partitioned-stats|operation/getPartitionedStats?version=[[pulsar:version_number]]}

<!--Java-->
```java
admin.topics().getPartitionedStats(topic, true /* per partition */, false /* is precise backlog */);
```

<!--END_DOCUSAURUS_CODE_TABS-->

### Internal stats

You can check the detailed statistics of a topic. The following is an example. For description of each stats, refer to [get internal stats](#get-internal-stats).

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

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
```shell
$ pulsar-admin topics stats-internal \
  persistent://test-tenant/namespace/topic
```

<!--REST API-->
{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace/:topic/internalStats|operation/getInternalStats?version=[[pulsar:version_number]]}

<!--Java-->
```java
admin.topics().getInternalStats(topic);
```

<!--END_DOCUSAURUS_CODE_TABS-->

## Publish to partitioned topics

By default, Pulsar topics are served by a single broker, which limits the maximum throughput of a topic. *Partitioned topics* can span multiple brokers and thus allow for higher throughput. 

You can publish to partitioned topics using Pulsar client libraries. When publishing to partitioned topics, you must specify a routing mode. If you do not specify any routing mode when you create a new producer, the round robin routing mode is used. 

### Routing mode

You can specify the routing mode in the ProducerConfiguration object that you use to configure your producer. The routing mode determines which partition(internal topic) that each message should be published to.

The following {@inject: javadoc:MessageRoutingMode:/client/org/apache/pulsar/client/api/MessageRoutingMode} options are available.

Mode     | Description 
:--------|:------------
`RoundRobinPartition` | If no key is provided, the producer publishes messages across all partitions in round-robin policy to achieve the maximum throughput. Round-robin is not done per individual message, round-robin is set to the same boundary of batching delay to ensure that batching is effective. If a key is specified on the message, the partitioned producer hashes the key and assigns message to a particular partition. This is the default mode. 
`SinglePartition`     | If no key is provided, the producer picks a single partition randomly and publishes all messages into that partition. If a key is specified on the message, the partitioned producer hashes the key and assigns message to a particular partition.
`CustomPartition`     | Use custom message router implementation that is called to determine the partition for a particular message. You can create a custom routing mode by using the Java client and implementing the {@inject: javadoc:MessageRouter:/client/org/apache/pulsar/client/api/MessageRouter} interface.

The following is an example:

```java
String pulsarBrokerRootUrl = "pulsar://localhost:6650";
String topic = "persistent://my-tenant/my-namespace/my-topic";

PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(pulsarBrokerRootUrl).build();
Producer<byte[]> producer = pulsarClient.newProducer()
        .topic(topic)
        .messageRoutingMode(MessageRoutingMode.SinglePartition)
        .create();
producer.send("Partitioned topic message".getBytes());
```

### Custom message router

To use a custom message router, you need to provide an implementation of the {@inject: javadoc:MessageRouter:/client/org/apache/pulsar/client/api/MessageRouter} interface, which has just one `choosePartition` method:

```java
public interface MessageRouter extends Serializable {
    int choosePartition(Message msg);
}
```

The following router routes every message to partition 10:

```java
public class AlwaysTenRouter implements MessageRouter {
    public int choosePartition(Message msg) {
        return 10;
    }
}
```

With that implementation, you can send

```java
String pulsarBrokerRootUrl = "pulsar://localhost:6650";
String topic = "persistent://my-tenant/my-cluster-my-namespace/my-topic";

PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(pulsarBrokerRootUrl).build();
Producer<byte[]> producer = pulsarClient.newProducer()
        .topic(topic)
        .messageRouter(new AlwaysTenRouter())
        .create();
producer.send("Partitioned topic message".getBytes());
```

### How to choose partitions when using a key
If a message has a key, it supersedes the round robin routing policy. The following example illustrates how to choose the partition when using a key.

```java
// If the message has a key, it supersedes the round robin routing policy
        if (msg.hasKey()) {
            return signSafeMod(hash.makeHash(msg.getKey()), topicMetadata.numPartitions());
        }

        if (isBatchingEnabled) { // if batching is enabled, choose partition on `partitionSwitchMs` boundary.
            long currentMs = clock.millis();
            return signSafeMod(currentMs / partitionSwitchMs + startPtnIdx, topicMetadata.numPartitions());
        } else {
            return signSafeMod(PARTITION_INDEX_UPDATER.getAndIncrement(this), topicMetadata.numPartitions());
        }
``` 

## Manage subscriptions
You can use [Pulsar admin API](admin-api-overview.md) to create, check, and delete subscriptions.
### Create subscription
You can create a subscription for a topic using one of the following methods.
<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
```shell
pulsar-admin topics create-subscription \
--subscription my-subscription \
persistent://test-tenant/ns1/tp1
```
<!--REST API-->
{@inject: endpoint|PUT|/admin/v2/persistent/:tenant/:namespace/:topic/subscription/:subscription|operation/createSubscriptions?version=[[pulsar:version_number]]}
<!--Java-->
```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
String subscriptionName = "my-subscription";
admin.topics().createSubscription(topic, subscriptionName, MessageId.latest);
```
<!--END_DOCUSAURUS_CODE_TABS-->
### Get subscription
You can check all subscription names for a given topic using one of the following methods.
<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
```shell
pulsar-admin topics subscriptions \
persistent://test-tenant/ns1/tp1 \
my-subscription
```
<!--REST API-->
{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace/:topic/subscriptions|operation/getSubscriptions?version=[[pulsar:version_number]]}
<!--Java-->
```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
admin.topics().getSubscriptions(topic);
```
<!--END_DOCUSAURUS_CODE_TABS-->
### Unsubscribe subscription 
When a subscription does not process messages any more, you can unsubscribe it using one of the following methods. 
<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
```shell
pulsar-admin topics unsubscribe \
--subscription my-subscription \
persistent://test-tenant/ns1/tp1 
```
<!--REST API-->
{@inject: endpoint|DELETE|/admin/v2/namespaces/:tenant/:namespace/:topic/subscription/:subscription|operation/deleteSubscription?version=[[pulsar:version_number]]}
<!--Java-->
```java
String topic = "persistent://my-tenant/my-namespace/my-topic";
String subscriptionName = "my-subscription";
admin.topics().deleteSubscription(topic, subscriptionName);
```
<!--END_DOCUSAURUS_CODE_TABS-->