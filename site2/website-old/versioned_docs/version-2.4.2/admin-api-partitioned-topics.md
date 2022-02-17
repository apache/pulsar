---
id: version-2.4.2-admin-api-partitioned-topics
title: Managing partitioned topics
sidebar_label: Partitioned topics
original_id: admin-api-partitioned-topics
---


You can use Pulsar's [admin API](admin-api-overview.md) to create and manage partitioned topics.

In all of the instructions and commands below, the topic name structure is:

```shell
persistent://tenant/namespace/topic
```

## Partitioned topics resources

### Create

Partitioned topics in Pulsar must be explicitly created. When creating a new partitioned topic you
need to provide a name for the topic as well as the desired number of partitions.

> #### Note
>
> By default, after 60 seconds of creation, topics are considered inactive and deleted automatically to prevent from generating trash data.
>
> To disable this feature, set `brokerDeleteInactiveTopicsEnabled`  to `false`.
>
> To change the frequency of checking inactive topics, set `brokerDeleteInactiveTopicsFrequencySeconds` to your desired value.
>
> For more information about these two parameters, see [here](reference-configuration.md#broker).

#### pulsar-admin

You can create partitioned topics using the [`create-partitioned-topic`](reference-pulsar-admin.md#create-partitioned-topic)
command and specifying the topic name as an argument and the number of partitions using the `-p` or `--partitions` flag.
Here's an example:

```shell
$ bin/pulsar-admin topics create-partitioned-topic \
  persistent://my-tenant/my-namespace/my-topic \
  --partitions 4
```

#### REST API

{@inject: endpoint|PUT|/admin/v2/:schema/:tenant/:namespace/:topic/partitions|operation/createPartitionedTopic?version=[[pulsar:version_number]]}

#### Java

```java
String topicName = "persistent://my-tenant/my-namespace/my-topic";
int numPartitions = 4;
admin.persistentTopics().createPartitionedTopic(topicName, numPartitions);
```

## Nonpartitioned topics resources

### Create

Nonpartitioned topics in Pulsar must be explicitly created if allowAutoTopicCreation or createIfMissing is disabled.
When creating a non-partitioned topic, you need to provide a topic name.

> #### Note
>
> By default, after 60 seconds of creation, topics are considered inactive and deleted automatically to prevent from generating trash data.
>
> To disable this feature, set `brokerDeleteInactiveTopicsEnabled` to `false`.
>
> To change the frequency of checking inactive topics, set `brokerDeleteInactiveTopicsFrequencySeconds` to your desired value.
>
> For more information about these two parameters, see [here](reference-configuration.md#broker).

#### pulsar-admin

You can create non-partitioned topics using the [`create`](reference-pulsar-admin.md#create)
command and specifying the topic name as an argument. This is an example command:

```shell
$ bin/pulsar-admin topics create persistent://my-tenant/my-namespace/my-topic
``` 

#### REST API

{@inject: endpoint|PUT|admin/v2/:schema/:tenant/:namespace/:topic|operation/createNonPartitionedTopic?version=[[pulsar:version_number]]}

#### Java

```java
String topicName = "persistent://my-tenant/my-namespace/my-topic";
admin.topics().createNonPartitionedTopic(topicName);
```

### Get metadata

Partitioned topics have metadata associated with them that you can fetch as a JSON object.
The following metadata fields are currently available:

Field | Meaning
:-----|:-------
`partitions` | The number of partitions into which the topic is divided

#### pulsar-admin

You can see the number of partitions in a partitioned topic using the
[`get-partitioned-topic-metadata`](reference-pulsar-admin.md#get-partitioned-topic-metadata)
subcommand. Here's an example:

```shell
$ pulsar-admin topics get-partitioned-topic-metadata \
  persistent://my-tenant/my-namespace/my-topic
{
  "partitions": 4
}
```

#### REST API

{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace/:topic/partitions|operation/getPartitionedMetadata?version=[[pulsar:version_number]]}

#### Java

```java
String topicName = "persistent://my-tenant/my-namespace/my-topic";
admin.persistentTopics().getPartitionedTopicMetadata(topicName);
```

### Update

You can update the number of partitions on an existing partitioned topic
*if* the topic is non-global. To update, the new number of partitions must be greater
than the existing number.

Decrementing the number of partitions would deleting the topic, which is not supported in Pulsar.

Already created partitioned producers and consumers will automatically find the newly created partitions.

#### pulsar-admin

Partitioned topics can be updated using the
[`update-partitioned-topic`](reference-pulsar-admin.md#update-partitioned-topic) command.

```shell
$ pulsar-admin topics update-partitioned-topic \
  persistent://my-tenant/my-namespace/my-topic \
  --partitions 8
```

#### REST API

{@inject: endpoint|POST|/admin/v2/:schema/:tenant/:cluster/:namespace/:destination/partitions|operation/updatePartitionedTopic?version=[[pulsar:version_number]]}

#### Java

```java
admin.persistentTopics().updatePartitionedTopic(persistentTopic, numPartitions);
```

### Delete

#### pulsar-admin

Partitioned topics can be deleted using the
[`delete-partitioned-topic`](reference-pulsar-admin.md#delete-partitioned-topic) command, specifying the topic by name:

```shell
$ bin/pulsar-admin topics delete-partitioned-topic \
  persistent://my-tenant/my-namespace/my-topic
```

#### REST API

{@inject: endpoint|DELETE|/admin/v2/:schema/:topic/:namespace/:destination/partitions|operation/deletePartitionedTopic?version=[[pulsar:version_number]]}

#### Java

```java
admin.persistentTopics().delete(persistentTopic);
```

### List

It provides a list of persistent topics existing under a given namespace.  

#### pulsar-admin

```shell
$ pulsar-admin topics list tenant/namespace
persistent://tenant/namespace/topic1
persistent://tenant/namespace/topic2
```

#### REST API

{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace|operation/getPartitionedTopicList?version=[[pulsar:version_number]]}

#### Java

```java
admin.persistentTopics().getList(namespace);
```

### Stats

It shows current statistics of a given partitioned topic. Here's an example payload:

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

The following stats are available:

|Stat|Description|
|----|-----------|
|msgRateIn|The sum of all local and replication publishers’ publish rates in messages per second|
|msgThroughputIn|Same as msgRateIn but in bytes per second instead of messages per second|
|msgRateOut|The sum of all local and replication consumers’ dispatch rates in messages per second|
|msgThroughputOut|Same as msgRateOut but in bytes per second instead of messages per second|
|averageMsgSize|Average message size, in bytes, from this publisher within the last interval|
|storageSize|The sum of the ledgers’ storage size for this topic|
|publishers|The list of all local publishers into the topic. There can be anywhere from zero to thousands.|
|producerId|Internal identifier for this producer on this topic|
|producerName|Internal identifier for this producer, generated by the client library|
|address|IP address and source port for the connection of this producer|
|connectedSince|Timestamp this producer was created or last reconnected|
|subscriptions|The list of all local subscriptions to the topic|
|my-subscription|The name of this subscription (client defined)|
|msgBacklog|The count of messages in backlog for this subscription|
|type|This subscription type|
|msgRateExpired|The rate at which messages were discarded instead of dispatched from this subscription due to TTL|
|consumers|The list of connected consumers for this subscription|
|consumerName|Internal identifier for this consumer, generated by the client library|
|availablePermits|The number of messages this consumer has space for in the client library’s listen queue. A value of 0 means the client library’s queue is full and receive() isn’t being called. A nonzero value means this consumer is ready to be dispatched messages.|
|replication|This section gives the stats for cross-colo replication of this topic|
|replicationBacklog|The outbound replication backlog in messages|
|connected|Whether the outbound replicator is connected|
|replicationDelayInSeconds|How long the oldest message has been waiting to be sent through the connection, if connected is true|
|inboundConnection|The IP and port of the broker in the remote cluster’s publisher connection to this broker|
|inboundConnectedSince|The TCP connection being used to publish messages to the remote cluster. If there are no local publishers connected, this connection is automatically closed after a minute.|

#### pulsar-admin

The stats for the partitioned topic and its connected producers and consumers can be fetched by using the
[`partitioned-stats`](reference-pulsar-admin.md#partitioned-stats) command, specifying the topic by name:

```shell
$ pulsar-admin topics partitioned-stats \
  persistent://test-tenant/namespace/topic \
  --per-partition        
```

#### REST API

{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace/:topic/partitioned-stats|operation/getPartitionedStats?version=[[pulsar:version_number]]}

#### Java

```java
admin.persistentTopics().getStats(persistentTopic);
```

### Internal stats

It shows detailed statistics of a topic.

|Stat|Description|
|----|-----------|
|entriesAddedCounter|Messages published since this broker loaded this topic|
|numberOfEntries|Total number of messages being tracked|
|totalSize|Total storage size in bytes of all messages|
|currentLedgerEntries|Count of messages written to the ledger currently open for writing|
|currentLedgerSize|Size in bytes of messages written to ledger currently open for writing|
|lastLedgerCreatedTimestamp|Time when last ledger was created|
|lastLedgerCreationFailureTimestamp|time when last ledger was failed|
|waitingCursorsCount|How many cursors are caught up and waiting for a new message to be published|
|pendingAddEntriesCount|How many messages have (asynchronous) write requests we are waiting on completion|
|lastConfirmedEntry|The ledgerid:entryid of the last message successfully written. If the entryid is -1, then the ledger has been opened or is currently being opened but has no entries written yet.|
|state|The state of the cursor ledger. Open means we have a cursor ledger for saving updates of the markDeletePosition.|
|ledgers|The ordered list of all ledgers for this topic holding its messages|
|cursors|The list of all cursors on this topic. There will be one for every subscription you saw in the topic stats.|
|markDeletePosition|The ack position: the last message the subscriber acknowledged receiving|
|readPosition|The latest position of subscriber for reading message|
|waitingReadOp|This is true when the subscription has read the latest message published to the topic and is waiting on new messages to be published.|
|pendingReadOps|The counter for how many outstanding read requests to the BookKeepers we have in progress|
|messagesConsumedCounter|Number of messages this cursor has acked since this broker loaded this topic|
|cursorLedger|The ledger being used to persistently store the current markDeletePosition|
|cursorLedgerLastEntry|The last entryid used to persistently store the current markDeletePosition|
|individuallyDeletedMessages|If Acks are being done out of order, shows the ranges of messages Acked between the markDeletePosition and the read-position|
|lastLedgerSwitchTimestamp|The last time the cursor ledger was rolled over|


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

The internal stats for the partitioned topic can be fetched by using the
[`stats-internal`](reference-pulsar-admin.md#stats-internal) command, specifying the topic by name:

```shell
$ pulsar-admin topics stats-internal \
  persistent://test-tenant/namespace/topic
```

#### REST API

{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace/:topic/internalStats|operation/getInternalStats?version=[[pulsar:version_number]]}

#### Java

```java
admin.persistentTopics().getInternalStats(persistentTopic);
```
