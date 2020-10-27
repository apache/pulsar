---
id: admin-api-partitioned-topics
title: Manage topics
sidebar_label: Topics
---

## Manage partitioned topics
You can use Pulsar [admin API](admin-api-overview.md) to create, update, delete and check status of partitioned topics.

### Create

Partitioned topics must be explicitly created. When creating a new partitioned topic, you need to provide a name and the number of partitions for the topic.

By default, after 60 seconds of creation, topics are considered inactive and deleted automatically to prevent from generating trash data. To disable this feature, set `brokerDeleteInactiveTopicsEnabled`  to `false`. To change the frequency of checking inactive topics, set `brokerDeleteInactiveTopicsFrequencySeconds` to your desired value.

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
> If a non-partitioned topic with the suffix '-partition-' followed by numeric value like 'xyz-topic-partition-10', then you can not create a partitioned topic with name 'xyz-topic', because the partitions of the partitioned topic could override the existing non-partitioned topic. You have to delete that non-partitioned topic first, and then create the partitioned topic.

<!--REST API-->
{@inject: endpoint|PUT|/admin/v2/persistent/:tenant/:namespace/:topic/partitions|operation/createPartitionedTopic}

<!--Java-->
```java
String topicName = "persistent://my-tenant/my-namespace/my-topic";
int numPartitions = 4;
admin.persistentTopics().createPartitionedTopic(topicName, numPartitions);
```

<!--END_DOCUSAURUS_CODE_TABS-->

### Create missed partitions

You can create partitions for partitioned topics. It can be used to repair partitions when topic auto creation is disabled.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
You can create missed partitions with the [`create-missed-partitions`](reference-pulsar-admin.md#create-missed-partitions) command and specify the topic name as an argument.

```shell
$ bin/pulsar-admin topics create-missed-partitions \
  persistent://my-tenant/my-namespace/my-topic \
```

<!--REST API-->
{@inject: endpoint|POST|/admin/v2/persistent/:tenant/:namespace/:topic|operation/createMissedPartitions}

<!--Java-->
```java
String topicName = "persistent://my-tenant/my-namespace/my-topic";
admin.persistentTopics().createMissedPartitions(topicName);
```

<!--END_DOCUSAURUS_CODE_TABS-->

### Get metadata

Partitioned topics are associated with metadata, you can view it as a JSON object. The following metadata field is available.

Field | Description
:-----|:-------
`partitions` | The number of partitions into which the topic is divided.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
You can view the number of partitions in a partitioned topic with the [`get-partitioned-topic-metadata`](reference-pulsar-admin.md#get-partitioned-topic-metadata) subcommand. 

```shell
$ pulsar-admin topics get-partitioned-topic-metadata \
  persistent://my-tenant/my-namespace/my-topic
{
  "partitions": 4
}
```
<!--REST API-->
{@inject: endpoint|GET|/admin/v2/persistent/:tenant/:namespace/:topic/partitions|operation/getPartitionedMetadata}

<!--Java-->
```java
String topicName = "persistent://my-tenant/my-namespace/my-topic";
admin.persistentTopics().getPartitionedTopicMetadata(topicName);
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
{@inject: endpoint|POST|/admin/v2/persistent/:tenant/:cluster/:namespace/:destination/partitions|operation/updatePartitionedTopic}

<!--Java-->
```java
admin.persistentTopics().updatePartitionedTopic(persistentTopic, numPartitions);
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
{@inject: endpoint|DELETE|/admin/v2/persistent/:topic/:namespace/:destination/partitions|operation/deletePartitionedTopic}

<!--Java-->
```java
admin.persistentTopics().delete(persistentTopic);
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
{@inject: endpoint|GET|/admin/v2/persistent/:tenant/:namespace|operation/getPartitionedTopicList}

<!--Java-->
```java
admin.persistentTopics().getList(namespace);
```

<!--END_DOCUSAURUS_CODE_TABS-->

### Stats

You can view the current statistics of a given partitioned topic. The following is an example payload. For description of each stats, refer to [get stats](#get-stats).

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

You can get the stats for the partitioned topic and its connected producers and consumers in the following ways.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
```shell
$ pulsar-admin topics partitioned-stats \
  persistent://test-tenant/namespace/topic \
  --per-partition
```

<!--REST API-->
{@inject: endpoint|GET|/admin/v2/persistent/:tenant/:namespace/:topic/partitioned-stats|operation/getPartitionedStats}

<!--Java-->
```java
admin.topics().getPartitionedStats(persistentTopic, true /* per partition */, false /* is precise backlog */);
```

<!--END_DOCUSAURUS_CODE_TABS-->

### Internal stats

You can view the detailed statistics of a topic. The following is an example. For description of each stats, refer to [get internal stats](#get-internal-stats).

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
{@inject: endpoint|GET|/admin/v2/persistent/:tenant/:namespace/:topic/internalStats|operation/getInternalStats}

<!--Java-->
```java
admin.persistentTopics().getInternalStats(persistentTopic);
```

<!--END_DOCUSAURUS_CODE_TABS-->

## Manage non-partitioned topics
You can use Pulsar's [admin API](admin-api-overview.md) to create and manage non-partitioned topics.

In all of the instructions and commands below, the topic name structure is:

```shell
persistent://tenant/namespace/topic
```

## Non-Partitioned topics resources

### Create

Non-partitioned topics in Pulsar must be explicitly created. When creating a new non-partitioned topic you
need to provide a name for the topic.

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

You can create non-partitioned topics using the [`create`](reference-pulsar-admin.md#create-3)
command and specifying the topic name as an argument.
Here's an example:

```shell
$ bin/pulsar-admin topics create \
  persistent://my-tenant/my-namespace/my-topic
```

> #### Note
>
> It's only allowed to create non partitioned topic of name contains suffix '-partition-' followed by numeric value like
> 'xyz-topic-partition-10', if there's already a partitioned topic with same name, in this case 'xyz-topic', and has
> number of partition larger then that numeric value in this case 11(partition index is start from 0). Else creation of such topic will fail.

#### REST API

{@inject: endpoint|PUT|/admin/v2/persistent/:tenant/:namespace/:topic|operation/createNonPartitionedTopic}

#### Java

```java
String topicName = "persistent://my-tenant/my-namespace/my-topic";
admin.topics().createNonPartitionedTopic(topicName);
```

### Delete

#### pulsar-admin

Non-partitioned topics can be deleted using the
[`delete`](reference-pulsar-admin.md#delete-4) command, specifying the topic by name:

```shell
$ bin/pulsar-admin topics delete \
  persistent://my-tenant/my-namespace/my-topic
```

#### REST API

{@inject: endpoint|DELETE|/admin/v2/persistent/:tenant/:namespace/:topic|operation/deleteTopic}

#### Java

```java
admin.topics().delete(persistentTopic);
```

### List

It provides a list of topics existing under a given namespace.  

#### pulsar-admin

```shell
$ pulsar-admin topics list tenant/namespace
persistent://tenant/namespace/topic1
persistent://tenant/namespace/topic2
```

#### REST API

{@inject: endpoint|GET|/admin/v2/persistent/:tenant/:namespace|operation/getList}

#### Java

```java
admin.topics().getList(namespace);
```

### Stats

It shows current statistics of a given topic. Here's an example payload:

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
|msgBacklogNoDelayed|The count of messages in backlog without delayed messages for this subscription|
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

The stats for the topic and its connected producers and consumers can be fetched by using the
[`stats`](reference-pulsar-admin.md#stats) command, specifying the topic by name:

```shell
$ pulsar-admin topics stats \
  persistent://test-tenant/namespace/topic \
  --get-precise-backlog
```

#### REST API

{@inject: endpoint|GET|/admin/v2/persistent/:tenant/:namespace/:topic/stats|operation/getStats}

#### Java

```java
admin.topics().getStats(persistentTopic, false /* is precise backlog */);
```
