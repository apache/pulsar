---
id: admin-api-partitioned-topics
title: Managing partitioned topics
sidebar_label: Topics1
---

## Manage partitioned topics
You can use Pulsar [admin API](admin-api-overview.md) to create, update, delete and check status of partitioned topics.

### Create

Partitioned topics must be explicitly created. When creating a new partitioned topic, you need to provide a name and the number of partitions for the topic.

By default, after 60 seconds of creation, topics are considered inactive and deleted automatically to prevent from generating trash data. To disable this feature, set `brokerDeleteInactiveTopicsEnabled` to `false`. To change the frequency of checking inactive topics, set `brokerDeleteInactiveTopicsFrequencySeconds` to your desired value.

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
> If a non-partitioned topic with the suffix '-partition-' followed by numeric value like 'xyz-topic-partition-10', then you can not create a partitioned topic with name 'xyz-topic', because the partitions of the partitioned topic could override the existing non-partitioned topic. To create such partitioned topic, you have to delete that non-partitioned topic first.

<!--REST API-->
{@inject: endpoint|PUT|/admin/v2/topics/:tenant/:namespace/:topic/partitions|operation/createPartitionedTopic}

<!--Java-->
```java
String topicName = "persistent://my-tenant/my-namespace/my-topic";
int numPartitions = 4;
admin.topics().createPartitionedTopic(topicName, numPartitions);
```

<!--END_DOCUSAURUS_CODE_TABS-->

### Create missed partitions

When topic auto creation is disabled, and you have a partitioned topic without any partitions, you can use the [`create-missed-partitions`](reference-pulsar-admin.md#create-missed-partitions) command to create partitions for the topic.

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
You can create missed partitions with the [`create-missed-partitions`](reference-pulsar-admin.md#create-missed-partitions) command and specify the topic name as an argument.

```shell
$ bin/pulsar-admin topics create-missed-partitions \
  persistent://my-tenant/my-namespace/my-topic \
```

<!--REST API-->
{@inject: endpoint|POST|/admin/v2/topics/:tenant/:namespace/:topic|operation/createMissedPartitions}

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
You can view the number of partitions in a partitioned topic with the [`get-partitioned-topic-metadata`](reference-pulsar-admin.md#get-partitioned-topic-metadata) subcommand. 

```shell
$ pulsar-admin topics get-partitioned-topic-metadata \
  persistent://my-tenant/my-namespace/my-topic
{
  "partitions": 4
}
```

<!--REST API-->
{@inject: endpoint|GET|/admin/v2/topics/:tenant/:namespace/:topic/partitions|operation/getPartitionedMetadata}

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
{@inject: endpoint|POST|/admin/v2/topics/:tenant/:cluster/:namespace/:destination/partitions|operation/updatePartitionedTopic}

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
{@inject: endpoint|DELETE|/admin/v2/topics/:topic/:namespace/:destination/partitions|operation/deletePartitionedTopic}

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
{@inject: endpoint|GET|/admin/v2/topics/:tenant/:namespace|operation/getPartitionedTopicList}

<!--Java-->
```java
admin.topics().getList(namespace);
```

<!--END_DOCUSAURUS_CODE_TABS-->

### Stats

You can view the current statistics of a given partitioned topic. The following is an example. For description of each stats, refer to [get stats](#get-stats).

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

You can view the current statistics of a given partitioned topic and its connected producers and consumers in the following ways. 

<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
```shell
$ pulsar-admin topics partitioned-stats \
  persistent://test-tenant/namespace/topic \
  --per-partition
```

<!--REST API-->
{@inject: endpoint|GET|/admin/v2/topics/:tenant/:namespace/:topic/partitioned-stats|operation/getPartitionedStats}

<!--Java-->
```java
admin.topics().getPartitionedStats(topic, true /* per partition */, false /* is precise backlog */);
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
{@inject: endpoint|GET|/admin/v2/topics/:tenant/:namespace/:topic/internalStats|operation/getInternalStats}

<!--Java-->
```java
admin.topics().getInternalStats(topic);
```

<!--END_DOCUSAURUS_CODE_TABS-->

## Manage non-partitioned topics
You can use Pulsar [admin API](admin-api-overview.md) to create, delete and check status of non-partitioned topics.

### Create
Non-partitioned topics must be explicitly created. When creating a new non-partitioned topic, you need to provide a name for the topic.

By default, after 60 seconds of creation, topics are considered inactive and deleted automatically to prevent from generating trash data. To disable this feature, set `brokerDeleteInactiveTopicsEnabled` to `false`. To change the frequency of checking inactive topics, set `brokerDeleteInactiveTopicsFrequencySeconds` to your desired value.

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
{@inject: endpoint|PUT|/admin/v2/topics/:tenant/:namespace/:topic|operation/createNonPartitionedTopic}

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
{@inject: endpoint|DELETE|/admin/v2/topics/:tenant/:namespace/:topic|operation/deleteTopic}

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
{@inject: endpoint|GET|/admin/v2/topics/:tenant/:namespace|operation/getList}

<!--Java-->
```java
admin.topics().getList(namespace);
```

<!--END_DOCUSAURUS_CODE_TABS-->

### Stats

You can view the current statistics of a given topic. The following is an example. For description of each stats, refer to [get stats](#get-stats).

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
You can view the current statistics of a given topic and its connected producers and consumers in the following ways.
<!--DOCUSAURUS_CODE_TABS-->
<!--pulsar-admin-->
```shell
$ pulsar-admin topics stats \
  persistent://test-tenant/namespace/topic \
  --get-precise-backlog
```

<!--REST API-->
{@inject: endpoint|GET|/admin/v2/topics/:tenant/:namespace/:topic/stats|operation/getStats}

<!--Java-->
```java
admin.topics().getStats(topic, false /* is precise backlog */);
```
<!--END_DOCUSAURUS_CODE_TABS-->