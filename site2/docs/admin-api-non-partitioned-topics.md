---
id: admin-api-non-partitioned-topics
title: Managing non-partitioned topics
sidebar_label: Non-Partitioned topics
---

## Manage non-partitioned topics
You can use Pulsar [admin API](admin-api-overview.md) to create, delete and check status of non-partitioned topics.

### Create
Non-partitioned topics must be explicitly created. When creating a new non-partitioned topic, you need to provide a name for the topic.

By default, after 60 seconds of creation, topics are considered inactive and deleted automatically to prevent from generating trash data. To disable this feature, set `brokerDeleteInactiveTopicsEnabled`  to `false`. To change the frequency of checking inactive topics, set `brokerDeleteInactiveTopicsFrequencySeconds` to your desired value.

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
> When you create a non-partitioned topic with the suffix '-partition-' followed by numeric value like 'xyz-topic-partition-x' for the topic name, if a partitioned topic with same name, in this case 'xyz-topic-partition-y' exists, then the numeric value(x) for the non-partitioned topic must be larger than the number of partitions(y) of the partitioned topic. Otherwise, you cannot create such non-partitioned topic. 

<!--REST API-->
{@inject: endpoint|PUT|/admin/v2/persistent/:tenant/:namespace/:topic|operation/createNonPartitionedTopic}

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
{@inject: endpoint|DELETE|/admin/v2/persistent/:tenant/:namespace/:topic|operation/deleteTopic}

<!--Java-->
```java
admin.topics().delete(persistentTopic);
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
{@inject: endpoint|GET|/admin/v2/persistent/:tenant/:namespace|operation/getList}

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
{@inject: endpoint|GET|/admin/v2/persistent/:tenant/:namespace/:topic/stats|operation/getStats}

<!--Java-->
```java
admin.topics().getStats(persistentTopic, false /* is precise backlog */);
```
<!--END_DOCUSAURUS_CODE_TABS-->