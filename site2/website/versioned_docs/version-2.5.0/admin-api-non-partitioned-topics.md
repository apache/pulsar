---
id: version-2.5.0-admin-api-non-partitioned-topics
title: Managing non-partitioned topics
sidebar_label: Non-Partitioned topics
original_id: admin-api-non-partitioned-topics
---


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

{@inject: endpoint|PUT|/admin/v2/:schema/:tenant/:namespace/:topic|operation/createNonPartitionedTopic?version=[[pulsar:version_number]]}

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

{@inject: endpoint|DELETE|/admin/v2/:schema/:tenant/:namespace/:topic|operation/deleteTopic?version=[[pulsar:version_number]]}

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

{@inject: endpoint|GET|/admin/v2/:schema/:tenant/:namespace|operation/getList?version=[[pulsar:version_number]]}

#### Java

```java
admin.topics().getList(namespace);
```
