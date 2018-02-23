<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

Pulsarの[admin API](../../admin/AdminInterface)を使用する事で、パーティションドトピックの作成と管理が可能です。

以下の全ての例において、トピック名の構造は次の通りです:

{% include topic.html p="property" c="cluster" n="namespace" t="topic" %}

### 作成

Pulsarのパーティションドトピックは明示的に作成しなければなりません。新しいパーティションドトピックを作成する際には、トピックの名前と必要なパーティションの数を指定する必要があります。

{% include admonition.html type="info" title="グローバルなパーティションドトピック" content="
グローバルなパーティションドトピックを作成する場合、ここで説明する手順でパーティションドトピックを作成し、トピック名に含まれるクラスタを`global`とする必要があります。
" %}

#### pulsar-admin

[`create-partitioned-topic`](../../reference/CliTools#pulsar-admin-persistent-create-partitioned-topic)コマンドを使用する事でパーティションドトピックの作成が可能です。この時、引数としてトピック名を、`-p`または`--partitions`オプションでパーティション数を指定できます。以下はその例となります:

```shell
$ bin/pulsar-admin persistent create-partitioned-topic \
  persistent://my-property/my-cluster-my-namespace/my-topic \
  --partitions 4
```

#### REST API

{% endpoint PUT /admin/persistent/:property/:cluster/:namespace/:destination/partitions %}

[詳細](../../reference/RestApi#/admin/persistent/:property/:cluster/:namespace/:destination/partitions)

#### Java

```java
String topicName = "persistent://my-property/my-cluster-my-namespace/my-topic";
int numPartitions = 4;
admin.persistentTopics().createPartitionedTopic(topicName, numPartitions);
```

### メタデータの取得

パーティションドトピックには、JSONオブジェクトとして取得できるメタデータが関連付けられています。現在、次のメタデータフィールドが利用可能です:

フィールド | 意味
:-----|:-------
`partitions` | トピックが分割されるパーティションの数

#### pulsar-admin

[`get-partitioned-topic-metadata`](../../reference/CliTools#pulsar-admin-persistent-get-partitioned-topic)サブコマンドを使用する事で、パーティションドトピックのパーティション数を確認できます。以下はその例となります:

```shell
$ pulsar-admin persistent get-partitioned-topic-metadata \
  persistent://my-property/my-cluster-my-namespace/my-topic
{
  "partitions": 4
}
```

#### REST API

{% endpoint GET /admin/persistent/:property/:cluster:/:namespace/:destination/partitions %}

[詳細](../../reference/RestApi#/admin/persistent/:property/:cluster/:namespace/:destination/partitions)

#### Java

```java
String topicName = "persistent://my-property/my-cluster-my-namespace/my-topic";
admin.persistentTopics().getPartitionedTopicMetadata(topicName);
```

### 更新

トピックがグローバル*でない*場合に限り、既存のパーティションドトピックのパーティション数を更新できます。更新するためには、新しいパーティション数は既存の数よりも大きいものでなければなりません。

パーティション数の削減は分割されたトピックの削除を意味するため、Pulsarではサポートされていません。

既に作成済みのパーティションドトピックのProducer/Consumerは新たに作成されたパーティションを見つける事ができないため、アプリケーションによって再作成される必要があります。新しく作成されたProducer/Consumerは、新たに追加されたパーティションにも接続できます。したがって、アプリケーションで全てのProducerが再起動されるまでは、Producerにおけるパーティションの順序付けに違反が発生する可能性があります。

#### pulsar-admin

パーティションドトピックは[`update-partitioned-topic`](../../reference/CliTools#pulsar-admin-persistent-update-partitioned-topic)コマンドを使用する事で更新可能です。

```shell
$ pulsar-admin persistent update-partitioned-topic \
  persistent://my-property/my-cluster-my-namespace/my-topic \
  --partitions 8
```

#### REST API

{% endpoint POST /admin/persistent/:property/:cluster/:namespace/:destination/partitions %}

[詳細](../../reference/RestApi#/admin/persistent/:property/:cluster/:namespace/:destination/partitions)

#### Java

```java
admin.persistentTopics().updatePartitionedTopic(persistentTopic, numPartitions);
```

### 削除

#### pulsar-admin

パーティションドトピックは[`delete-partitioned-topic`](../../reference/CliTools#pulsar-admin-persistent-delete-partitioned-topic)コマンドを使用し、トピック名を指定する事で削除可能です:

```shell
$ bin/pulsar-admin persistent delete-partitioned-topic \
  persistent://my-property/my-cluster-my-namespace/my-topic
```

#### REST API

{% endpoint DELETE /admin/persistent/:property/:cluster/:namespace/:destination/partitions %}

[詳細](../../reference/RestApi#/admin/persistent/:property/:cluster/:namespace/:destination/partitions)

#### Java

```java
admin.persistentTopics().delete(persistentTopic);
```

### リストの取得

指定されたネームスペースの下に存在するパーティションドトピックのリストを表示します。

#### pulsar-admin

```shell
$ pulsar-admin persistent list prop-1/cluster-1/namespace
persistent://property/cluster/namespace/topic
persistent://property/cluster/namespace/topic
```

#### REST API

{% endpoint GET /admin/persistent/:property/:cluster/:namespace %}

[詳細](../../reference/RestApi#/admin/persistent/:property/:cluster/:namespace)

#### Java

```java
admin.persistentTopics().getList(namespace);
```

### 統計情報

指定されたパーティションドトピックの現在の統計情報を表示します。以下はペイロードの例となります:

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

統計情報には次のようなものが含まれます:

{% include stats-ja.html id="partitioned_topics" %}

#### pulsar-admin

[`partitioned-stats`](../../reference/CliTools#pulsar-admin-persistent-partitioned-stats)

```shell
$ pulsar-admin persistent partitioned-stats \
  persistent://test-property/cl1/ns1/tp1 \
  --per-partition        
```

#### REST API

{% endpoint GET /admin/persistent/:property/:cluster/:namespace/:destination/partitioned-stats %}

[詳細](../../reference/RestApi#/admin/persistent/:property/:cluster/:namespace/:destination/partitioned-stats)

#### Java

```java
admin.persistentTopics().getStats(persistentTopic);
```

### 詳細な統計情報

トピックの詳細な統計情報を表示します。

{% include stats-ja.html id="topics" %}

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

```shell
$ pulsar-admin persistent stats-internal \
  persistent://test-property/cl1/ns1/tp1
```

#### REST API

{% endpoint GET /admin/persistent/:property/:cluster/:namespace/:destination/internalStats %}

[詳細](../../reference/RestApi#/admin/persistent/:property/:cluster/:namespace/:destination/internalStats)

#### Java

```java
admin.persistentTopics().getInternalStats(persistentTopic);
```
