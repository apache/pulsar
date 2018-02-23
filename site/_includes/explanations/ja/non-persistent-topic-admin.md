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

`non-persistent`は発行されたメッセージをリアルタイムでのみconsumeを行いたく、永続的な保証が不要なアプリケーションで利用することができます。`non-persistent`はメッセージを永続化するオーバーヘッドをなくすことでメッセージの発行レイテンシを抑えることができます。

以下の全ての例において、トピック名の構造は次の通りです:

`non-persistent://property/cluster/namespace/topic`


### 統計情報の取得

パーティションドトピックではないトピックの現在の統計情報を表示します。

  -   **msgRateIn**: 全てのローカルのPublisherとレプリケーション用のPublisherの発行レートの合計で、1秒あたりのメッセージ数です。

  -   **msgThroughputIn**: 上記と同様ですが、1秒あたりのバイト数です。

  -   **msgRateOut**: 全てのローカルのConsumerとレプリケーション用のConsumerへの配送レートの合計で、1秒あたりのメッセージ数です。

  -   **msgThroughputOut**: 上記と同様ですが、1秒あたりのバイト数です。

  -   **averageMsgSize**: 直近のインターバル内で発行されたメッセージの平均バイトサイズです。

  -   **publishers**: トピック内の全てのローカルPublisherの一覧です。0または何千もの可能性があります。

  -   **averageMsgSize**: 直近のインターバル内でこのPublisherからのメッセージの平均バイトサイズです。

  -   **producerId**: このトピック上での、このProducerの内部的な識別子です。

  -   **producerName**: クライアントライブラリによって生成されたこのProducerの内部的な識別子です。

  -   **address**: このProducerの接続用のIPアドレスと送信元ポートです。

  -   **connectedSince**: このProducerが作成または最後に再接続したタイムスタンプです。

  -   **subscriptions**: トピックに対してのローカルの全サブスクリプションリストです。

  -   **my-subscription**: このサブスクリプションの名前です (クライアントが定義します) 。

  -   **type**: このサブスクリプションのタイプです。

  -   **consumers**: このサブスクリプションに接続しているConsumerリストです。

  -   **consumerName**: クライアントライブラリによって生成されたこのConsumerの内部的な識別子です。

  -   **availablePermits**: このConsumerがクライアントライブラリのlistenキューに格納できるメッセージ数です。0はクライアントライブラリのキューがいっぱいであり、receive()はコールされないことを意味します。0でない場合には、このConsumerはメッセージを配送される準備ができています。

  -   **replication**: このセクションは、トピックのクラスタ間でのレプリケーションの統計情報を示します。

  -   **connected**: 送信レプリケータが接続されているかどうかです。

  -   **inboundConnection**: このBrokerに対しての、リモートクラスタのPublisher接続におけるそのBrokerのIPとポートです。

  -   **inboundConnectedSince**: リモートクラスタにメッセージを発行するために使われているTCP接続です。もし接続しているローカルのPublisherがいない場合には、この接続は数分後に自動的に閉じられます。

  -   **msgDropRate**: Publisherのメッセージ発行に対してBrokerは接続ごとに設定された転送数のみを許可しており、閾値を超えて発行された全てのメッセージは削除されます。Brokerは利用できない制限がありコネクションが書き込み可能ではない場合もサブスクリプションのメッセージを削除します。


```json
{
  "msgRateIn": 4641.528542257553,
  "msgThroughputIn": 44663039.74947473,
  "msgRateOut": 0,
  "msgThroughputOut": 0,
  "averageMsgSize": 1232439.816728665,
  "storageSize": 135532389160,
  "msgDropRate" : 0.0,
  "publishers": [
    {
      "msgRateIn": 57.855383881403576,
      "msgThroughputIn": 558994.7078932219,
      "averageMsgSize": 613135,
      "producerId": 0,
      "producerName": null,
      "address": null,
      "connectedSince": null,
      "msgDropRate" : 0.0
    }
  ],
  "subscriptions": {
    "my-topic_subscription": {
      "msgRateOut": 0,
      "msgThroughputOut": 0,
      "msgBacklog": 116632,
      "type": null,
      "msgRateExpired": 36.98245516804671,
       "consumers" : [ {
        "msgRateOut" : 20343.506296021893,
        "msgThroughputOut" : 2.0979855364233278E7,
        "msgRateRedeliver" : 0.0,
        "consumerName" : "fe3c0",
        "availablePermits" : 950,
        "unackedMessages" : 0,
        "blockedConsumerOnUnackedMsgs" : false,
        "address" : "/10.73.210.249:60578",
        "connectedSince" : "2017-07-26 15:13:48.026-0700",
        "clientVersion" : "1.19-incubating-SNAPSHOT"
      } ],
      "msgDropRate" : 432.2390921571593

    }
  },
  "replication": {}
}
```

#### pulsar-admin

トピックの統計情報は[`stats`](../../reference/CliTools#stats)コマンドを使って取得できます。

```shell
$ pulsar-admin non-persistent stats \
  non-persistent://test-property/cl1/ns1/tp1 \
```

#### REST API

{% endpoint GET /admin/non-persistent/:property/:cluster/:namespace/:destination/stats %}


#### Java

```java
String destination = "non-persistent://my-property/my-cluster-my-namespace/my-topic";
admin.nonPersistentTopics().getStats(destination);
```

### 詳細な統計情報の取得

トピックの詳細な統計情報を表示します。

#### pulsar-admin

トピックの詳細な統計情報は[`stats-internal`](../../reference/CliTools#stats-internal)で取得できます。

```shell
$ pulsar-admin non-persistent stats-internal \
  non-persistent://test-property/cl1/ns1/tp1 \

{
  "entriesAddedCounter" : 48834,
  "numberOfEntries" : 0,
  "totalSize" : 0,
  "cursors" : {
    "s1" : {
      "waitingReadOp" : false,
      "pendingReadOps" : 0,
      "messagesConsumedCounter" : 0,
      "cursorLedger" : 0,
      "cursorLedgerLastEntry" : 0
    }
  }
}

```

#### REST API

{% endpoint GET /admin/non-persistent/:property/:cluster/:namespace/:destination/internalStats %}


#### Java

```java
String destination = "non-persistent://my-property/my-cluster-my-namespace/my-topic";
admin.nonPersistentTopics().getInternalStats(destination);
```

### パーティションドトピックの作成

Pulsarのパーティションドトピックは明示的に作成しなければなりません。新しいパーティションドトピックを作成する際には、トピックの名前と必要なパーティションの数を指定する必要があります。

#### pulsar-admin

```shell
$ bin/pulsar-admin non-persistent create-partitioned-topic \
  non-persistent://my-property/my-cluster-my-namespace/my-topic \
  --partitions 4
```

#### REST API

{% endpoint PUT /admin/non-persistent/:property/:cluster/:namespace/:destination/partitions %}

#### Java

```java
String topicName = "non-persistent://my-property/my-cluster-my-namespace/my-topic";
int numPartitions = 4;
admin.nonPersistentTopics().createPartitionedTopic(topicName, numPartitions);
```

### メタデータの取得
パーティションドトピックには、JSONオブジェクトとして取得できるメタデータが関連付けられています。現在、次のメタデータフィールドが利用可能です:

フィールド | 意味
:-----|:-------
`partitions` | トピックが分割されるパーティションの数

#### pulsar-admin

```shell
$ pulsar-admin persistent get-partitioned-topic-metadata \
  non-persistent://my-property/my-cluster-my-namespace/my-topic
{
  "partitions": 4
}
```

#### REST API

{% endpoint GET /admin/non-persistent/:property/:cluster:/:namespace/:destination/partitions %}


#### Java

```java
String topicName = "non-persistent://my-property/my-cluster-my-namespace/my-topic";
admin.nonPersistentTopics().getPartitionedTopicMetadata(topicName);
```
