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

`persistent`は、メッセージを発行してconsumeするための論理的なエンドポイントであるトピックにアクセスするのに役立ちます。Producerはトピックにメッセージを発行し、Consumerはトピックを購読してトピックに発行されたメッセージをconsumeします。

以下のすべての手順とコマンドで、トピック名の構造は次のとおりです

{% include topic.html p="property" c="cluster" n="namespace" t="topic" %}

### トピックのリストの取得

指定されたネームスペースの下に存在するパーシステントトピックのリストを提供します。

#### pulsar-admin

[`list`](../../reference/CliTools#list)コマンドを使ってトピックのリストを取得できます。

```shell
$ pulsar-admin persistent list \
  my-property/my-cluster/my-namespace \
  my-topic
```

#### REST API

{% endpoint GET /admin/persistent/:property/:cluster/:namespace %}

[詳細](../../reference/RestApi#/admin/persistent/:property/:cluster/:namespace)

#### Java

```java
String namespace = "my-property/my-cluster-my-namespace";
admin.persistentTopics().getList(namespace);
```

### パーミッションの付与

特定のトピックに対して特定のアクションを実行するために、クライアントロールに対するパーミッションを付与します。

#### pulsar-admin

[`grant-permission`](../../reference/CliTools#grant-permission)コマンドを使ってパーミッションを付与できます。

```shell
$ pulsar-admin persistent grant-permission \
  --actions produce,consume --role application1 \
  persistent://test-property/cl1/ns1/tp1 \

```

#### REST API

{% endpoint POST /admin/namespaces/:property/:cluster/:namespace/permissions/:role %}

[詳細](../../reference/RestApi#/admin/namespaces/:property/:cluster/:namespace/permissions/:role)

#### Java

```java
String destination = "persistent://my-property/my-cluster-my-namespace/my-topic";
String role = "test-role";
Set<AuthAction> actions  = Sets.newHashSet(AuthAction.produce, AuthAction.consume);
admin.persistentTopics().grantPermission(destination, role, actions);
```

### パーミッションの取得

[`permissions`](../../reference/CliTools#permissions)コマンドを使ってパーミッションを取得できます。

#### pulsar-admin

TODO: admin

```shell
$ pulsar-admin persistent permissions \
  persistent://test-property/cl1/ns1/tp1 \

{
    "application1": [
        "consume",
        "produce"
    ]
}
```

#### REST API

{% endpoint GET /admin/namespaces/:property/:cluster/:namespace/permissions %}

[詳細](../../reference/RestApi#/admin/namespaces/:property/:cluster/:namespace/permissions)

#### Java

```java
String destination = "persistent://my-property/my-cluster-my-namespace/my-topic";
admin.persistentTopics().getPermissions(destination);
```

### パーミッションの剥奪

クライアントロールに付与されているパーミッションを剥奪します。

#### pulsar-admin

[`revoke-permission`](../../reference/CliTools#revoke-permission)コマンドを使ってパーミッションを剥奪できます。

```shell
$ pulsar-admin persistent revoke-permission \
  --role application1 \
  persistent://test-property/cl1/ns1/tp1 \

{
    "application1": [
        "consume",
        "produce"
    ]
}
```

#### REST API

{% endpoint DELETE /admin/namespaces/:property/:cluster/:namespace/permissions/:role %}

[詳細](../../reference/RestApi#/admin/namespaces/:property/:cluster/:namespace/permissions/:role)

#### Java

```java
String destination = "persistent://my-property/my-cluster-my-namespace/my-topic";
String role = "test-role";
admin.persistentTopics().revokePermissions(destination, role);
```

### 統計情報の取得

パーティションドではないトピックの現在の統計情報を表示します。

  -   **msgRateIn**: 全てのローカルのPublisherとレプリケーション用のPublisherの発行レートの合計で、1秒あたりのメッセージ数です。

  -   **msgThroughputIn**: 上記と同様ですが、1秒あたりのバイト数です。

  -   **msgRateOut**: 全てのローカルのConsumerとレプリケーション用のConsumerへの配送レートの合計で、1秒あたりのメッセージ数です。

  -   **msgThroughputOut**: 上記と同様ですが、1秒あたりのバイト数です。

  -   **averageMsgSize**: 直近のインターバル内で発行されたメッセージの平均バイトサイズです。

  -   **storageSize**: Ledgerストレージのサイズの合計です。

  -   **publishers**: トピック内の全てのローカルPublisherの一覧です。0または何千もの可能性があります。

  -   **averageMsgSize**: 直近のインターバル内でこのPublisherからのメッセージの平均バイトサイズです。

  -   **producerId**: このトピック上での、このProducerの内部的な識別子です。

  -   **producerName**: クライアントライブラリによって生成されたこのProducerの内部的な識別子です。

  -   **address**: このProducerの接続用のIPアドレスと送信元ポートです。

  -   **connectedSince**: このProducerが作成または最後に再接続したタイムスタンプです。

  -   **subscriptions**: トピックに対してのローカルの全サブスクリプションリストです。

  -   **my-subscription**: このサブスクリプションの名前です (クライアントが定義します) 。

  -   **msgBacklog**: このサブスクリプションのバックログにあるメッセージの数です。

  -   **type**: このサブスクリプションのタイプです。

  -   **msgRateExpired**: TTLのため、サブスクリプションに配信されず破棄されているメッセージのレートです。

  -   **consumers**: このサブスクリプションに接続しているConsumerリストです。

  -   **consumerName**: クライアントライブラリによって生成されたこのConsumerの内部的な識別子です。

  -   **availablePermits**: このConsumerがクライアントライブラリのlistenキューに格納できるメッセージ数です。0はクライアントライブラリのキューがいっぱいであり、receive()はコールされないことを意味します。0でない場合には、このConsumerはメッセージを配送される準備ができています。

  -   **replication**: このセクションは、トピックのクラスタ間でのレプリケーションの統計情報を示します。

  -   **replicationBacklog**: メッセージの送信レプリケーションのバックログです。

  -   **connected**: 送信レプリケータが接続されているかどうかです。

  -   **replicationDelayInSeconds**: 接続されている場合、一番古いメッセージが送信されずに待機している時間です。

  -   **inboundConnection**: このBrokerに対しての、リモートクラスタのPublisher接続におけるそのBrokerのIPとポートです。

  -   **inboundConnectedSince**: リモートクラスタにメッセージを発行するために使われているTCP接続です。もし接続しているローカルのPublisherがいない場合には、この接続は数分後に自動的に閉じられます。

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

[`stats`](../../reference/CliTools#stats)コマンドを使ってトピックの統計情報を取得できます。

```shell
$ pulsar-admin persistent stats \
  persistent://test-property/cl1/ns1/tp1 \
```

#### REST API

{% endpoint GET /admin/persistent/:property/:cluster/:namespace/:destination/stats %}

[詳細](../../reference/RestApi#/admin/persistent/:property/:cluster/:namespace/:destination/stats)

#### Java

```java
String destination = "persistent://my-property/my-cluster-my-namespace/my-topic";
admin.persistentTopics().getStats(destination);
```

### 詳細な統計情報の取得

トピックの詳細な統計情報を表示します。

  -   **entriesAddedCounter**: Brokerがこのトピックを読み込んだ後に発行されたメッセージです。

  -   **numberOfEntries**: トラックされているメッセージの総数です。

  -   **totalSize**: 全てのメッセージの合計ストレージ容量 (バイト) です。

  -   **currentLedgerEntries**: 現在書き込み用に開いているLedgerに書き込まれたメッセージの数です。

  -   **currentLedgerSize**: 現在書き込み用に開いているLedgerに書き込まれたメッセージのサイズ (バイト) です。

  -   **lastLedgerCreatedTimestamp**: 最後のLedgerが作成された時間です。

  -   **lastLedgerCreationFailureTimestamp**: 最後のLedgerが破損した時間です。

  -   **waitingCursorsCount**: "捕捉"され、新しいメッセージが発行されるのを待つカーソルの数です。

  -   **pendingAddEntriesCount**: 完了を待っている書き込みリクエストを持つメッセージの数です。

  -   **lastConfirmedEntry**: 書き込みが完了した最新のメッセージのLedgerID:エントリIDです。エントリIDが-1の場合、Ledgerはオープンしたばかりであるか、現在オープンされているものの書き込みされていません。

  -   **state**: 書き込みのためのLedgerの状態です。LedgerOpenedは発行されたメッセージを保存するためのLedgerがオープンされていることを意味します。

  -   **ledgers**: メッセージを保持しているこのトピックの全てのLedgerの順序付きリストです。

  -   **cursors**: このトピックの全てのカーソルのリストです。トピックの統計情報に表示されたサブスクリプションごとに1つずつ表示されます。

  -   **markDeletePosition**: Ackの位置です。つまり、Subscriberが受信のAckを返した最後のメッセージです。

  -   **readPosition**: メッセージを読むためのSubscriberの最新の位置です。

  -   **waitingReadOp**: サブスクリプションがこのトピックに発行された最新のメッセージを受信し、新しいメッセージが発行されるのを待っている場合は、これが`true`となります。

  -   **pendingReadOps**: BookKeeperに対して処理中の未解決の読み込みリクエストの数です。

  -   **messagesConsumedCounter**: このBrokerがこのトピックをロードしてからこのカーソルがAckしたメッセージの数です。

  -   **cursorLedger**: 現在のmarkDeletePositionを永続的に保存するために使用されるLedgerです。

  -   **cursorLedgerLastEntry**: 現在のmarkDeletePositionを永続的に保存するために使用された最後のエントリIDです。

  -   **individuallyDeletedMessages**: Ackが順不同で返されている場合、markDeletePositionと読み取り位置の間でAckを返されたメッセージの範囲を表示します。

  -   **lastLedgerSwitchTimestamp**: カーソルLedgerがロールオーバーされた最後の時間です。

  -   **state**: カーソルLedgerの状態です。`Open`はmarkDeletePositionの更新を保存するためのLedgerを持つことを意味します。

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

[`stats-internal`](../../reference/CliTools#stats-internal)コマンドを使って、トピックの詳細な統計情報が取得できます。

```shell
$ pulsar-admin persistent stats-internal \
  persistent://test-property/cl1/ns1/tp1 \
```

#### REST API

{% endpoint GET /admin/persistent/:property/:cluster/:namespace/:destination/internalStats %}

[詳細](../../reference/RestApi#/admin/persistent/:property/:cluster/:namespace/:destination/internalStats)

#### Java

```java
String destination = "persistent://my-property/my-cluster-my-namespace/my-topic";
admin.persistentTopics().getInternalStats(destination);
```

### メッセージの閲覧

特定のトピックの特定のサブスクリプションについてN個のメッセージを表示します。

#### pulsar-admin


```shell
$ pulsar-admin persistent peek-messages \
  --count 10 --subscription my-subscription \
  persistent://test-property/cl1/ns1/tp1 \

Message ID: 315674752:0  
Properties:  {  "X-Pulsar-publish-time" : "2015-07-13 17:40:28.451"  }
msg-payload
```

#### REST API

{% endpoint GET /admin/persistent/:property/:cluster/:namespace/:destination/subscription/:subName/position/:messagePosition %}

[詳細](../../reference/RestApi#/admin/persistent/:property/:cluster/:namespace/:destination/subscription/:subName/position/:messagePosition)

#### Java

```java
String destination = "persistent://my-property/my-cluster-my-namespace/my-topic";
String subName = "my-subscription";
int numMessages = 1;
admin.persistentTopics().peekMessages(destination, subName, numMessages);
```

### メッセージのスキップ

特定のトピックの特定のサブスクリプションに対してN個のメッセージをスキップします。

#### pulsar-admin


```shell
$ pulsar-admin persistent skip \
  --count 10 --subscription my-subscription \
  persistent://test-property/cl1/ns1/tp1 \
```

#### REST API

{% endpoint POST /admin/persistent/:property/:cluster/:namespace/:destination/subscription/:subName/skip/:numMessages %}

[詳細](../../reference/RestApi#/admin/persistent/:property/:cluster/:namespace/:destination/subscription/:subName/skip/:numMessages)

#### Java

```java
String destination = "persistent://my-property/my-cluster-my-namespace/my-topic";
String subName = "my-subscription";
int numMessages = 1;
admin.persistentTopics().skipMessages(destination, subName, numMessages);
```

### 全てのメッセージのスキップ

特定のトピックの特定のサブスクリプションのすべての古いメッセージをスキップします。

#### pulsar-admin


```shell
$ pulsar-admin persistent skip-all \
  --subscription my-subscription \
  persistent://test-property/cl1/ns1/tp1 \
```

#### REST API

{% endpoint POST /admin/persistent/:property/:cluster/:namespace/:destination/subscription/:subName/skip_all %}

[詳細](../../reference/RestApi#/admin/persistent/:property/:cluster/:namespace/:destination/subscription/:subName/skip_all)

#### Java

```java
String destination = "persistent://my-property/my-cluster-my-namespace/my-topic";
String subName = "my-subscription";
admin.persistentTopics().skipAllMessages(destination, subName);
```

### カーソルのリセット

サブスクリプションのカーソル位置をX分前に記録された位置に戻します。これは本質的にはX分前のカーソルの時間と位置を計算し、その位置でリセットします。

#### pulsar-admin


```shell
$ pulsar-admin persistent reset-cursor \
  --subscription my-subscription --time 10 \
  persistent://test-property/cl1/ns1/tp1 \
```

#### REST API

{% endpoint POST /admin/persistent/:property/:cluster/:namespace/:destination/subscription/:subName/resetcursor/:timestamp %}

[詳細](../../reference/RestApi#/admin/persistent/:property/:cluster/:namespace/:destination/subscription/:subName/resetcursor/:timestamp)

#### Java

```java
String destination = "persistent://my-property/my-cluster-my-namespace/my-topic";
String subName = "my-subscription";
long timestamp = 2342343L;
admin.persistentTopics().skipAllMessages(destination, subName, timestamp);
```

### トピックのルックアップ

特定のトピックに対応しているBrokerのURLを探索します。

#### pulsar-admin


```shell
$ pulsar-admin persistent lookup \
  persistent://test-property/cl1/ns1/tp1 \

 "pulsar://broker1.org.com:4480"
```

#### REST API

{% endpoint GET /lookup/v2/destination/persistent/:property/:cluster/:namespace/:destination %}

#### Java

```java
String destination = "persistent://my-property/my-cluster-my-namespace/my-topic";
admin.lookup().lookupDestination(destination);
```

### サブスクリプションの取得

指定されたトピックの全てのサブスクリプション名を表示します。

#### pulsar-admin

```shell
$ pulsar-admin persistent subscriptions \
  persistent://test-property/cl1/ns1/tp1 \

 my-subscription
```

#### REST API

{% endpoint GET /admin/persistent/:property/:cluster/:namespace/:destination/subscriptions %}

[詳細](../../reference/RestApi#/admin/persistent/:property/:cluster/:namespace/:destination/subscriptions)

#### Java

```java
String destination = "persistent://my-property/my-cluster-my-namespace/my-topic";
admin.persistentTopics().getSubscriptions(destination);
```

### 購読の解除

それ以上のメッセージを処理しないサブスクリプションの登録を解除するのに役立ちます。

#### pulsar-admin


```shell
$ pulsar-admin persistent unsubscribe \
  --subscription my-subscription \
  persistent://test-property/cl1/ns1/tp1 \
```

#### REST API

{% endpoint POST /admin/namespaces/:property/:cluster/:namespace/unsubscribe/:subscription %}

[詳細](../../reference/RestApi#/admin/namespaces/:property/:cluster/:namespace/unsubscribe/:subscription)

#### Java

```java
String destination = "persistent://my-property/my-cluster-my-namespace/my-topic";
String subscriptionName = "my-subscription";
admin.persistentTopics().deleteSubscription(destination, subscriptionName);
```
