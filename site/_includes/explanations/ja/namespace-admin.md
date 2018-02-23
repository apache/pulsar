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

Pulsarの{% popover_ja ネームスペース %}は{% popover_ja トピック %}の論理的なグループです。

ネームスペースは以下のように管理されます:

* [`pulsar-admin`](../../reference/CliTools#pulsar-admin)ツールの[`namespaces`](../../reference/CliTools#pulsar-admin-namespaces)コマンド
* admin [REST API](../../reference/RestApi)のエンドポイント`/admin/namespaces`
* Java [admin API](../../admin/AdminInterface)の{% javadoc PulsarAdmin admin org.apache.pulsar.client.admin.PulsarAdmin %}オブジェクトの`namespaces`メソッド

### 作成

指定した{% popover_ja プロパティ %}およびPulsar{% popover_ja クラスタ %}内に新しいネームスペースを作成できます。

#### pulsar-admin

[`create`](../../reference/CliTools#pulsar-admin-namespaces-create)サブコマンドを使用し、ネームスペースを指定します:

```shell
$ pulsar-admin namespaces create test-property/cl1/ns1
```

#### REST API

{% endpoint PUT /admin/namespaces/:property/:cluster/:namespace %}

[詳細](../../reference/RestApi#/admin/namespaces/:property/:cluster/:namespace)

#### Java

```java
admin.namespaces().createNamespace(namespace);
```

### ポリシーの取得

ネームスペースに関連付けられた現在のポリシーはいつでも取得できます。

#### pulsar-admin


[`policies`](../../reference/CliTools#pulsar-admin-namespaces-policies)サブコマンドを使用して、ネームスペースを指定します:

```shell
$ pulsar-admin namespaces policies test-property/cl1/ns1
{
  "auth_policies": {
    "namespace_auth": {},
    "destination_auth": {}
  },
  "replication_clusters": [],
  "bundles_activated": true,
  "bundles": {
    "boundaries": [
      "0x00000000",
      "0xffffffff"
    ],
    "numBundles": 1
  },
  "backlog_quota_map": {},
  "persistence": null,
  "latency_stats_sample_rate": {},
  "message_ttl_in_seconds": 0,
  "retention_policies": null,
  "deleted": false
}
```

#### REST API

{% endpoint GET /admin/namespaces/:property/:cluster/:namespace %}

[詳細](../../reference/RestApi#/admin/namespaces/:property/:cluster/:namespace)

#### Java

```java
admin.namespaces().getPolicies(namespace);
```

### プロパティ内のネームスペースのリストを取得

指定されたPulsarの{% popover_ja プロパティ %}内のすべてのネームスペースのリストを取得することができます。

#### pulsar-admin

[`list`](../../reference/CliTools#pulsar-admin-namespaces-list)サブコマンドを使用して、プロパティを指定します:

```shell
$ pulsar-admin namespaces list test-property
test-property/cl1/ns1
test-property/cl2/ns2
```

#### REST API

{% endpoint GET /admin/namespaces/:property %}

[詳細](../../reference/RestApi#/admin/namespaces/:property)

#### Java

```java
admin.namespaces().getNamespaces(property);
```

### クラスタ内のネームスペースのリストを取得

指定したPulsar{% popover_ja クラスタ %}内のすべてのネームスペースを表示できます。

#### pulsar-admin

[`list-cluster`](../../reference/CliTools#pulsar-admin-namespaces-list-cluster)サブコマンドを使用して、クラスタを指定します:

```shell
$ pulsar-admin namespaces list-cluster test-property/cl1
test-property/cl1/ns1
test-property/cl1/ns1
```

#### REST API

{% endpoint GET /admin/namespaces/:property/:cluster %}

[詳細](../../reference/RestApi#/admin/namespaces/:property/:cluster)

#### Java

```java
admin.namespaces().getNamespaces(property, cluster);
```

### 削除

プロパティ/クラスタから既存のネームスペースを削除できます。

#### pulsar-admin

[`delete`](../../reference/CliTools#pulsar-admin-namespaces-delete)サブコマンドを使用して、ネームスペースを指定します:

```shell
$ pulsar-admin namespaces delete test-property/cl1/ns1
```

#### REST

{% endpoint DELETE /admin/namespaces/:property/:cluster/:namespace %}

[詳細](../../reference/RestApi#/admin/namespaces/:property/:cluster/:namespace)

#### Java

```java
admin.namespaces().deleteNamespace(namespace);
```

## パーミッションの管理

{% include explanations/ja/permissions.md %}

#### レプリケーションクラスタの設定

ネームスペースにレプリケーションクラスタを設定し、Pulsarが内部的に発行されたメッセージを一つのコロケーションから別のコロケーションにレプリケートできるようにします。しかし、レプリケーションクラスタをセットするためにはネームスペースは*test-property/**global**/ns1*のようにグローバルである必要があります。つまりクラスタ名は*"global"*でなければなりません。

###### CLI

```
$ pulsar-admin namespaces set-clusters test-property/cl1/ns1 \
  --clusters cl2
```

###### REST

```
{% endpoint POST /admin/namespaces/:property/:cluster/:namespace/replication %}
```

###### Java

```java
admin.namespaces().setNamespaceReplicationClusters(namespace, clusters)
```    

#### レプリケーションクラスタの取得

指定されたネームスペースのレプリケーションクラスタのリストを提供します。

###### CLI

```
$ pulsar-admin namespaces get-clusters test-property/cl1/ns1
```

```
cl2
```

###### REST

```
GET /admin/namespaces/{property}/{cluster}/{namespace}/replication
```

###### Java

```java
admin.namespaces().getNamespaceReplicationClusters(namespace)
```

#### バックログクォータポリシーの設定

バックログクォータはBrokerが特定の閾値に達したとき、ネームスペースの帯域幅/ストレージを制限するのに役立ちます。管理者はこの制限と制限に達したときに行う下記のアクションの内一つを設定できます。

  1.  producer_request_hold: Brokerはproduceリクエストのペイロードをホールドし、永続化しないようになります

  2.  producer_exception: Brokerは例外を発生させてクライアントとの接続を切断します

  3.  consumer_backlog_eviction: Brokerはバックログメッセージの破棄を開始します

  バックログクォータ制限はバックログクォータタイプ: destination_storageを定義することによって考慮されるようになります。

###### CLI

```
$ pulsar-admin namespaces set-backlog-quota --limit 10 --policy producer_request_hold test-property/cl1/ns1
```

```
N/A
```

###### REST

```
POST /admin/namespaces/{property}/{cluster}/{namespace}/backlogQuota
```

###### Java

```java
admin.namespaces().setBacklogQuota(namespace, new BacklogQuota(limit, policy))
```

#### バックログクォータポリシーの取得

指定したネームスペースのバックログクォータ設定を表示します。

###### CLI

```
$ pulsar-admin namespaces get-backlog-quotas test-property/cl1/ns1
```

```json
{
    "destination_storage": {
        "limit": 10,
        "policy": "producer_request_hold"
    }
}          
```

###### REST

```
GET /admin/namespaces/{property}/{cluster}/{namespace}/backlogQuotaMap
```

###### Java

```java
admin.namespaces().getBacklogQuotaMap(namespace)
```

#### バックログクォータポリシーの削除  

指定したネームスペースのバックログクォータポリシーを削除します。

###### CLI

```
$ pulsar-admin namespaces remove-backlog-quota test-property/cl1/ns1
```

```
N/A
```

###### REST

```
DELETE /admin/namespaces/{property}/{cluster}/{namespace}/backlogQuota
```

###### Java

```java
admin.namespaces().removeBacklogQuota(namespace, backlogQuotaType)
```

#### 永続性ポリシーの設定

永続性ポリシーは指定したネームスペースにあるすべてのトピックの永続性レベルを設定できます。

  -   Bookkeeper-ack-quorum: 各エントリに対して書き込み成功のAckを待機するBookieの数（保証されるコピーの数）、デフォルト: 0

  -   Bookkeeper-ensemble: 一つのトピックに対して使用されるBookieの数、デフォルト: 0

  -   Bookkeeper-write-quorum: 各エントリに対して書き込みを行うBookieの数、デフォルト: 0

  -   Ml-mark-delete-max-rate: mark-delete操作のスロットル率 (0は無制限)、デフォルト: 0.0

###### CLI

```
$ pulsar-admin namespaces set-persistence --bookkeeper-ack-quorum 2 --bookkeeper-ensemble 3 --bookkeeper-write-quorum 2 --ml-mark-delete-max-rate 0 test-property/cl1/ns1
```

```
N/A
```

###### REST

```
POST /admin/persistent/{property}/{cluster}/{namespace}/persistence
```

###### Java

```java
admin.namespaces().setPersistence(namespace,new PersistencePolicies(bookkeeperEnsemble, bookkeeperWriteQuorum,bookkeeperAckQuorum,managedLedgerMaxMarkDeleteRate))
```


#### 永続性ポリシーの取得

指定したネームスペースの永続性ポリシーの設定を表示します。

###### CLI

```
$ pulsar-admin namespaces get-persistence test-property/cl1/ns1
```

```json
{
  "bookkeeperEnsemble": 3,
  "bookkeeperWriteQuorum": 2,
  "bookkeeperAckQuorum": 2,
  "managedLedgerMaxMarkDeleteRate": 0
}
```

###### REST

```
GET /admin/namespaces/{property}/{cluster}/{namespace}/persistence
```

###### Java

```java
admin.namespaces().getPersistence(namespace)
```


#### ネームスペースバンドルのアンロード

ネームスペースバンドルは同じネームスペースに属するトピックの仮想的なグループです。多数のバンドルによりBrokerの負荷が高まった場合、このコマンドを使って処理が重いバンドルをそのBrokerから取り外し、より負荷が小さい他のBrokerに扱わせることができます。ネームスペースバンドルは0x00000000と0xffffffffのように開始と終了のレンジによって定義されます。

###### CLI

```
$ pulsar-admin namespaces unload --bundle 0x00000000_0xffffffff test-property/pstg-gq1/ns1
```

```
N/A
```

###### REST

```
PUT /admin/namespaces/{property}/{cluster}/{namespace}/unload
```

###### Java

```java
admin.namespaces().unloadNamespaceBundle(namespace, bundle)
```


#### メッセージTTLの設定

メッセージの生存時間（秒）を設定します。

###### CLI

```
$ pulsar-admin namespaces set-message-ttl --messageTTL 100 test-property/cl1/ns1
```

```
N/A
```

###### REST

```
POST /admin/namespaces/{property}/{cluster}/{namespace}/messageTTL
```

###### Java

```java
admin.namespaces().setNamespaceMessageTTL(namespace, messageTTL)
```

#### メッセージTTLの取得

ネームスペースに対して設定されたメッセージTTLを取得します。

###### CLI

```
$ pulsar-admin namespaces get-message-ttl test-property/cl1/ns1
```

```
100
```


###### REST

```
GET /admin/namespaces/{property}/{cluster}/{namespace}/messageTTL
```

###### Java

```java
admin.namespaces().getNamespaceReplicationClusters(namespace)
```


#### バンドルの分割

各ネームスペースのバンドルは複数のトピックを含み、各バンドルはただ一つのBrokerによって扱われます。バンドルがそれに含まれる複数のトピックの処理で重くなった場合、Brokerに対し負荷を発生させます。これを解決するため、管理者はこのコマンドを用いてバンドルを分割できます。

###### CLI

```
$ pulsar-admin namespaces split-bundle --bundle 0x00000000_0xffffffff test-property/cl1/ns1
```

```
N/A
```

###### REST

```
PUT /admin/namespaces/{property}/{cluster}/{namespace}/{bundle}/split
```

###### Java

```java
admin.namespaces().splitNamespaceBundle(namespace, bundle)
```


#### バックログの削除

指定したネームスペースに属するすべてのトピックのすべてのメッセージバックログを削除します。特定のサブスクリプションのバックログのみを削除することも可能です。

###### CLI

```
$ pulsar-admin namespaces clear-backlog --sub my-subscription test-property/pstg-gq1/ns1
```

```
N/A
```

###### REST

```
POST /admin/namespaces/{property}/{cluster}/{namespace}/clearBacklog
```

###### Java

```java
admin.namespaces().clearNamespaceBacklogForSubscription(namespace, subscription)
```


#### バンドルバックログの削除

特定のネームスペースバンドルに属するすべてのトピックのすべてのメッセージバックログを削除します。特定のサブスクリプションのバックログのみを削除することも可能です。

###### CLI

```
$ pulsar-admin namespaces clear-backlog  --bundle 0x00000000_0xffffffff  --sub my-subscription test-property/pstg-gq1/ns1
```

```
N/A
```

###### REST

```
POST /admin/namespaces/{property}/{cluster}/{namespace}/{bundle}/clearBacklog
```

###### Java

```java
admin.namespaces().clearNamespaceBundleBacklogForSubscription(namespace, bundle, subscription)
```


#### 保存情報の設定

各ネームスペースは複数のトピックを含み、各トピックの保存サイズ（ストレージサイズ）は特定の閾値を超えるべきではなく、特定の期間まで保持されるべきです。このコマンドを使って、指定したネームスペース内のトピックの保存サイズと時間を設定できます。

###### CLI

```
$ pulsar-admin set-retention --size 10 --time 100 test-property/cl1/ns1
```

```
N/A
```

###### REST

```
POST /admin/namespaces/{property}/{cluster}/{namespace}/retention
```

###### Java

```java
admin.namespaces().setRetention(namespace, new RetentionPolicies(retentionTimeInMin, retentionSizeInMB))
```


#### 保存情報の取得

指定したネームスペースの保存情報を表示します。

###### CLI

```
$ pulsar-admin namespaces get-retention test-property/cl1/ns1
```

```json
{
    "retentionTimeInMinutes": 10,
    "retentionSizeInMB": 100
}
```

###### REST

```
GET /admin/namespaces/{property}/{cluster}/{namespace}/retention
```

###### Java

```java
admin.namespaces().getRetention(namespace)
```

### ネームスペース隔離

作成中

### Brokerからのアンロード

ネームスペース、もしくは{% popover_ja ネームスペースバンドル %}を現在担当しているPulsar {% popover_ja Broker %}からアンロードできます。

#### pulsar-admin

[`namespaces`](../../reference/CliTools#pulsar-admin-namespaces)コマンドの[`unload`](../../reference/CliTools#pulsar-admin-namespaces-unload)サブコマンドを使用します。

##### 例

```shell
$ pulsar-admin namespaces unload my-prop/my-cluster/my-ns
```

#### REST API

#### Java
