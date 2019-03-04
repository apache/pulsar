---
title: メッセージの保存と有効期限
lead: Pulsarインスタンスに保存されているメッセージの保存期間の管理
tags_ja: [admin, expiry, retention, backlog]
---

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

Pulsar {% popover_ja Broker %}は、メッセージの[永続ストレージ](../../getting-started/ConceptsAndArchitecture#永続ストレージ)を含むPulsarインスタンスを通過するメッセージを処理する責任を持ちます。デフォルトでは、Brokerは:

* {% popover_ja Consumer %}によって{% popover_ja Ack %} (確認応答) の返された全てのメッセージを直ちに削除します。
* {% popover_ja Ack未受信 %}の全てのメッセージを[バックログ](#バックログクォータ)に永続的に保存します。

Pulsarでは、{% popover_ja ネームスペース %}レベルでこれらのデフォルトの挙動を上書きできます。方法は次の2通りです:

* [保存ポリシー](#保存ポリシー)をセットする事で既に{% popover_ja Consumer %}から{% popover_ja Ack %}の返されたメッセージを永続的に保存できます。
* [生存時間](#生存時間) (TTL) を使用する事で指定された期間内にAckの返されなかったメッセージを削除できます。

Pulsarの[adminインターフェース](../../admin/AdminInterface)は保存ポリシーとTTLを{% popover_ja ネームスペース %}レベル (すなわち特定の{% popover_ja プロパティ %}の内部、かつ特定の{% popover_ja クラスタ %}または[`global`](../../getting-started/ConceptsAndArchitecture#globalクラスタ)クラスタの内部) で管理する事を可能にします。

{% include admonition.html type="warning" title="保存とTTLを同時に使用しないでください" content="
メッセージの保存ポリシーとTTLは同様の目的を果たすものですので、併用すべきではありません。任意のネームスペースに対してどちらか一方を使用してください。
" %}

## 保存ポリシー

デフォルトでは、メッセージが{% popover_ja Broker %}に届くと{% popover_ja Consumer %}によって{% popover_ja Ack %}が返されるまで保存されます。そしてAckが返された時点でメッセージは削除されます。任意の{% popover_ja ネームスペース %}内の全てのトピックに*保存ポリシー*をセットする事で、この挙動を上書きして既にAckの返されたメッセージであっても保存できるようになります。保存ポリシーを設定する際には*サイズ制限*または*時間制限*をセットできます。

例えば、10ギガバイトのサイズ制限をセットしたとすると、そのネームスペース内の全てのトピックのメッセージは*たとえAckが返されたとしても*サイズ制限に達するまでは保存されます。もし1日という時間制限をセットした場合、ネームスペース内の全てのトピックのメッセージは24時間は保存されます。

### デフォルト

[`defaultRetentionTimeInMinutes`](../../reference/Configuration#broker-defaultRetentionTimeInMinutes)と[`defaultRetentionSizeInMB`](../../reference/Configuration#broker-defaultRetentionSizeInMB)の2つの設定パラメータは、{% popover_ja インスタンス %}全体のメッセージ保存ポリシーのデフォルト値をセットするために使用できます。

これらのパラメータは設定ファイル[`broker.conf`](../../reference/Configuration#broker)に存在します。

### 保存ポリシーのセット

ネームスペースと、サイズ制限と時間制限のどちらか*または両方*を指定する事で保存ポリシーをセットできます。

#### pulsar-admin

[`set-retention`](../../reference/CliTools#pulsar-admin-namespaces-set-retention)サブコマンドを使用し、ネームスペースと、`-s`/`--size`を使用してサイズ制限を、`-t`/`--time`を使用して時間制限を指定してください。

##### 例

ネームスペース`my-prop/my-cluster/my-ns`に対して10ギガバイトのサイズ制限をセット:

```shell
$ pulsar-admin namespaces set-retention my-prop/my-cluster/my-ns \
  --size 10G
```

ネームスペース`my-prop/my-cluster/my-ns`に対して3時間の時間制限をセット:

```shell
$ pulsar-admin namespaces set-retention my-prop/my-cluster/my-ns \
  --time 3h
```

サイズ制限と時間制限の両方をセット:

```shell
$ pulsar-admin namespaces set-retention my-prop/my-cluster/my-ns \
  --size 10G \
  --time 3h
```

#### REST API

{% endpoint POST /admin/namespaces/:property/:cluster/:namespace/retention %}

[詳細](../../reference/RestApi#/admin/namespaces/:property/:cluster/:namespace/retention)

#### Java

```java
int retentionTime = 10; // 10分間
int retentionSize = 500; // 500メガバイト
RetentionPolicies policies = new RetentionPolicies(retentionTime, retentionSize);
admin.namespaces().setRetention(namespace, policies);
```

### 保存ポリシーの取得

ネームスペースを指定する事でその保存ポリシーを取得できます。出力は`retentionTimeInMinutes`と`retentionSizeInMB`という2つのキーを持つJSONオブジェクトとなります。

#### pulsar-admin

[`get-retention`](../../reference/CliTools#pulsar-admin-namespaces-get-retention)サブコマンドを使用してネームスペースを指定してください。

##### 例

```shell
$ pulsar-admin namespaces get-retention my-prop/my-cluster/my-ns
{
  "retentionTimeInMinutes": 10,
  "retentionSizeInMB": 0
}
```

#### REST API

{% endpoint GET /admin/namespaces/:property/:cluster/:namespace/retention %}

[詳細](../../reference/RestApi#/admin/namespaces/:property/:cluster/:namespace/retention)

#### Java

```java
admin.namespaces().getRetention(namespace);
```

## バックログクォータ

*バックログ*は、{% popover_ja Bookie %}に保存されたあるトピックに対するAck未受信のメッセージの集合です。Pulsarは全てのAck未受信のメッセージを、それらがConsumerによって処理されてAckが返されるまでの間バックログに保存します。

許容可能なバックログのサイズは*バックログクォータ*を使用する事で{% popover_ja ネームスペース %}レベルで制御できます。バックログクォータをセットするには次の設定が必要です:

* ネームスペース内の各トピックに対して許容される*サイズの閾値*
* 閾値を超えた場合に{% popover_ja Broker %}が実行するアクションを規定する*保存ポリシー*

利用可能な保存ポリシーは次の通りです:

ポリシー | アクション
:------|:------
`producer_request_hold` | Brokerはproduceリクエストをホールドし、メッセージのペイロードを永続化しなくなります
`producer_exception` | Brokerは例外をスローする事でクライアントとの接続を切断します
`consumer_backlog_eviction` | Brokerはバックログからメッセージの破棄を開始します

{% include admonition.html type="warning" title="保存ポリシーのタイプの区別に注意してください" content='
既にご理解頂けたかと思いますが、Pulsarには「保存ポリシー」という用語の定義が2つ存在します。1つは既にAck受信済みのメッセージの永続ストレージに適用され、もう1つはバックログに適用されます。
' %}

バックログクォータは{% popover_ja ネームスペース %}レベルで制御され、以下のようにして管理できます:

### サイズの閾値とバックログ保存ポリシーをセット

ネームスペース、サイズ制限、ポリシー名を指定する事で、{% popover_ja ネームスペース %}内の全てのトピックに対してサイズの閾値とバックログ保存ポリシーをセットできます。

#### pulsar-admin

[`set-backlog-quota`](../../reference/CliTools#pulsar-admin-namespaces-set-backlog-quota)サブコマンドを使用し、ネームスペースと、`-l`/`--limit`を使用してサイズ制限を、`-p`/`--policy`を使用して保存ポリシーを指定してください。

##### 例

```shell
$ pulsar-admin namespaces set-backlog-quota my-prop/my-cluster/my-ns \
  --limit 2G \
  --policy producer_request_hold
```

#### REST API

{% endpoint POST /admin/namespaces/:property/:cluster/:namespace/backlogQuota %}

[詳細](../../reference/RestApi#/admin/namespaces/:property/:cluster/:namespace/backlogQuota)

#### Java

```java
long sizeLimit = 2147483648L;
BacklogQuota.RetentionPolicy policy =
  BacklogQuota.RetentionPolicy.producer_request_hold;
BacklogQuota quota = new BacklogQuota(sizeLimit, policy);
admin.namespaces().setBacklogQuota(namespace, quota);
```

### バックログの閾値と保存ポリシーの取得

ネームスペースに適用されたサイズ制限とバックログの保存ポリシーを確認できます。

#### pulsar-admin

[`get-backlog-quotas`](../../reference/CliTools#pulsar-admin-namespaces-get-backlog-quotas)サブコマンドを使用してネームスペースを指定してください。以下は例となります:

```shell
$ pulsar-admin namespaces get-backlog-quotas my-prop/my-cluster/my-ns
{
  "destination_storage": {
    "limit" : 2147483648,
    "policy" : "producer_request_hold"
  }
}
```

#### REST API

{% endpoint GET /admin/namespaces/:property/:cluster/:namespace/backlogQuotaMap %}

[詳細](../../reference/RestApi#/admin/namespaces/:property/:cluster/:namespace/backlogQuota)

#### Java

```java
Map<BacklogQuota.BacklogQuotaType,BacklogQuota> quotas =
  admin.namespaces().getBacklogQuotas(namespace);
```

### バックログクォータの削除

#### pulsar-admin

[`remove-backlog-quota`](../../reference/CliTools#pulsar-admin-namespaces-remove-backlog-quotas)サブコマンドを使用してネームスペースを指定してください。以下は例となります:

```shell
$ pulsar-admin namespaces remove-backlog-quota my-prop/my-cluster/my-ns
```

#### REST API

{% endpoint DELETE /admin/namespaces/:property/:cluster/:namespace/backlogQuota %}

[詳細](../../reference/RestApi#/admin/namespaces/:property/:cluster/:namespace/backlogQuota)

#### Java

```java
admin.namespaces().removeBacklogQuota(namespace);
```

### バックログを空にする

#### pulsar-admin

[`clear-backlog`](../../reference/CliTools#pulsar-admin-namespaces-clear-backlog)サブコマンドを使用してください。

##### 例

```shell
$ pulsar-admin namespaces clear-backlog my-prop/my-cluster/my-ns
```

デフォルトでは、ネームスペースのバックログを本当に空にするかの確認を求められます。`-f`/`--force`を使用する事で確認を無効化できます。

## 生存時間 (TTL)

デフォルトでは、Pulsarは全てのAck未受信メッセージをいつまでも保存します。そのため、大量のメッセージがAck未受信の状態になるとディスク領域の使用量が増大する可能性があります。ディスク領域が問題となる場合は、Ack未受信のメッセージの生存時間 (TTL) をセットする事ができます。

### ネームスペースにTTLをセット

#### pulsar-admin

[`set-message-ttl`](../../reference/CliTools#pulsar-admin-namespaces-set-message-ttl)サブコマンドを使用し、ネームスペースと`-ttl`/`--messageTTL`を使用してTTL (秒単位) を指定してください。

##### 例

```shell
$ pulsar-admin namespaces set-message-ttl my-prop/my-cluster/my-ns \
  --messageTTL 120 # 2分間のTTL
```

#### REST API

{% endpoint POST /admin/namespaces/:property/:cluster/:namespace/messageTTL %}

[詳細](../../reference/RestApi#/admin/namespaces/:property/:cluster/:namespace/messageTTL)

#### Java

```java
admin.namespaces().setNamespaceMessageTTL(namespace, ttlInSeconds);
```

### ネームスペースのTTL設定の取得

#### pulsar-admin

[`get-message-ttl`](../../reference/CliTools#pulsar-admin-namespaces-get-message-ttl)サブコマンドを使用してネームスペースを指定してください。

##### 例

```shell
$ pulsar-admin namespaces get-message-ttl my-prop/my-cluster/my-ns
60
```

#### REST API

{% endpoint GET /admin/namespaces/:property/:cluster/:namespace/messageTTL %}

[詳細](../../reference/RestApi#/admin/namespaces/:property/:cluster/:namespace/messageTTL)

#### Java

```java
admin.namespaces().get
```
