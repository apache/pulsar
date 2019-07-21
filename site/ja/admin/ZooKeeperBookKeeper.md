---
title: ZooKeeperとBookKeeperの管理
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

Pulsarは2つの外部システムを使って根本的なタスクを行っています：

* [ZooKeeper](https://zookeeper.apache.org/)は様々な構成や調整に関わるタスクを行う役割を持ちます。
* [BookKeeper](http://bookkeeper.apache.org/)はメッセージデータの[永続ストレージ](../../getting-started/ConceptsAndArchitecture#永続ストレージ)としての役割を持ちます。

ZooKeeperとBookKeeperはどちらもオープンソースの[Apache](https://www.apache.org/)プロジェクトです。

{% include admonition.html type='info' content='
これら2つのシステムのPulsarにおける役割についての説明図は[PulsarはZooKeeperとBookKeeperをどのように利用しているか](#pulsarはzookeeperとbookkeeperをどのように利用しているか)を見てください。' %}

## ZooKeeper

{% include explanations/ja/deploying-zk.md %}

### ZooKeeperの設定

PulsarにおけるZooKeeperの設定はインストール時に生成された`conf`ディレクトリにある2つの設定ファイル: `conf/zookeeper.conf`（[Local ZooKeeper](#local-zookeeper)用）と`conf/global-zookeeper.conf`（[Configuration Store](#global-zookeeper)用）によって操作されます。

#### Local ZooKeeper

Local ZooKeeperの設定は[`conf/zookeeper.conf`](../../reference/Configuration#zookeeper)によって操作されます。下記の表は利用可能な設定項目を表しています：

{% include config.html id="zookeeper" %}

#### Configuration Store

Configuration Storeの設定は[`conf/global-zookeeper.conf`](../../reference/Configuration#configuration-store)によって操作されます。下記の表は利用可能な設定項目を表しています：

{% include config.html id="configuration-store" %}

## BookKeeper

{% popover_ja BookKeeper %}はPulsarにおけるすべての永続メッセージのストレージとしての役割を持ちます。BookKeeperは分散型の[ログ先行書き込み（write ahead logging, WAL）](https://ja.wikipedia.org/wiki/%E3%83%AD%E3%82%B0%E5%85%88%E8%A1%8C%E6%9B%B8%E3%81%8D%E8%BE%BC%E3%81%BF)システムであり、{% popover_ja Ledger %}と呼ばれる独立したログの読み込みの一貫性を保証します。個々のBookKeeperのサーバーは*Bookie*とも呼ばれます。

{% include admonition.html type="info" content="
Pulsarにおけるメッセージの永続性、保存、有効期限を扱うためのガイドは[こちら](../../advanced/RetentionExpiry)を参照してください。
" %}

### BookKeeperのデプロイ

{% include explanations/ja/deploying-bk.md %}

### BookKeeperの設定

BookKeeperの設定可能なパラメータは[`conf/bookkeeper.conf`](../../reference/Configuration#bookkeeper)にあります。

`conf/bookkeeper.conf`において必要とされる最小の設定変更：

```properties
# Journal用ディスクのマウントポイントを変更してください
journalDirectory=data/bookkeeper/journal

# Ledger用ストレージディスクのマウントポイントを指定してください
ledgerDirectories=data/bookkeeper/ledgers

# Local ZooKeeperのクォーラムを指定してください
zkServers=zk1.example.com:2181,zk2.example.com:2181,zk3.example.com:2181

# Ledgerのマネージャータイプを変更してください
ledgerManagerType=hierarchical
```

{% include admonition.html type='info' content='BookKeeperに関する詳細な情報は[公式ドキュメント](http://bookkeeper.apache.org)を参照してください。' %}

## BookKeeperの永続性ポリシー

Pulsarでは*永続性ポリシー*を{% popover_ja ネームスペース %}単位での設定が可能です。これは{% popover_ja BookKeeper %}がメッセージの永続ストレージをどう扱うかを定義します。ポリシーは4つの要素から成っています：

* 各エントリに対して書き込み成功の{% popover_ja Ack %}を待機するBookieの数（保証されるコピーの数）
* 一つのトピックに対して使用される{% popover_ja Bookie %}の数
* 各エントリに対して書き込みを行うBookieの数
* mark-delete操作のスロットル率

### 永続性ポリシーの設定

BookKeeperの永続性ポリシーを{% popover_ja ネームスペース %}単位で設定できます。

#### pulsar-admin

[`set-persistence`](../../reference/CliTools#pulsar-admin-namespaces-set-persistence)サブコマンドを使い、ネームスペースと適用したいポリシーを指定してください。以下が利用可能なフラグです：

フラグ | 説明 | デフォルト値
:----|:------------|:-------
`-a`, `--bookkeeper-ack-quorom` | 各エントリに対して書き込み成功の{% popover_ja Ack %}を待機するBookieの数（保証されるコピーの数） | 0
`-e`, `--bookkeeper-ensemble` | 一つのトピックに対して使用される{% popover_ja Bookie %}の数 | 0
`-w`, `--bookkeeper-write-quorum` | 各エントリに対して書き込みを行うBookieの数 | 0
`-r`, `--ml-mark-delete-max-rate` | mark-delete操作のスロットル率（0は無制限） | 0

##### 例

```shell
$ pulsar-admin namespaces set-persistence my-prop/my-cluster/my-ns \
  --bookkeeper-ack-quorom 3 \
  --bookeeper-ensemble 2
```

#### REST API

{% endpoint POST /admin/namespaces/:property/:cluster/:namespace/persistence %}

[詳細](../../reference/RestApi#/admin/namespaces/:property/:cluster/:namespace/persistence)

#### Java

```java
int bkEnsemble = 2;
int bkQuorum = 3;
int bkAckQuorum = 2;
double markDeleteRate = 0.7;
PersistencePolicies policies =
  new PersistencePolicies(ensemble, quorum, ackQuorum, markDeleteRate);
admin.namespaces().setPersistence(namespace, policies);
```

### 永続性ポリシーの取得

ネームスペースに現在適用されている永続性ポリシーを表示できます。

#### pulsar-admin

[`get-persistence`](../../reference/CliTools#pulsar-admin-namespaces-get-persistence)サブコマンドを使い、ネームスペースを指定してください。

##### 例

```shell
$ pulsar-admin namespaces get-persistence my-prop/my-cluster/my-ns
{
  "bookkeeperEnsemble": 1,
  "bookkeeperWriteQuorum": 1,
  "bookkeeperAckQuorum", 1,
  "managedLedgerMaxMarkDeleteRate": 0
}
```

#### REST API

{% endpoint GET /admin/namespaces/:property/:cluster/:namespace/persistence %}

[詳細](../../reference/RestApi#/admin/namespaces/:property/:cluster/:namespace/persistence)

#### Java

```java
PersistencePolicies policies = admin.namespaces().getPersistence(namespace);
```

## PulsarはZooKeeperとBookKeeperをどのように利用しているか

この図はZooKeeperとBookKeeperのPulsarクラスタ内における役割を表しています：

![ZooKeeperとBookKeeper](/img/pulsar_system_architecture.png)

各Pulsar{% popover_ja クラスタ %}は一つ以上の{% popover_ja Broker %}から構成されます。各Brokerは{% popover_ja Bookie %}のアンサンブルに依存しています。
