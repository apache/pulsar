---
title: Pulsarインスタンスのデプロイ
tags_ja: [admin, instance]
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

Pulsar*インスタンス*は協調して動作する複数のPulsar{% popover_ja クラスタ %}から構成されます。クラスタは複数のデータセンターや地理的地域に分散させる事ができ、[ジオレプリケーション](../../admin/GeoReplication)を使用してそれらの間での複製が可能です。マルチクラスタのPulsarインスタンスをデプロイするには、以下の基本的な手順を踏む必要があります:

* 2つの異なる[ZooKeeper](#zookeeperのデプロイ)クォーラムのデプロイ: インスタンス内の個別のクラスタのための[Local ZooKeeper](#local-zookeeperのデプロイ)クォーラムとインスタンス全体のタスクのための[Configuration Store](#configuration-storeのデプロイ)クォーラム
* 各クラスタの[メタデータ](#クラスタメタデータの初期化)の初期化
* 各Pulsarクラスタ内の{% popover_ja Bookie %}の[BookKeeperクラスタ](#bookkeeperのデプロイ)のデプロイ
* 各Pulsarクラスタ内の[Broker](../../admin/ClustersBrokers#brokerの管理)のデプロイ


もし単一のPulsarクラスタをデプロイする場合は、[クラスタとBroker](../../admin/ClustersBrokers)のガイドを参照してください。

{% include admonition.html type="info" title='PulsarをローカルやKubernetesで実行しますか？' content="
このガイドでは、Kubernetesでないプロダクション環境にPulsarをデプロイする方法を説明します。開発目的で単独のマシン上でスタンドアローンのPulsarクラスタを実行したい場合、[ローカルクラスタのセットアップ](../../getting-started/LocalCluster)ガイドを参照してください。[Kubernetes](https://kubernetes.io)でPulsarを実行したい場合、[Kubernetes上のPulsar](../Kubernetes)ガイドを参照してください。このガイドには、[Google Kubernetes Engine](../Kubernetes#google-kubernetes-engine)と[Amazon Web Services](../Kubernetes#amazon-web-services)を使用してKubernetes上でPulsarを実行するセクションが含まれています。
" %}

{% include explanations/ja/install-package.md %}

## ZooKeeperのデプロイ

{% include explanations/ja/deploying-zk.md %}

## クラスタメタデータの初期化

インスタンスのLocal ZooKeeperとConfiguration Storeをセットアップしたら、インスタンス内の各クラスタのZooKeeperに対していくつかのメタデータを書き込む必要があります。書き込みは一度だけで構いません。

このメタデータは、[`pulsar`](../../reference/CliTools#pulsar) CLIツールの[`initialize-cluster-metadata`](../../reference/CliTools#pulsar-initialize-cluster-metadata)コマンドを使用して初期化できます。以下は例となります:

```shell
$ bin/pulsar initialize-cluster-metadata \
  --cluster us-west \
  --zookeeper zk1.us-west.example.com:2181 \
  --configuration-store zk1.us-west.example.com:2184 \
  --web-service-url http://pulsar.us-west.example.com:8080/ \
  --web-service-url-tls https://pulsar.us-west.example.com:8443/ \
  --broker-service-url pulsar://pulsar.us-west.example.com:6650/ \
  --broker-service-url-tls pulsar+ssl://pulsar.us-west.example.com:6651/
```

上の例からもわかるように、次の要素を指定する必要があります:

* クラスタの名前
* そのクラスタのLocal ZooKeeperをカンマで連結した文字列
* インスタンス全体のConfiguration Storeをカンマで連結した文字列
* クラスタのウェブサービスのURL
* クラスタ内の{% popover_ja Broker %}との対話を可能にするBrokerサービスのURL

{% include admonition.html type="info" title="globalクラスタ" content='
それぞれのPulsarインスタンスには、他のクラスタと同様に管理できる`global`クラスタが存在します。`global`クラスタはグローバルなトピックの作成を可能にします。
' %}

[TLS](../../admin/Authz#tlsクライアント認証)を使用している場合、そのクラスタのためのTLSウェブサービスURLとクラスタ内のBrokerのためのTLS BrokerサービスURLも指定する必要があります。

インスタンス内の各クラスタに対して必ず`initialize-cluster-metadata`を実行してください。

## BookKeeperのデプロイ

{% include explanations/ja/deploying-bk.md %}

## Brokerのデプロイ

ZooKeeperのセットアップ、クラスタメタデータの初期化、BookKeeperの{% popover_ja Bookie %}の起動が完了したら、{% popover_ja Broker %}のデプロイが可能になります。

### Brokerの設定

Brokerは設定ファイル[`conf/broker.conf`](../../reference/Configuration#broker)を使用する事で設定が可能です。

Brokerの設定で最も重要な点は、各BrokerがLocal ZooKeeperクォーラムとConfiguration Storeクォーラムを認識しているかを確認する事です。[`zookeeperServers`](../../reference/Configuration#broker-zookeeperServers)パラメータにLocal ZooKeeperクォーラムを、[`configurationStoreServers`](../../reference/Configuration#broker-configurationStoreServers)パラメータにConfiguration Storeクォーラムを反映させてください（ただし、同じクラスタ内に存在するConfiguration Storeのみを指定する必要があります）。

また、[`clusterName`](../../reference/Configuration#broker-clusterName)パラメータを使用してBrokerが所属する{% popover_ja クラスタ %}の名前を指定する必要があります。

設定の例を以下に示します:

```properties
# Local ZooKeeperをカンマで連結した文字列
zookeeperServers=zk1.us-west.example.com:2181,zk2.us-west.example.com:2181,zk3.us-west.example.com:2181

# Configuration Storeをカンマで連結した文字列
configurationStoreServers=zk1.us-west.example.com:2184,zk2.us-west.example.com:2184,zk3.us-west.example.com:2184

clusterName=us-west
```

### Brokerのハードウェア

Pulsar Brokerはローカルディスクを使用しないため、特別なハードウェアを必要としません。ソフトウェアがフルに活用できる高速なCPUと10Gbpsの[NIC](https://ja.wikipedia.org/wiki/%E3%83%8D%E3%83%83%E3%83%88%E3%83%AF%E3%83%BC%E3%82%AF%E3%82%AB%E3%83%BC%E3%83%89)をお勧めします。

### Brokerサービスの起動

[`pulsar-daemon`](../../reference/CliTools#pulsar-daemon) CLIツールで[nohup](https://en.wikipedia.org/wiki/Nohup)を使用してバックグラウンドでBrokerを起動できます:

```shell
$ bin/pulsar-daemon start broker
```

また、[`pulsar broker`](../../reference/CliTools#pulsar-broker)を使用する事でフォアグラウンドでもBrokerを起動できます:

```shell
$ bin/pulsar broker
```

## サービスディスカバリ

{% include explanations/ja/service-discovery-setup.md %}

## adminクライアントと検証

この時点で、Pulsarインスタンスを利用する準備が整いました。これで、各クラスタの[adminクライアント](../../admin/AdminInterface)を提供するクライアントマシンを構成可能になります。設定ファイル[`conf/client.conf`](../../reference/Configuration#クライアント)を使用してadminクライアントの設定が可能です。

最も重要な点は、[`serviceUrl`](../../reference/Configuration#クライアント)パラメータにクラスタの正しいサービスURLを指定する事です:

```properties
serviceUrl=http://pulsar.us-west.example.com:8080/
```

## 新しいテナントのプロビジョニング

Pulsarは根本的に{% popover_ja マルチテナント %}のシステムとして構築されました。新しいテナントはPulsarの{% popover_ja プロパティ %}としてプロビジョニングされます。

新しいテナントがシステムを利用できるようにするには、新しい{% popover_ja プロパティ %}を作成する必要があります。新しいプロパティは[`pulsar-admin`](../../reference/CliTools#pulsar-admin-properties-create) CLIツールを使用する事で作成できます:

```shell
$ bin/pulsar-admin properties create test-prop \
  --allowed-clusters us-west \
  --admin-roles test-admin-role
```

これによって、`test-admin-role`ロールを持つユーザが`us-west`クラスタのみを使用できる`test-prop`プロパティの設定を管理できるようになります。これ以降は、テナントはリソースを自分自身で管理できます。

テナントが作成されたら、そのプロパティ内のトピックの{% popover_ja ネームスペース %}を作成する必要があります。

最初のステップはネームスペースを作成する事です。ネームスペースは多くのトピックを含む事のできる管理単位です。一般的な方法は、単一のテナントからユースケースごとにネームスペースを作成するというものです。

```shell
$ bin/pulsar-admin namespaces create test-prop/us-west/ns1
```

##### ProducerとConsumerのテスト

以上でメッセージを送受信するための準備が全て整いました。システムをテストする最も簡単な方法は、`pulsar-perf`クライアントツールです。

ここで先ほど作成したネームスペース内のトピックを使用しましょう。トピックはProducerかConsumerが初めて使用した時に自動的に作成されます。

この場合のトピック名は次のようになります:

{% include topic.html p="test-prop" c="us-west" n="ns1" t="my-topic" %}

トピックのサブスクリプションを作成し、メッセージを待ち受けるConsumerを開始します:

```shell
$ bin/pulsar-perf consume persistent://test-prop/us-west/ns1/my-topic
```

一定のレートでメッセージを送信し、10秒ごとに統計情報をレポートするProducerを開始します:

```shell
$ bin/pulsar-perf produce persistent://test-prop/us-west/ns1/my-topic
```

トピックの統計情報を閲覧するには次のコマンドを実行します:

```shell
$ bin/pulsar-admin persistent stats persistent://test-prop/us-west/ns1/my-topic
```
