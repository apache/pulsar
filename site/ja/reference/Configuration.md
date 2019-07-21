---
title: Pulsarの設定
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

Pulsarの設定は[インストール](../../getting-started/LocalCluster)される[`conf`]({{ site.pulsar_repo }}/conf)ディレクトリ内の一連の設定ファイルにより管理することができます。

* [BookKeeper](#bookkeeper)
* [Broker](#broker)
* [クライアント](#クライアント)
* [サービスディスカバリ](#サービスディスカバリ)
* [Configuration Store](#configuration-store)
* [Log4j](#log4j)
* [Log4j shell](#log4j-shell)
* [スタンドアローン](#スタンドアローン)
* [WebSocket](#websocket)
* [ZooKeeper](#zookeeper)

## BookKeeper

{% popover_ja BookKeeper %}は分散型のログストレージシステムで、Pulsarではメッセージの永続ストレージとして利用しています。

{% include config.html id="bookkeeper" %}

## Broker

Pulsar {% popover_ja Broker %}は{% popover_ja Producer %}から受信するメッセージの処理、{% popover_ja Consumer %}へのメッセージのディスパッチ、{% popover_ja クラスタ %}間でのデータの複製などを担当しています。

{% include config.html id="broker" %}

## クライアント

CLIツール[`pulsar-client`](../CliTools#pulsar-client)は、Pulsarへのメッセージの発行と{% popover_ja トピック %}からのメッセージの受信ができます。このツールはクライアントライブラリの代わりに利用できます。

{% include config.html id="client" %}

## サービスディスカバリ

{% include config.html id="discovery" %}

## Configuration Store

{% include config.html id="configuration-store" %}

## Log4j

{% include config.html id="log4j" %}

## Log4j shell

{% include config.html id="log4j-shell" %}

## スタンドアローン

{% include config.html id="standalone" %}

## WebSocket

{% include config.html id="websocket" %}

## ZooKeeper

{% popover_ja ZooKeeper %}はPulsarの広範囲で重要な構成や調整に関わるタスクを処理します。ZooKeeperのデフォルト設定ファイルは`conf/zookeeper.conf`にあります。以下のパラメータが利用可能です: 

{% include config.html id="zookeeper" %}

上記表のパラメータに加えて、PulsarのためにZooKeeperを設定するにはZooKeeperクラスタにおける各ノードの`conf/zookeeper.conf`ファイルに対して`server.N`の行を追加してください。`N`はZooKeeperノードの番号です。 以下は3つのノードでのZooKeeperクラスタの例です:

```properties
server.1=zk1.us-west.example.com:2888:3888
server.2=zk2.us-west.example.com:2888:3888
server.3=zk3.us-west.example.com:2888:3888
```

{% include admonition.html type="info" content="
ZooKeeperの設定のより完全で包括的な説明は[ZooKeeper管理者ガイド](https://zookeeper.apache.org/doc/current/zookeeperAdmin.html)を参照することを強くお勧めします。" %}
