---
title: シミュレーションツール
tags_ja: [simulation]
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

時折、ロードマネージャがうまく負荷を制御できるか観察するために、テスト環境を作成し、人工的に負荷を生じさせる必要があります。負荷シミュレーションコントローラ、負荷シミュレーションクライアント、Brokerモニタはこの負荷を作成し、マネージャへの影響をより簡単に観察するために作成されました。

## シミュレーションクライアント
シミュレーションクライアントは設定可能なメッセージレートとサイズでトピックの作成と購読を行うツールです。時折、大きな負荷をシミュレートするために複数のクライアントマシンを使用する必要があるので、ユーザがシミュレーションクライアントと直接対話するのではなく、リクエストをシミュレーションコントローラに委任し、クライアントにシグナルを送信して負荷をかけ始めます。クライアントの実装は`org.apache.pulsar.testclient.LoadSimulationClient`クラスにあります。

### 使い方
シミュレーションクライアントを起動するために、以下のように`pulsar-perf`スクリプトの`simulation-client`コマンドを使用します:

```
pulsar-perf simulation-client --port <リッスンポート> --service-url <PulsarのサービスURL>
```

クライアントがコントローラコマンドを受け取る準備が完了します。
## シミュレーションコントローラ
シミュレーションコントローラはシミュレーションクライアントにシグナルを送信し、新しいトピックの作成、古いトピックの停止、トピックに起因する負荷の変更、その他いくつかのタスクをリクエストします。`org.apache.pulsar.testclient.LoadSimulationController`クラスに実装されており、コマンドを送信するためのシェルのユーザインターフェースを提供しています。

### 使い方
シミュレーションコントローラを起動するために、以下のように`pulsar-perf`スクリプトの`simulation-controller`を使用します:

```
pulsar-perf simulation-controller --cluster <シミュレートするクラスタ名> --client-port <クライアントのリッスンするポート>
--clients <カンマ区切りのクライアントのホストネーム>
```

クライアントはコントローラが起動される前に起動されるべきです。シンプルなプロンプトが表示され、そこでシミュレーションクライアントにコマンドが発行できます。引数はテナント名、ネームスペース名、トピック名です。全てのケースで、テナント、ネームスペース、トピックの名前が使われます。例えば、トピック`persistent://my_cluster/my_tenant/my_namespace/my_topic`では、テナント名は`my_tenant`, ネームスペース名は`my_namespace`, トピック名は`my_topic`です。コントローラは以下のアクションを実行できます:

* Producer, Consumerと共に新しいトピックを作成する
    * `trade <テナント> <ネームスペース> <トピック> [--rate <メッセージレート (毎秒)>]
    [--rand-rate <下限>,<上限>]
    [--size <メッセージサイズ (バイト)>]`
* Producer, Consumerと共に新しいトピックのグループを作成する
    * `trade_group <テナント> <グループ> <ネームスペース数> [--rate <メッセージレート (毎秒)>]
    [--rand-rate <下限>,<上限>]
    [--separation <トピックを作成する間隔 (ms)>] [--size <メッセージサイズ (バイト)>]
    [--topics-per-namespace <ネームスペース毎のトピック数>]`
* 既存トピックの設定変更
    * `change <テナント> <ネームスペース> <トピック> [--rate <メッセージレート (毎秒)>]
    [--rand-rate <下限>,<上限>]
    [--size <メッセージサイズ (バイト)>]`
* トピックのグループの設定変更
    * `change_group <テナント> <グループ> [--rate <メッセージサイズ (バイト)>] [--rand-rate <下限>,<上限>]
    [--size <メッセージサイズ (バイト)>] [--topics-per-namespace <ネームスペース毎のトピック数>]`
* 作成したトピックを停止する
    * `stop <テナント> <ネームスペース> <トピック>`
* 作成したトピックグループを停止する
    * `stop_group <テナント> <グループ>`
* 1つのZooKeeperから他のZooKeeperへヒストリカルデータをコピーし、そのメッセージレート、サイズに基いてシミュレートする
    * `copy <テナント> <ソースZooKeeper> <ターゲットZooKeeper> [--rate-multiplier 値]`
* 現在のZooKeeperのヒストリカルデータの負荷をシミュレートする (シミュレートされているZooKeeperと同じである必要があります)
    * `simulate <テナント> <ZooKeeper> [--rate-multiplier 値]`
* アクティブなZooKeeperから最新のデータをストリームし、そのZooKeeperのリアルタイムな負荷をシミュレートする
    * `stream <テナント> <ZooKeeper> [--rate-multiplier 値]`

これらのコマンドの"グループ"という引数はユーザに1度で複数のトピックの作成や変更を可能にします。`trade_group`コマンドが呼ばれた時にグループが作成され、グループに所属する全てのトピックは`change_group`及び`stop_group`コマンドで、それぞれ修正、もしくは停止されます。全てのZooKeeper引数は`zookeeper_host:port`という形式です。

#### コピー、シミュレート、ストリームの違い
`copy`, `simulate`, `stream`コマンドは非常に似ていますが、大きな違いがあります。`copy`は外部ZooKeeperの静的な負荷を、シミュレートしているZooKeeper上でシミュレートするために使用されます。したがって、`ソースZooKeeper`はコピー元のZooKeeper、`ターゲットZooKeeper`はシミュレートしているZooKeeperです。そして、両方のロードマネージャの実装で、ソースのヒストリカルデータを十分に活用できます。一方で、`simulate`はシミュレートしているZooKeeper1つのみを使用します。`SimpleLoadManagerImpl`のヒストリカルデータを持ち、`ModularLoadManagerImpl`と等価なヒストリカルデータを作成しているZooKeeper上でシミュレートしていることを仮定しています。そして、ヒストリカルデータによる負荷が、クライアントによってシミュレートされます。最後に、`stream`はシミュレートされているZooKeeperとは異なるアクティブなZooKeeperを用い、そこからデータを読み込み、リアルタイムの負荷をシミュレートします。全てのケースで、`--rate-multiplier`オプションを用いることで、いくらかの割合で負荷をシミュレートすることができます。例えば、`--rate-multiplier 0.05`を用いた場合、メッセージはシミュレートされているされている負荷の割合の`5%`だけ送信されます。

## Brokerモニタ
これらのシミュレーションでのロードマネージャの動きを監視するために、`org.apache.pulsar.testclient.BrokerMonitor`に実装されたBrokerモニタを利用できます。Brokerモニタはウォッチャを使用して更新された時に表形式の負荷データをコンソールに出力します。

### 使い方
Brokerモニタを起動するために`pulsar-perf`スクリプトの`monitor-brokers`コマンドを使います:

```
pulsar-perf monitor-brokers --connect-string <ZooKeeper host:port>
```

インタラプトされるまで、コンソールに負荷データが表示されます。
