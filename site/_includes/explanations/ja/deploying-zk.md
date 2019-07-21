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

各Pulsar{% popover_ja インスタンス %}は2つの別々のZooKeeperクォーラムに依存します。

* [Local ZooKeeper](#local-zookeeperのデプロイ)は{% popover_ja クラスタ %}レベルで動作し、クラスタ固有の設定管理や協調のための機能を提供します。各Pulsarクラスタには専用のZooKeeperクラスタが必要となります。
* [Configuration Store](#configuration-storeのデプロイ)は{% popover_ja インスタンス %}レベルで動作し、システム全体 (すなわち全てのクラスタ) の設定管理のための機能を提供します。Configuration Storeクォーラムは独立したマシンのクラスタか、あるいはLocal ZooKeeperと同じマシンでも提供できます。

### Local ZooKeeperのデプロイ

ZooKeeperはPulsarの多様かつ重要な協調と設定に関するタスクを管理します。

Pulsarインスタンスをデプロイするには*Pulsar{% popover_ja クラスタ %}ごとに*Local {% popover_ja ZooKeeper %}クラスタを立ち上げる必要があります。

まず、全てのZooKeeperサーバを[`conf/zookeeper.conf`](../../reference/Configuration#zookeeper)ファイルで指定されたクォーラム設定に追加します。クラスタにおける各ノードに対応する`server.N`の行を追加してください。`N`はZooKeeperノードの番号です。以下は3つのノードでのZooKeeperクラスタの例です:

```properties
server.1=zk1.us-west.example.com:2888:3888
server.2=zk2.us-west.example.com:2888:3888
server.3=zk3.us-west.example.com:2888:3888
```

それぞれのホストでは、`myid`ファイルでそのノードのIDを指定する必要があります。このファイルはデフォルトでは`data/zookeeper`フォルダに存在します ([`dataDir`](../../reference/Configuration#zookeeper-dataDir)パラメータで変更可能) 。

{% include admonition.html type="info" content="
`myid`などの詳細はZooKeeperのドキュメントの[複数サーバのセットアップガイド](https://zookeeper.apache.org/doc/r3.4.10/zookeeperAdmin.html#sc_zkMulitServerSetup)を参照してください。
" %}

例えば、ZooKeeperサーバ`zk1.us-west.example.com`では次のようにして`myid`の値をセットできます:

```shell
$ mkdir -p data/zookeeper
$ echo 1 > data/zookeeper/myid
```

`zk2.us-west.example.com`ではコマンドは`echo 2 > data/zookeeper/myid`になります。

各サーバが`zookeeper.conf`の設定に追加され、適切な`myid`エントリを持っていれば、[`pulsar-daemon`](../../reference/CliTools#pulsar-daemon) CLIツールで (nohupを使用してバックグラウンドで) 全てのホストでZooKeeperを起動できます。

```shell
$ bin/pulsar-daemon start zookeeper
```

### Configuration Storeのデプロイ

上記のセクションで設定・起動されたZooKeeperクラスタは、単一のPulsar{% popover_ja クラスタ %}を管理するために使用される*Local* ZooKeeperクラスタです。しかし、完全なPulsar{% popover_ja インスタンス %}にはローカルクラスタに加えてインスタンスレベルの設定と調整タスクを処理する*Global* ZooKeeperクォーラムも必要です。

[単一クラスタ](#単一クラスタのpulsarインスタンス)から成るインスタンスをデプロイする場合、Configuration Storeのための別のクラスタは必要ありません。しかしもし[複数クラスタ](#複数クラスタのpulsarインスタンス)をデプロイするのであれば、インスタンスレベルのタスク用に別のZooKeeperクラスタを立ち上げる必要があります。

{% include message-ja.html id="global_cluster" %}

#### 単一クラスタのPulsarインスタンス

Pulsar{% popover_ja インスタンス %}がただ1つのクラスタで構成されている場合、{% popover_ja Configuration Store %}はLocal ZooKeeperクォーラムと同じマシン上で、異なるTCPポートで実行できます。

単一クラスタのインスタンスにConfiguration Storeをデプロイするには、ローカルクォーラムに使用されているのと同じZooKeeperサーバを、[Local ZooKeeper](#local-zookeeper)と同様の方法で設定ファイル[`conf/global_zookeeper.conf`](../../reference/Configuration#configuration-store)に追加してください。方法は[Local ZooKeeper](#local-zookeeper)の場合と同様ですが、異なるポート (ZooKeeperのデフォルトは2181) を使用してください。以下は2184番ポートを使用する3つのノードから構成されるZooKeeperクラスタの例です:

```properties
clientPort=2184
server.1=zk1.us-west.example.com:2185:2186
server.2=zk2.us-west.example.com:2185:2186
server.3=zk3.us-west.example.com:2185:2186
```

これまでと同様に、各サーバの`data/global-zookeeper/myid`に`myid`ファイルを作成します。

#### 複数クラスタのPulsarインスタンス

異なる地理的地域に分散したクラスタから成るグローバルなPulsarインスタンスをデプロイする場合、Configuration Storeはある地域全体の障害やネットワークパーティションに耐性のある、高い可用性と強い一貫性を持ったメタデータストアとして機能します。

ここで重要な点は、ZKクォーラムのメンバーが少なくとも3つの地域にまたがっており、その他の地域がオブザーバとして実行されている事を確認する事です。

この場合も、Configuration Storeサーバの想定される負荷はとても低いため、Local ZooKeeperクォーラムに使用されているのと同じホストを共有させる事ができます。

例として、`us-west`, `us-east`, `us-central`, `eu-central`, `ap-south`というクラスタを持つPulsarインスタンスを想定します。また、各クラスタには次のような名前のLocal ZooKeeperサーバが存在しているとします:

```
zk[1-3].${CLUSTER}.example.com
```

このシナリオでは、いくつかのクラスタからクォーラムの参加者を選び、それ以外の全てをZKのオブザーバにします。例えば、7台のサーバクォーラムを形成するには、`us-west`から3台、`us-central`から2台、`us-east`から2台のサーバを選ぶ事ができます。

これにより、これらの地域の内の1つに到達できなくてもConfiguration Storeへの書き込みが可能になります。

全てのサーバのZKの設定は次のようになります:

```properties
clientPort=2184
server.1=zk1.us-west.example.com:2185:2186
server.2=zk2.us-west.example.com:2185:2186
server.3=zk3.us-west.example.com:2185:2186
server.4=zk1.us-central.example.com:2185:2186
server.5=zk2.us-central.example.com:2185:2186
server.6=zk3.us-central.example.com:2185:2186:observer
server.7=zk1.us-east.example.com:2185:2186
server.8=zk2.us-east.example.com:2185:2186
server.9=zk3.us-east.example.com:2185:2186:observer
server.10=zk1.eu-central.example.com:2185:2186:observer
server.11=zk2.eu-central.example.com:2185:2186:observer
server.12=zk3.eu-central.example.com:2185:2186:observer
server.13=zk1.ap-south.example.com:2185:2186:observer
server.14=zk2.ap-south.example.com:2185:2186:observer
server.15=zk3.ap-south.example.com:2185:2186:observer
```

さらにZKのオブザーバには次の設定が必要です:

```properties
peerType=observer
```

##### サービスの起動

Configuration Storeの設定が完了したら、[`pulsar-daemon`](../../reference/CliTools#pulsar-daemon)を使用してサービスを起動できます。

```shell
$ bin/pulsar-daemon start configuration-store
```
