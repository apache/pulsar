# Pulsarクラスタのセットアップ

<!-- TOC depthFrom:2 depthTo:4 withLinks:1 updateOnSave:1 orderedList:0 -->

- [セットアップ](#セットアップ)
	- [システム要件](#システム要件)
	- [コンポーネント](#コンポーネント)
		- [ZooKeeper](#zookeeper)
		- [Global ZooKeeper](#global-zookeeper)
		- [クラスタのメタデータの初期化](#クラスタのメタデータの初期化)
		- [BookKeeper](#bookkeeper)
		- [Broker](#broker)
		- [Service Discovery](#service-discovery)
		- [adminクライアントと検証](#adminクライアントと検証)
- [モニタリング](#モニタリング)

<!-- /TOC -->

## セットアップ

### システム要件

サポートされるプラットフォーム:
  * Linux
  * MacOS X

必要なソフトウェア:
  * Java 1.8

### コンポーネント

#### ZooKeeper

全てのZKサーバをクォーラム構成に追加します。
全てのZKサーバの `conf/zookeeper.conf` を編集し、以下の行を追加してください:

```
server.1=zk1.us-west.example.com:2888:3888
server.2=zk2.us-west.example.com:2888:3888
server.3=zk3.us-west.example.com:2888:3888
...
```

全てのホストでZKサービスを開始します:

```shell
$ bin/pulsar-daemon start zookeeper
```

#### Global ZooKeeper

参加者と全てのオブサーバを追加する事でグローバルクォーラムを構成します。

##### 単一クラスタのPulsarインスタンス

単一クラスタでPulsarインスタンスをデプロイする場合、
Global ZooKeeperは_ローカル_のZKクォーラムと同じマシンにデプロイし、異なるTCPポートで実行させる事ができます。
`conf/global_zookeeper.conf` にサーバを追加し、ポート `2184` でサービスを開始してください:

```
clientPort=2184
server.1=zk1.us-west.example.com:2185:2186
server.2=zk2.us-west.example.com:2185:2186
server.3=zk3.us-west.example.com:2185:2186
...
```

##### 複数クラスタのPulsarインスタンス

異なる地理的地域に分散したクラスタを持つグローバルなPulsarインスタンスをデプロイする場合、
Global ZooKeeperは地域全体の障害や分断に耐え得る高い可用性と強い一貫性を持つメタデータストアとして機能します。

ここで重要な点は、ZKクォーラムのメンバが少なくとも3つの地域にまたがって存在しており、
他の地域がオブザーバとして動作している事を確認する事です。

この場合も、Global ZooKeeperサーバの負荷は非常に低いため、
ローカルのZooKeeperクォーラムに使用されているのと同じホストを共有させる事ができます。

例として、クラスタ `us-west`, `us-east`, `us-central`, `eu-central`, `ap-south` を持つPulsarインスタンスを仮定します。
また、各クラスタには次のような名前のローカルZKサーバがあるものとします。
```
zk[1-3].${CLUSTER}.example.com
```

このシナリオでは、少数のクラスタからクォーラムの参加者を選び、そのほかの全てをZKのオブザーバにします。
例えば、7台のサーバから成るクォーラムを形成するには、`us-west`から3台、`us-central`から2台、`us-east`から2台のサーバを選択できます。

これによって、これらの地域の内1つに到達できなくてもGlobal ZooKeeperへの書き込みが保証されます。

全てのサーバにおけるZKの設定は次のようになります:

```
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

さらに、ZKのオブザーバは次のように設定する必要があります:

```
peerType=observer
```

##### サービスの開始

```shell
$ bin/pulsar-daemon start global-zookeeper
```

#### クラスタのメタデータの初期化

新しいクラスタをセットアップする場合、最初に初期化する必要のあるメタデータがいくつか存在します。
次のコマンドでPulsarのメタデータだけではなくBookKeeperの準備も行う事ができます。

```shell
$ bin/pulsar initialize-cluster-metadata --cluster us-west \
                                         --zookeeper zk1.us-west.example.com:2181 \
                                         --global-zookeeper zk1.us-west.example.com:2184 \
                                         --service-url http://pulsar.us-west.example.com:8080/ \
                                         --service-url-tls https://pulsar.us-west.example.com:8443/
```

#### BookKeeper

Bookieホストはディスク上にデータを保存する責任があり、良好なパフォーマンスを確保するためには適切なハードウェア構成を持つ事が非常に重要です。

キャパシティには2つの重要な側面があります:

- 読み書きに対するディスクI/Oのキャパシティ
- ストレージのキャパシティ

Bookieに書き込まれたエントリは、Pulsar BrokerにAck (確認応答) を返す前に常にディスク上で同期されます。
書き込みのレイテンシを低くするために、BookKeeperは複数のデバイスを使用するように設計されています:

- 耐久性を確保するための_Journal_

  - シーケンシャルな書き込みに対しては、このデバイスで高速な_fsync_操作が可能である事が重要です。
  通常、小型で高速なSSDか、RAIDコントローラとバッテリバックアップ式のライトキャッシュを備えたHDDが適しています。
  どちらのソリューションもfsyncのレイテンシは~0.4 msに達します。

- _"Ledgerストレージデバイス"_

  - これは全てのConsumerがメッセージのAckを返すまでの間データが格納される場所です。
  書き込みはバックグラウンドで行われるため、書き込みI/Oはそれほど重要ではありません。
  読み込みはほとんどのタイプでシーケンシャルに行われ、一部のConsumerがバックログからメッセージを取り出している場合にのみ発生します。
  一般的な構成では、大量のデータを保存できるようにRAIDコントローラを備えた複数のHDDを使用します。

##### 設定

`conf/bookkeeper.conf` の設定の必要最小限の変更は次の通りです:

```shell
# Change to point to journal disk mount point
journalDirectory=data/bookkeeper/journal

# Point to ledger storage disk mount point
ledgerDirectories=data/bookkeeper/ledgers

# Point to local ZK quorum
zkServers=zk1.example.com:2181,zk2.example.com:2181,zk3.example.com:2181

# Change the ledger manager type
ledgerManagerType=hierarchical
```

Apache BookKeeperの詳細なドキュメントは http://bookkeeper.apache.org/ を参照してください。

##### サービスの開始

Bookieを起動します:
```shell
$ bin/pulsar-daemon start bookie
```

Bookieが正常に動作している事を確認します:

```shell
$ bin/bookkeeper shell bookiesanity
```

これによってローカルのBookieに新しいLedgerが作成され、少数のエントリを書き込み、それらを読み込んで最後にLedgerを削除します。

#### Broker

Pulsar Brokerはローカルディスクを使用しないため特別なハードウェアの検討を必要としません。
ソフトウェアが十分に利用できる高速なCPUと10GbpsのNICが推奨されます。

`conf/broker.conf` の最小限の設定変更は次の通りです:

```shell
# Local ZK servers
zookeeperServers=zk1.us-west.example.com:2181,zk2.us-west.example.com:2181,zk3.us-west.example.com:2181

# Global Zookeeper quorum connection string. Here we just need to specify the
# servers located in the same cluster
globalZookeeperServers=zk1.us-west.example.com:2184,zk2.us-west.example.com:2184,zk3.us-west.example.com:2184

clusterName=us-west
```

##### Brokerサービスの開始

```shell
$ bin/pulsar-daemon start broker
```

#### Service Discovery

Service Discoveryコンポーネントは、クライアントが使用する単一のURLを提供するために使用されます。

利用者は提供されたディスカバリサービスか、何らかの他の方法を使用できます。
唯一の要件は、クライアントが `http://pulsar.us-west.example.com:8080/` にHTTPリクエストを送信した時、
それが (DNSやIP、HTTPリダイレクトなどを通して) アクティブなBroker 1台に優先度なしでリダイレクトされなければならない事です。

Pulsarに含まれるディスカバリサービスはHTTPリダイレクトで動作し、ZooKeeper上のアクティブなBrokerのリストを管理します。

`conf/discovery.conf` にZKサーバを追記してください:
```shell
# Zookeeper quorum connection string
zookeeperServers=zk1.us-west.example.com:2181,zk2.us-west.example.com:2181,zk3.us-west.example.com:2181

# Global zookeeper quorum connection string
globalZookeeperServers=zk1.us-west.example.com:2184,zk2.us-west.example.com:2184,zk3.us-west.example.com:2184
```

サービスを開始します:

```
$ bin/pulsar-daemon start discovery
```

#### adminクライアントと検証

この時点で、クラスタを利用する準備が整ったはずです。
adminクライアントとして機能するクライアントマシンを構成できるようになりました。

クライアントホストの `conf/client.conf` を編集して正しいサービスURLを指定してください:

```shell
serviceUrl=http://pulsar.us-west.example.com:8080/
```

##### 新しいテナントの準備

新しいテナントがシステムを利用できるようにするためには、新しいプロパティを作成しなければいけません。
通常、この作業はPulsarクラスタの管理者または自動化されたツールによって行われます:

```shell
$ bin/pulsar-admin properties create test \
                --allowed-clusters us-west \
                --admin-roles test-admin-role
```

これによって、ロール `test-admin-role` 持つユーザは、クラスタ `us-west` のみの利用を許可されたプロパティ `test` の構成を管理できるようになります。

ここから、テナントは自身のリソースを自身で管理できます。

最初のステップはネームスペースの作成です。
ネームスペースは多くのトピックを含む事のできる管理単位です。
一般的なプラクティスは、単一のテナントから異なる用途ごとにネームスペースを作成する事です。

```shell
$ bin/pulsar-admin namespaces create test/us-west/ns1
```

##### ProducerとConsumerのテスト

これでメッセージを送受信するための全ての準備が整いました。
システムをテストする最も簡単な方法は、`pulsar-perf` クライアントツールです。

たった今作成したネームスペースの中のトピックを使用しましょう。
トピックはProducerまたはConsumerがそのトピックを初めて使用する時に自動的に作成されます。

今回の場合、トピック名は次のようになります:
```
persistent://test/us-west/ns1/my-topic
```

トピック上にサブスクリプションを作成し、メッセージを待ち受けるConsumerを起動します:

```shell
$ bin/pulsar-perf consume persistent://test/us-west/ns1/my-topic
```

固定レートでメッセージを発行し、10秒ごとに統計情報をレポートするProducerを起動します:

```shell
$ bin/pulsar-perf produce persistent://test/us-west/ns1/my-topic
```

トピックの統計情報をレポートするには次のようにします:
```shell
$ bin/pulsar-admin persistent stats persistent://test/us-west/ns1/my-topic
```


--------------------------------------------------------------------------------

## モニタリング

### Brokerの統計情報

Pulsar Brokerのメトリクスは複数のBrokerから収集され、JSON形式で送出されます。

メトリクスには主に2つのタイプが存在します:

* 個別のトピックの統計情報を含む、Destinationダンプ
```shell
bin/pulsar-admin broker-stats destinations
```

* Brokerの情報とネームスペースレベルで集約されたトピックの統計情報を含む、Brokerメトリクス
```shell
bin/pulsar-admin broker-stats monitoring-metrics
```

全てのメッセージレートは1分ごとに更新されます。

### BookKeeperの統計情報

BookKeeperで動作する統計フレームワークがいくつか存在しており、`conf/bookkeeper.conf` の `statsProviderClass` の値を変更する事で有効化できます。

上記の手順に従うと、`DataSketchesMetricsProvider` が有効になります。
これはレイテンシの分位値をレートや数と共に計算するための非常に効果的な方法を備えます。

統計情報は一定期間ごとにJSONファイルにダンプされ、毎回上書きされます。

```properties
statsProviderClass=org.apache.bokkeeper.stats.datasketches.DataSketchesMetricsProvider
dataSketchesMetricsJsonFileReporter=data/bookie-stats.json
dataSketchesMetricsUpdateIntervalSeconds=60
```
