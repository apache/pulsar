---
title: Pulsarのコンセプトとアーキテクチャ
lead: Pulsarの動作に関する概要
tags_ja:
- architecture
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

{% popover_ja Pulsar %}は[Yahoo](http://yahoo.github.io/)で開発され、現在は[Apache Software Foundation](https://www.apache.org/)の管理下にある、サーバからサーバへメッセージングを行うためのマルチテナント、ハイパフォーマンスなソリューションです。

Pulsarの主要な特徴:

* 複数{% popover_ja クラスタ %}のネイティブサポートとクラスタを横断するメッセージのシームレスな[ジオレプリケーション](../../admin/GeoReplication)
* メッセージの発行及びエンドツーエンドの通信が低レイテンシ
* 100万以上のトピックに対するシームレスなスケーラビリティ
* [Java](../../clients/Java), [Python](../../clients/Python), [C++](../../clients/Cpp)によるシンプルな[クライアントAPI](#クライアントapi)
* {% popover_ja トピック %}に対する複数の[サブスクリプションモード](#サブスクリプションモード) ([Exclusive](#exclusive), [Shared](#shared), [Failover](#failover))
* [Apache BookKeeper](http://bookkeeper.apache.org/)を用いた[永続ストレージ](#永続ストレージ)による配信保証

## Producer, Consumer, トピック、サブスクリプション

Pulasrは[publish-subscribe](https://ja.wikipedia.org/wiki/%E5%87%BA%E7%89%88-%E8%B3%BC%E8%AA%AD%E5%9E%8B%E3%83%A2%E3%83%87%E3%83%AB)パターン (別名{% popover_ja pub-sub %}) で構築されています。このパターンでは、[Producer](#producer)が[トピック](#トピック)に対してメッセージを発行します。そして、[Consumer](#consumer)はトピックを[購読](#サブスクリプションモード) し、受け取ったメッセージに対して処理をし、処理の完了時に{% popover_ja Ack %} (確認応答) を送信します。

一度{% popover_ja サブスクリプション %}が作られると、例えConsumerが切断しても、全てのメッセージはPulsarによって[保持](#永続ストレージ)されます。保持されたメッセージはConsumerが処理に成功し{% popover_ja Ack %}を返した時にのみ破棄されます。

### Producer

Producerはトピックに接続し、Pulsarの{% popover_ja Broker %}にメッセージを送信します。

#### 送信モード

Producerはメッセージを同期的あるいは非同期的にBrokerに送信できます。

| モード       | 説明                                                                                                                                                                                                                                                                                                                                                              |
|:-----------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 同期送信  | Producerはメッセージを送信した後BrokerからのAckを待ちます。もしAckが受信されない場合はProducerは送信処理が失敗したと判断します。                                                                                                                                        |
| 非同期送信 | Producerはメッセージをブロッキングキューに入れ、制御を戻します。クライアントライブラリはバックグラウンドでBrokerに送信します。キューが最大量 ([設定可能](../../reference/Configuration#broker)) に達した場合、Producerは送信APIを呼び出した時、Producerに渡される引数に応じてブロックされる、あるいは失敗する可能性があります。|

#### 圧縮

帯域を節約するため、メッセージを圧縮することが可能です。圧縮と解凍はどちらもクライアントで動作します。以下の圧縮形式をサポートしています:

* [LZ4](https://github.com/lz4/lz4)
* [ZLIB](https://zlib.net/)

#### バッチ

バッチ処理が可能な場合、Producerはメッセージを蓄積し、1つのリクエストでメッセージのバッチを送信しようとします。バッチサイズはメッセージの最大数と最大発行レイテンシで定義されます。

### Consumer

Consumerはサブスクリプションを通じてトピックに接続し、メッセージを受け取ります。

#### 受信モード

メッセージは同期的あるいは非同期的に受信されます。

| Mode          | Description                                                                                                                                                                                                   |
|:--------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 同期受信  | メッセージが利用可能になるまでブロックされます。                                                                                                                                                  |
| 非同期受信 | CompletableFutureインスタンスとしてすぐに返却されます。[`CompletableFuture`](http://www.baeldung.com/java-completablefuture)インスタンスは新しいメッセージが利用可能になった時、受信して完了します。|

#### Ack

Consumerはメッセージの処理に成功した際、Brokerがメッセージを破棄することができるように{% popover_ja Ack %}を送る必要があります (そうしなければ、メッセージは[保存](#永続ストレージ)されます) 。

メッセージは1つ1つ個別に、あるいは累積的にAckが返されます。累積的なAckをするConsumerは、最後に受け取ったメッセージのAckを返します。その場合、Ackを返したメッセージまでのストリーム内の全てのメッセージはそのConsumerに再送されません。

{% include admonition.html type='warning' content='累積的なAckは[サブスクリプションモード](#サブスクリプションモード)がSharedの場合は複数のConsumerが同じサブスクリプションにアクセスするため使用できません。' %}

#### メッセージリスナー

カスタマイズされたメッセージリスナーの実装をConsumerに渡すことができます。例えば[Javaクライアント](../../clients/Java)では{% javadoc MesssageListener client com.yahoo.pulsar.client.api.MessageListener %}インターフェイスを提供しています。新しいメッセージを受け取った時、`received`メソッドが実行されます。

### トピック

他のpub-subシステムと同様に、PulsarのトピックはProducerからConsumerにメッセージを送信するための名前のついたチャンネルです。トピックの名前は明確に定義された構造を持つURLとして表現されます:

{% include topic.html p="プロパティ" c="クラスタ" n="ネームスペース" t="トピック" %}

| トピック名の要素 | 説明                                                                                                                                                                                                                                  |
|:---------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `persistent`         | トピックのタイプを示します。Pulsarは2種類のトピック (パーシステント, ノンパーシステント) をサポートしています。 パーシステントトピックでは全てのメッセージはディスク ({% popover_ja Broker %}が{% popover_ja スタンドアローン %}でない場合は複数のディスクを指します。) に[永続化](#永続ストレージ)されます。一方、[ノンパーシステント](#ノンパーシステントトピック)トピックはメッセージをディスクに永続化しません。 |
| `プロパティ`           | そのインスタンス内のトピックの{% popover_ja テナント %}を示します。テナントはPulsarでの{% popover_ja マルチテナント %}のにおいて不可欠であり、クラスタを横断して利用されます。                                                                                    |
| `クラスタ`            | トピックがどこに位置するかを示します。 一般的に、各地域やデータセンターに1つの{% popover_ja クラスタ %}があります。                                                                                                                   |
| `ネームスペース`          | トピックの管理単位を示します。関連トピックのグループ化メカニズムとして機能します。ほとんどのトピックの設定はネームスペース単位で実行されます。各プロパティ (テナント) は複数のネームスペースを持ちます。                              |
| `トピック`              | トピック名の最後の部分です。トピック名は自由であり、Pulsarインスタンスにおいて特別な意味を持ちません。                                                                                                                                       |

### サブスクリプションモード

サブスクリプションモードは{% popover_ja Consumer %}にどのようにメッセージが配送されるかを決定するルールです。Pulsarでは[Exclusive](#exclusive), [Shared](#shared), [Failover](#failover)の3つのサブスクリプションモードが利用可能です。以下の図は、それぞれのモードに関する説明を図示したものです。

![サブスクリプションモード](/img/pulsar_subscriptions.jpg)

#### Exclusive

*Exclusive* モードでは、1つのサブスクリプションに対して1つのConsumerのみが接続を許可されます。2つ以上のConsumerが同じサブスクリプション名を用いてトピックに対して購読を試みた場合、エラーが発生します。

上図では、**Consumer-A** のみがメッセージをconsumeできます。

Exclusiveはデフォルトのサブスクリプションモードです。

#### Shared

*Shared* モードでは、複数のConsumerが同じサブスクリプションに接続できます。メッセージはラウンドロビンで複数のConsumerに配送され、各メッセージは1つのConsumerのみに配送されます。Consumerが切断するとき、送信されたがAck (確認応答) が返ってきていない全てのメッセージは、再送のためにリスケジュールされます。

上図では、**Consumer-B-1** と **Consumer-B-2** がトピックを購読できます。しかし、**Consumer-C-1** やその他のConsumerも同様に購読できます。

{% include message-ja.html id="shared_mode_limitations" %}

#### Failover

*Failover* モードでは、複数のConsumerが同じサブスクリプションに接続できます。ConsumerはConsumer名により辞書順にソートされ、辞書順で最初のConsumerがMaster Consumerとしてメッセージを受信します。

このConsumerが切断された場合、Ackが返ってきていないものとその後に続く全てのメッセージは辞書順で次のConsumerに配送されます。

上図では、**Consumer-C-1** はMaster Consumerであり、**Consumer-C-2** は **Consumer-C-1** が切断した時にメッセージを受け取ります。

### パーティションドトピック

{% include explanations/ja/partitioned-topics.md %}

### ノンパーシステントトピック

{% include explanations/ja/non-persistent-topics.md %}

## アーキテクチャの概要

上位レベルでは、Pulsar{% popover_ja インスタンス %}は単一または複数の{% popover_ja クラスタ %}で構成されます。インスタンス内のクラスタはそれぞれの間でデータを[レプリケーション](#レプリケーション)できます。

Pulsarクラスタ内で:

* 1つ以上の{% popover_ja Broker %}は{% popover_ja Producer %}から送信されるメッセージを処理し、ロードバランシングし、{% popover_ja Consumer %}にメッセージを送信します。また、様々な調整タスクを処理するため{% popover_ja Configuration Store %}と通信し、{% popover_ja BookKeeper %}インスタンス ({% popover_ja Bookie %}) にメッセージを永続化します。クラスタ特有の情報は{% popover_ja ZooKeeper %}クラスタから取得します。
* {% popover_ja BookKeeper %}クラスタは1つ以上の{% popover_ja Bookie %}で構成され、メッセージの[永続ストレージ](#永続ストレージ)を制御します。
* {% popover_ja ZooKeeper %}クラスタはそのクラスタの固有の情報を制御します。

以下の図はPulsarクラスタの説明です:

![アーキテクチャ略図](/img/pulsar_system_architecture.png)

より広範な{% popover_ja インスタンス %}レベルでは、{% popover_ja Configuration Store %}と呼ばれるインスタンス全体のZooKeeperクラスタは、[ジオレプリケーション](#レプリケーション)などの複数のクラスタを横断した調整タスクを処理します。

## Broker

Pulsarの{% popover_ja Broker %}はステートレスなコンポーネントで、主に2つの異なるコンポーネントを実行します:

* ProducerやConsumerのための[トピックのルックアップ](#クライアントセットアップフェイズ)と[管理操作](../../reference/RestApi)のためのRESTインターフェースを提供するHTTPサーバ
* 全てのデータ転送を独自[バイナリプロトコル](../../project/BinaryProtocol)上で行う非同期TCPサーバである{% popover_ja ディスパッチャ %}

パフォーマンスのために、バックログがキャッシュサイズを超えない限り、メッセージは通常[Managed Ledger](#managed-ledger)のキャッシュから送信され、超えた場合にはBrokerが{% popover_ja BookKeeper %}からエントリの読み出しを始めます。

最後に、{% popover_ja ジオレプリケーション %}をサポートするために、Brokerはレプリケータを管理します。このレプリケータはPulsarクライアントライブラリを利用し、ローカルクラスタ内で発行されるエントリをtailし[Javaクライアントライブラリ](../../clients/Java)を使ってリモートクラスタに再発行します。

{% include admonition.html type="info" content="Pulsar Brokerの管理ガイドとして、[クラスタとBroker](../../admin/ClustersBrokers#brokerの管理)をご確認ください。" %}

## クラスタ

Pulsarの{% popover_ja インスタンス %}は1つ以上のクラスタで構成されています。クラスタは以下で構成されています:

* 1つ以上のPulsar [Broker](#broker)
* クラスタレベルの設定のために使用される{% popover_ja ZooKeeper %}
* メッセージの[永続ストレージ](#永続ストレージ)のために使用されるBookieのアンサンブル

クラスタは[ジオレプリケーション](#レプリケーション)を用いて相互にメッセージを複製できます。

{% include admonition.html type="info" content="Pulsarクラスタの管理ガイドとして、[クラスタとBroker](../../admin/ClustersBrokers#クラスタの管理)をご確認ください。" %}

### Globalクラスタ

どのPulsar{% popover_ja インスタンス %}にも、特定のクラスタに限らないネームスペース、トピックを管理するための`global`と呼ばれるインスタンス全体のクラスタが存在します。`global`クラスタはインスタンスの最初のクラスタの[メタデータを初期化する](../../admin/ClustersBrokers#クラスタメタデータの初期化)時に自動的に作成されます。

グローバルなトピックの名前は次の基本的な構造を持っています (`global`クラスタに注意してください) :

{% include topic.html p="my-property" c="global" n="my-namespace" t="my-topic" %}

## メタデータストア

Pulsarはメタデータストレージ、クラスタの設定と協調のために[Apache Zookeeper](https://zookeeper.apache.org/)を利用しています。Pulsarのインスタンスで:

* {% popover_ja Configuration Store %}は{% popover_ja プロパティ %}、{% popover_ja ネームスペース %}、グローバルに一貫性が求められるその他のエンティティの情報を保存します。
* 各クラスタはそのクラスタ自身のLocal ZooKeeperアンサンブルにオーナーシップのメタデータ、Brokerのロードレポート、BookKeeperの{% popover_ja Ledger %}のメタデータなどの{% popover_ja クラスタ %}特有の情報を保存します。

[新しいクラスタ](../../admin/ClustersBrokers#クラスタメタデータの初期化)を作る時

## 永続ストレージ

![BrokerとBookie](/img/broker-bookie.png)

Pulsarはアプリケーションのためにメッセージの配信保証を提供しています。メッセージがPulsar {% popover_ja Broker %}に正しく到達した時、そのメッセージは意図された対象に配信されます。

この保証のためには{% popover_ja Consumer %}から{% popover_ja Ack %}を受け取っていないメッセージを、Ackを受け取るまで永続的に保存しておく必要があります。このメッセージングの方式は*永続メッセージング*と呼ばれます。Pulsarでは、全てのメッセージのN個のコピーが保存されディスクに同期されます。例えば、2つのサーバを横断した4つのコピーがミラーリングされた[RAID](https://ja.wikipedia.org/wiki/RAID)ボリュームに保存されます。

Pulsarは[Apache BookKeeper](http://bookkeeper.apache.org/)と呼ばれるシステムを永続ストレージとして利用しています。BookKeeperは分散型[ログ先行書き込み](https://ja.wikipedia.org/wiki/%E3%83%AD%E3%82%B0%E5%85%88%E8%A1%8C%E6%9B%B8%E3%81%8D%E8%BE%BC%E3%81%BF) (WAL) システムです。このシステムはPulsarにとって以下の利点があります:

* Pulsarは[Ledger](#ledger)と呼ばれる多くの独立したログを使用可能になります。使用されるに連れて{% popover_ja トピック %}のために複数のLedgerが作成されます。
* エントリの複製を処理するシーケンシャルデータにとって非常に効率的なストレージを提供します。
* 様々なシステムの障害が発生した場合でも、Ledgerの読み取りの一貫性を保証します。
* Bookie間での均等なI/Oの分散を提供します。
* キャパシティとスループットの両方で水平スケーラブルです。キャパシティはクラスタに{% popover_ja Bookie %}を追加することで直ちに増加可能です。
* {% popover_ja Bookie %}は数千のLedgerを読み取りと書き取り同時に操作するようデザインされています。複数のディスク装置を使うことによって (1つはJournal、もう1つは一般的なストレージ)、Bookieは読み取り操作の影響を現在進行中の書き込み操作のレイテンシから分離させることができます。

メッセージデータに加えて、*カーソル* もまたBookKeeperに永続的に保存されます。カーソルは{% popover_ja Consumer %}の{% popover_ja サブスクリプション %}の購読位置です。BookKeeperによって、PulsarはConsumerの購読位置をスケーラブルな方法で保存可能です。

現時点ではPulsarは永続的なメッセージストレージのみをサポートしています。これは全ての{% popover_ja トピック %}名の先頭の`persistent`を表しています:

{% include topic.html p="my-property" c="global" n="my-namespace" t="my-topic" %}

近い将来、Pulsarは一時的なメッセージストレージをサポートします。

### Ledger

{% popover_ja Ledger %}は複数のBookKeeperのストレージノード ({% popover_ja Bookie %}) に割り当てられた1つのwriterを持つ追加専用のデータ構造です。Ledgerのエントリは複数のBookieに複製されます。Ledger自体は非常にシンプルなセマンティクスを持ちます:

* PulsarのBrokerはLedgerの作成、Ledgerへのエントリの追加、Ledgerのクローズが可能です。
* Ledgerがクローズ (明示的に、あるいはwriterのクラッシュのよる場合の両方) された後、それは読み取り専用モードでのみオープン可能です。
* Ledger内のエントリが不要になったら、システムからそのLedger自体を削除できます (全てのBookie上から)。

#### Ledgerの読み取り一貫性

BookKeeperの主な強みは、障害発生時のLedgerの読み取りの一貫性を保証することです。  
Ledgerは単一のプロセスによってのみ書き込み可能なので、プロセスは非常に効率よく自由にエントリを追加することができ (追加のコンセンサスが不要) 、障害後には、Ledgerは状態を確定しログに最後にコミットされたエントリを確定するリカバリープロセスを実行します。  
その後、Ledgerの全てのreaderは全く同じ内容を参照することが保証されます。

#### Managed Ledger

BookKeeperのLedgerが単一のログ抽象化を提供するため、単一トピックのストレージ層を表す *Managed Ledger* と呼ばれるLedgerの上位概念を表すライブラリが開発されました。Managed Ledgerは、ストリームの最後に追加し続ける単一のwriterとストリームをconsumeしている複数{% popover_ja カーソル %} (それぞれが独自の関連位置を保持します) を持つメッセージストリームの抽象化を表します。

内部的には、1つのManaged Ledgerはデータを保持するために複数のBookKeeperのLedgerを使います。  
複数のLedgerを持つのには2つの理由があります:

1. 障害後にLedgerはもはや書き込めないため新しいLedgerを作成する必要があるため
2. すべてのカーソルがLedgerに含まれるすべてのメッセージをconsumeした時にLedgerを削除する可能性があるため定期的にLedgerをロールオーバーしたいため

### Journalストレージ

BookKeeperでは、*Journal* ファイルはBookKeeperのトランザクションログを含みます。[Ledger](#ledger)をアップデートする前に、Bookieは更新を記述するトランザクションが永続 (揮発性ではない) ストレージに書き込まれることを保証する必要があります。Bookieが起動する、あるいは古いJournalファイルがJournalファイルサイズの上限 ([`journalMaxSizeMB`](../../reference/Configuration#bookkeeper-journalMaxSizeMB)パラメータで設定可能) に達すると、新しいJournalファイルが作成されます。

### 非永続ストレージ

BookKeeperの将来的なバージョンでは *非永続ストレージ* をサポートし、複数の耐久モードをトピックレベルでサポートします。これにより、トピックレベルでの耐久モードが設定可能になり、トピック名の`persistent`に代わり`non-persistent`識別子が利用可能になります。

## レプリケーション

Pulsarでは、異なった地理的場所でメッセージをproduceしてconsumeできます。 たとえば、アプリケーションがある地域やマーケットにデータをproduceし、他の地域やマーケットでconsumeするために処理したい場合があります。 Pulsarの[ジオレプリケーション](../../admin/GeoReplication)を使用することで、それが可能になります。

## マルチテナンシー

Pulsarは{% popover_ja マルチテナント %}のシステムとして一から作られました。マルチテナンシーをサポートするために、Pulsarは{% popover_ja プロパティ %}という概念を持ちます。プロパティはクラスタを横断して利用可能であり、それぞれに適用された[認証と認可](../../admin/Authz)のスキームを持ちます。それらは[ストレージ割当](TODO)、[メッセージTTL](TODO)、[隔離ポリシー](TODO)を管理できる管理単位です。

Pulsarのマルチテナントの性質はトピックURLに目に見えて反映されています。それらは以下のような構造です:

{% include topic.html p="property" c="cluster" n="namespace" t="topic" %}

このように、プロパティはトピックの分類の最も基本的な単位です ({% popover_ja クラスタ %}よりもさらに基本的です)。

### プロパティとネームスペース

{% include explanations/ja/properties-namespaces.md %}

## 認証と認可

PulsarはBrokerで設定可能であるプラガブルな[認証](../../admin/Authz)機構を備えており、クライアントを特定してトピックとプロパティに対するアクセス権限を確認するための[認可](../../admin/Authz#認可)もサポートしています。

## クライアントインターフェース

Pulsarは[Java](../../clients/Java)と[C++](../../clients/Cpp)でクライアントAPIを公開しています。クライアントAPIはクライアントとBroker間の通信プロトコルを最適化、カプセル化し、アプリケーションで使用するためのシンプルかつ直感的なAPIを提供しています。

現在の公式Pulsarのクライアントライブラリは{% popover_ja Broker %}に対する透過的な再接続と接続フェイルオーバー、Brokerによる{% popover_ja Ack %}を受け取るまでのメッセージのキューイング、バックオフによるリトライなどのヒューリスティックを備えています。

{% include admonition.html type="success" title="カスタムクライアントライブラリ" content="
もしクライアントライブラリを作成したい場合は、Pulasrのカスタム[バイナリプロトコル](../../project/BinaryProtocol)のドキュメントをご確認ください。
" %}

### クライアントセットアップフェイズ

アプリケーションがProducer/Consumerを作成したい時、Pulsarクライアントライブラリは2つのステップで構成されるセットアップフェイズを開始します:

1. クライアントは{% popover_ja Broker %}にHTTPルックアップリクエストを送って、トピックのオーナーを特定しようとします。リクエストは1つのアクティブなBrokerに到達し、(キャッシュされた) ZooKeeperのメタデータを参照することによって、どのBrokerがトピックを提供しているのかを知ることができ、誰もそのトピックを提供していない場合は最も負荷の少ないBrokerにトピックを割り当てます。
1. 一度クライアントライブラリがBrokerのアドレスを取得すると、TCPコネクションが作成され (もしくは既存のコネクションプールから再利用され) 認証されます。この接続内で、クライアントとBrokerはカスタムプロトコルのバイナリコマンドを交換します。この時点で、クライアントはProducer/Consumerを作成するためのコマンドをBrokerに送信します。Brokerは認可ポリシーが検証された後にこのコマンドに応じます。

TCP接続が切断された時は、クライアントは直ちにセットアップフェイズを再実行し、操作が成功するまでExponential Backoffを続けProducerやConsumerを再確立します。

## サービスディスカバリ

Pulsarの{% popover_ja Broker %}に接続する[クライアント](../../getting-started/Clients)は単一のURLを使ってPulsarの{% popover_ja インスタンス %}全体と通信できる必要があります。Pulsarはビルトインのサービスディスカバリ機構を提供しています。これは、[Pulsarインスタンスのデプロイ](../../deployment/InstanceSetup#サービスディスカバリのセットアップ)ガイドを見ることでセットアップできます。

ユーザ自身によるサービスディスカバリシステムを使うことができます。独自のシステムを利用する場合は、クライアントが`http://pulsar.us-west.example.com:8080`のようなエンドポイントにHTTPリクエストを送信した際、クライアントは目的の{% popover_ja クラスタ %}内の *いずれかの* アクティブなBrokerにDNS, HTTP, IP, その他の方法を用いてリダイレクトされる必要があります。
