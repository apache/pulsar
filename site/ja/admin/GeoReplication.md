---
title: Pulsarのジオレプリケーション
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

*ジオレプリケーション*とは、Pulsar{% popover_ja インスタンス %}の複数の{% popover_ja クラスタ %}を横断して永続的に保存されたメッセージをレプリケーションすることです。

## どのように動作するか

以下の図はPulsarクラスタを横断したジオレプリケーションについて説明します:

![レプリケーション図](/img/GeoReplication.png)

この図では、Producer **P1**, **P2**, **P3**が**Cluster-A**, **Cluster-B**, **Cluster-C**のトピック**T1**にメッセージを発行する際、それぞれのメッセージはクラスタを横断して瞬時に複製されます。複製されると、Consumer **C1**, **C2**はそれらのメッセージをそれぞれのクラスタからconsumeできます。

ジオレプリケーションなしでは、Consumer **C1**と**C2**はProducer **P3**によって発行されたメッセージをconsumeできません。

## ジオレプリケーションとPulsarプロパティ

ジオレプリケーションを使用するために、Pulsarの{% popover_ja プロパティ %}単位の設定をする必要があります。両方のクラスタにアクセスできるプロパティが作成されている場合にのみ、クラスタ間でジオレプリケーションを有効にすることができます。

ジオレプリケーションは2つのクラスタ間で有効にする必要がありますが、実際には{% popover_ja ネームスペース %}単位で管理されています。ネームスペースのジオレプリケーションを有効にするには、以下の操作を行う必要があります:

* [グローバルネームスペースを作成する](#グローバルネームスペースの作成)
* 2つ以上のプロビジョニングされたクラスタ間で複製するようにネームスペースを設定する

そのネームスペースの*いかなる*トピックに発行されたいかなるメッセージも指定された全てのクラスタにレプリケートされます。

## ローカル永続化と転送

Pulsarトピックにメッセージがproduceされると、まずローカルクラスタに永続化され、リモートクラスタに非同期に転送されます。

通常、接続に問題がない場合、メッセージはローカルConsumerに配送されると同時に、すぐに複製されます。通常、エンドツーエンドの配信待ち時間は、リモートリージョン間のネットワーク[ラウンドトリップタイム](https://ja.wikipedia.org/wiki/%E3%83%A9%E3%82%A6%E3%83%B3%E3%83%89%E3%83%88%E3%83%AA%E3%83%83%E3%83%97%E3%82%BF%E3%82%A4%E3%83%A0) (RTT) によって定義されます。

アプリケーションは、リモートクラスタに到達できない場合 (ネットワークパーティションの間など) にも、いずれかのクラスタでProducerとConsumerを作成できます。

{% include message-ja.html id="subscriptions_local" %}

上図の例で、トピック**T1**は**Cluster-A**, **Cluster-B**, **Cluster-C**の3クラスタ間でレプリケートされます。

任意のクラスタで生成されたすべてのメッセージは、他のすべてのクラスタ内のすべてのサブスクリプションに配信されます。この場合、Consumer **C1**および**C2**は、Producer **P1**, **P2**, **P3**によって発行されたすべてのメッセージを受信します。 順序はProducerごとに保証されています。

## レプリケーションの設定

[上記](#ジオレプリケーションとpulsarプロパティ)で述べたように、Pulsarのジオレプリケーションは{% popover_ja プロパティ %}単位の設定が必要です。

### プロパティにパーミッションを与える

クラスタへのレプリケーションを確立するために、{% popover_ja テナント %}はそのクラスタを利用するパーミッションが必要です。このパーミッションはプロパティの作成時あるいは作成後に付与することができます。

作成時に、必要なクラスタを指定するには以下のようにします:

```shell
$ bin/pulsar-admin properties create my-property \
  --admin-roles my-admin-role \
  --allowed-clusters us-west,us-east,us-cent
```

既存プロパティのパーミッションの更新を行うには、`create`の代わりに`update`を利用します。

### グローバルネームスペースを作成する

レプリケーションは*グローバル*トピックで使用する必要があります。つまり、グローバル{% popover_ja ネームスペース %}に属し、特定のクラスタに結びついていないトピックを意味します。

グローバルネームスペースは`global`という仮想のクラスタに作成される必要があります。例えば以下のように作成します:

```shell
$ bin/pulsar-admin namespaces create my-property/global/my-namespace
```

最初はネームスペースはどのクラスタにも割り当てられていません。`set-clusters`サブコマンドを使用して、ネームスペースをクラスタに割り当てることができます:

```shell
$ bin/pulsar-admin namespaces set-clusters my-property/global/my-namespace \
  --clusters us-west,us-east,us-cent
```

ネームスペースのレプリケーションクラスタは、進行中のトラフィックを中断することなく、いつでも変更できます。レプリケーションチャネルは、設定が変更されるとすぐにすべてのクラスタで即座にセットアップまたは停止されます。

### グローバルトピックの使用

グローバルネームスペースを作成すると、そのネームスペース内でProducerまたはConsumerが作成するトピックはすべてグローバルになります。通常、各アプリケーションはローカルクラスタに対する `serviceUrl`を使用します。

#### 選択的なレプリケーション

デフォルトでは、メッセージはネームスペースに設定されたすべてのクラスタに複製されます。メッセージに対してレプリケーションリストを指定することによって、レプリケーションを選択的に制限することができます。メッセージは、レプリケーションリストのサブセットにのみレプリケートされます。

以下は[Java API](../../clients/Java)での一例です。{% javadoc Message client com.yahoo.pulsar.client.api.Message %}オブジェクトの生成時に`setReplicationClusters`メソッドを使用することに注意してください:

```java
List<String> restrictReplicationTo = new ArrayList<>;
restrictReplicationTo.add("us-west");
restrictReplicationTo.add("us-east");

Message message = MessageBuilder.create()
        .setContent("my-payload".getBytes())
        .setReplicationClusters(restrictReplicationTo)
        .build();

producer.send(message);
```

#### トピックの統計情報

グローバルトピックのトピック別の統計情報は、[`pulsar-admin`](../../reference/CliTools#pulsar-admin)ツールと[REST API](../../reference/RestApi)から利用可能です:

```shell
$ bin/pulsar-admin persistent stats persistent://my-property/global/my-namespace/my-topic
```

各クラスタは、受信と送信に関するレプリケーションレートとバックログを含むローカル統計情報をレポートします。

#### グローバルトピックの消去

グローバルトピックが複数のリージョンに存在する場合、グローバルトピックを直接削除することはできません。その代わり、自動的なトピックガベージコレクションを利用できます。

Pulsarでは、トピックが使用されなくなったとき、つまりProducerまたはConsumerが接続されておらず、*さらに*サブスクリプションが存在しない場合にトピックが自動的に削除されます。グローバルトピックの場合、各リージョンはフォールトトレラントメカニズムを使用して、いつトピックをローカルで削除するのが安全かを判断します。

グローバルトピックを削除するには、トピックのすべてのProducerとConsumerを閉じて、すべてのレプリケーションクラスタでそのローカルサブスクリプションをすべて削除します。
Pulsarは、トピックの有効なサブスクリプションがシステムに残っていないと判断すると、そのトピックをガベージコレクトします。
