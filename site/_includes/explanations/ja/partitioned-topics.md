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

通常のトピックは単一の{% popover_ja Broker %}によってのみ提供されるため、トピックの最大スループットが制限されます。*パーティションドトピック*は複数のBrokerによって処理される特殊なトピックであり、より高いスループットの実現を可能にします。

裏側では、パーティションドトピックはN個の内部トピックとして実装されます。ここでのNはパーティションの数です。パーティションドトピックにメッセージを発行すると、それぞれのメッセージは複数のBrokerの内のいずれかにルーティングされます。Broker間のパーティションの分散はPulsarによって自動的に処理されます。

次の図は以上の事を示したものです:

![パーティションドトピック](/img/pulsar_partitioned_topic.jpg)

ここでは、トピック**T1**は3つのBrokerにまたがる5つのパーティション (**P0**から**P4**) を持ちます。Brokerよりも多くのパーティションが存在するため、2つのBrokerが2つのパーティションを処理し、3つ目のBrokerは1つのパーティションしか処理しません (Pulsarはこのパーティションの分散を自動的に処理します) 。

このトピックに対するメッセージは2つの{% popover_ja Consumer %}に配信されます。[ルーティングモード](#ルーティングモード)によってそれぞれのパーティションをどのBrokerが処理するかが決定され、[サブスクリプションモード](../../getting-started/ConceptsAndArchitecture#サブスクリプションモード)によってどのメッセージがどのCondumerに行くかが決定されます。

ルーティングとサブスクリプションモードをどうするかという問題は、ほとんどの場合分けて考える事ができます。パーティショニング/ルーティングはスループットに対する懸念によって決まり、サブスクリプションはアプリケーションのセマンティクスによって決まるべきです。

サブスクリプションモードがどのように動作するかという点において、パーティションドトピックと通常のトピックに違いはありません。パーティショニングは、メッセージが{% popover_ja Producer %}によって発行される時と、{% popover_ja Consumer %}によって処理され{% popover_ja Ack %} (確認応答) が返される時の間に、何が起こるかだけを決定します。

パーティションドトピックは[admin API](../../admin/AdminInterface)から明示的に作成する必要があります。パーティション数はトピックを作成する際に指定できます。

#### ルーティングモード

パーティションドトピックにメッセージの発行を行う時、*ルーティングモード*を指定しなければなりません。ルーティングモードはどのパーティション---つまりどの内部トピック---に各メッセージを発行するかを決定します。

デフォルトでは利用可能なルーティングモードが3つ存在します:

モード | 説明 | 順序保証
:----|:------------|:------------------
キーハッシュ | メッセージにキープロパティが指定されていた場合、パーティションドProducerはキーのハッシュを計算し、それを特定のパーティションに割り当てます。 | キーバケットごとの保証
シングルデフォルトパーティション | キーが指定されていなかった場合、各Producerのメッセージは専用のパーティション (最初はランダムに選択) にルーティングされます。 | Producerごとの保証
ラウンドロビンディストリビューション | キーが指定されていなかった場合、最大スループットを達成するために全てのメッセージが異なるパーティションにラウンドロビン方式でルーティングされます。 | なし

これらデフォルトのモードに加えて、もし[Javaクライアント](../../clients/Java)を使用しているなら{% javadoc MessageRouter client com.yahoo.pulsar.client.api.MessageRouter %}インターフェースを実装する事で独自のルーティングモードを作成可能です。
