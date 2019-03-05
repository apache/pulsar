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

{% include admonition.html type="success" title='注意' content="
この機能はまだ実験モードであり、実装の詳細は将来のリリースで変更される可能性があります。
" %}

その名前が示すように、メッセージを複数のディスクに永続化するパーシステント (永続) トピックとは異なり、ノンパーシステント (非永続) トピックはメッセージをストレージディスクに永続化しません。

したがって、永続的な配信を使用している場合、メッセージはディスクやデータベースに永続化されるため、Brokerの再起動やSubscriberのフェイルオーバーに耐えられるようになります。非永続的な配信を使用している間にBrokerがダウンしたりSubscriberが切断されたりすると、Subscriberは全ての転送中のメッセージを失います。そのため、ノンパーシステントトピックではクライアントはメッセージを失う事があります。

- ノンパーシステントトピックでは、Brokerが発行されたメッセージを受け取ると、いかなるストレージにも永続化する事なく即座にそのメッセージを接続された全てのSubscriberに配信します。したがってSubscriberとBrokerの接続が切断されると、Brokerは転送中のメッセージを配信できず、Subscriberはそれらのメッセージをもう一度受け取る事はできません。また、Consumerがメッセージをconsumeするのに十分なパーミッションを持っていないか、ConsumerのTCPチャネルが書き込み可能でない場合も、BrokerはConsumer用のメッセージをドロップします。したがって、Consumerのメッセージドロップを避けるために、Consumerの受信キュー (十分なパーミッションを得るため) とTCPの受信ウィンドウサイズ (チャネルを書き込み可能にするため) は適切に設定されるべきです。
- Brokerは、クライアントの接続ごとに設定された数の転送中メッセージしか許可しません。したがって、Producerがこれよりも高い割合でメッセージを発行しようとすると、Brokerは新しく受信したメッセージをSubscriberに配信する事なくサイレントにドロップします。ただし、BrokerはProducerにメッセージドロップを通知するために、ドロップされたメッセージに対して特殊なメッセージID (`msg-id: -1:-1`) でAck (確認応答) を返します。

### パフォーマンス

通常、非永続メッセージングは永続メッセージングよりも高速です。これは、Brokerがメッセージを永続化せず、接続された全てのSubscriberにメッセージを配信すると即座にProducerにAckを返すためです。したがって、ノンパーシステントトピックではProducerの発行レイテンシは比較的低くなります。

## クライアントAPI

トピック名は次のようになります:

```
non-persistent://my-property/us-west/my-namespace/my-topic
```

ProducerとConsumerは、トピック名を`non-persistent`で始めなければならない点を除き、パーシステントトピックと同様の方法でノンパーシステントトピックに接続できます。

ノンパーシステントトピックは3つの異なるサブスクリプションモード (**Exclusive**, **Shared**, **Failover**) を全てサポートしています。サブスクリプションモードの詳細は[入門](../../getting-started/ConceptsAndArchitecture)で既に解説しています。

### Consumer API

```java
PulsarClient client = PulsarClient.create("pulsar://localhost:6650");

Consumer consumer = client.subscribe(
            "non-persistent://sample/standalone/ns1/my-topic",
            "my-subscribtion-name");
```

### Producer API

```java
PulsarClient client = PulsarClient.create("pulsar://localhost:6650");

Producer producer = client.createProducer(
            "non-persistent://sample/standalone/ns1/my-topic");
```

### Brokerの設定

多くの場合、ノンパーシステントトピック提供するためだけにクラスタ内に専用のBrokerを設定する必要はほとんどないでしょう。

Brokerが特定のタイプのトピックのみを所有できるようにするための設定は次の通りです:

```
# Brokerによるパーシステントトピックのロードを無効化
enablePersistentTopics=false
# Brokerによるノンパーシステントトピックのロードを有効化
enableNonPersistentTopics=true
```
