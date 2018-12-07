---
title: パーティションドトピック
lead: トピック内の負荷を分散させる事でメッセージのスループットを向上
tags_ja:
- topics
- partitioning
- admin 
- clients
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

デフォルトでは、Pulsarの{% popover_ja トピック %}は単一の{% popover_ja Broker %}によって提供されます。しかし単一のBrokerを使用すると、トピックの最大スループットが制限されてしまいます。*パーティションドトピック*は複数のBrokerに跨らせる事のできる特殊なトピックであり、より高いスループットの実現を可能にします。パーティションドトピックがどのように動作するかの説明は下記の[コンセプト](#コンセプト)のセクションを参照してください。

Pulsarのクライアントライブラリを使用する事でパーティションドトピックにメッセージを[発行](#パーティションドトピックへのメッセージの発行)できます。また、Pulsarの[admin API](../../admin/AdminInterface)を使用する事でパーティションドトピックの[作成と管理](#パーティションドトピックの管理)ができます。

## パーティションドトピックへのメッセージの発行

パーティションドトピックにメッセージを発行する場合、通常のトピックとの唯一の違いは新しい{% popover_ja Producer %}を作成する時に[ルーティングモード](../../getting-started/ConceptsAndArchitecture#ルーティングモード)を指定する必要がある事です。[Java](#java)の例を以下に示します。

### Java

Javaクライアントのパーティションドトピックへのメッセージの発行は、[通常のトピックに対するメッセージの発行](../../clients/Java#producer)と同様に動作します。違いは、現在使用可能なメッセージルータまたはカスタムルータのどちらかを指定する必要がある事です。

#### ルーティングモード

Producerの設定に使用する{% javadoc ProducerConfiguration client org.apache.pulsar.client.api.ProducerConfiguration %}オブジェクトでルーティングモードの指定が可能です。選択肢は次の3つです:

* `SinglePartition`
* `RoundRobinPartition`
* `CustomPartition`

以下に例を示します:

```java
String pulsarBrokerRootUrl = "pulsar://localhost:6650";
String topic = "persistent://my-property/my-cluster-my-namespace/my-topic";

PulsarClient client = PulsarClient.create(pulsarBrokerRootUrl);
ProducerConfiguration config = new ProducerConfiguration();
config.setMessageRoutingMode(ProducerConfiguration.MessageRoutingMode.SinglePartition);
Producer producer = client.createProducer(topic, config);
producer.send("Partitioned topic message".getBytes());
```

#### カスタムメッセージルータ

カスタムメッセージルータを使用するためには、{% javadoc MessageRouter client org.apache.pulsar.client.api.MessageRouter %}インターフェースの実装を提供する必要があります。これには、`choosePartition`メソッド1つしかありません:

```java
public interface MessageRouter extends Serializable {
    int choosePartition(Message msg);
}
```

以下は、全てのメッセージをパーティション10にルーティングするルータ (あまり有用ではありません) です:

```java
public class AlwaysTenRouter implements MessageRouter {
    public int choosePartition(Message msg) {
        return 10;
    }
}
```

この実装を用いてメッセージを送信するには次のようにします:

```java
String pulsarBrokerRootUrl = "pulsar://localhost:6650";
String topic = "persistent://my-property/my-cluster-my-namespace/my-topic";

PulsarClient client = PulsarClient.create(pulsarBrokerRootUrl);
ProducerConfiguration config = new ProducerConfiguration();
config.setMessageRouter(AlwaysTenRouter);
Producer producer = client.createProducer(topic, config);
producer.send("Partitioned topic message".getBytes());
```


## Pulsar adminのセットアップ

{% include explanations/ja/admin-setup.md %}

## パーティションドトピックの管理

{% include explanations/ja/partitioned-topic-admin.md %}

## コンセプト

{% include explanations/ja/partitioned-topics.md %}
