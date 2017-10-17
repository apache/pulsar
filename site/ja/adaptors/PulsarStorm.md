---
title: Apache StormのためのPulsarアダプタ
tags_ja: [storm, java]
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

Pulsar Stormは[Apache Storm](http://storm.apache.org/) Topologyと連携するためのアダプタであり、データの送受信を行うためのStormの実装を提供します。

アプリケーションはPulsar Spoutを通してStorm Topologyにデータを注入したり、Pulsar Boltを通してStorm Topologyからデータを取り出すことができます。

## Pulsar Stormアダプタの利用

Pulsar Stormアダプタの依存を含めます:

```xml
<dependency>
  <groupId>org.apache.pulsar</groupId>
  <artifactId>pulsar-storm</artifactId>
  <version>${pulsar.version}</version>
</dependency>
```

## Pulsar Spout

Pulsar Spoutを利用して、Pulsarの{% popover_ja トピック %}上に発行されたデータをStorm Topologyがconsumeできます。Pulsar Spoutは受信したメッセージとクライアントが提供する`MessageToValuesMapper`をもとにStorm Tupleを発行します。

下流のBoltでの処理に失敗したTupleはSpoutによって再注入されます。この再注入は指数バックオフに従い、設定可能なタイムアウト(デフォルト:60秒)またはリトライ回数のどちらかに達するまで行われます。その後、Consumerによって{% popover_ja Ack %}(確認応答)が返されます。下記はSpoutの例です:

```java
// Pulsarクライアントの設定
ClientConfiguration clientConf = new ClientConfiguration();

// Pulsar Consumerの設定
ConsumerConfiguration consumerConf = new ConsumerConfiguration();

@SuppressWarnings("serial")
MessageToValuesMapper messageToValuesMapper = new MessageToValuesMapper() {

    @Override
    public Values toValues(Message msg) {
        return new Values(new String(msg.getData()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // 出力フィールドを宣言
        declarer.declare(new Fields("string"));
    }
};

// Pulsar Spoutの設定
PulsarSpoutConfiguration spoutConf = new PulsarSpoutConfiguration();
spoutConf.setServiceUrl("pulsar://broker.messaging.usw.example.com:6650");
spoutConf.setTopic("persistent://my-property/usw/my-ns/my-topic1");
spoutConf.setSubscriptionName("my-subscriber-name1");
spoutConf.setMessageToValuesMapper(messageToValuesMapper);

// Pulsar Spoutインスタンスの生成
PulsarSpout spout = new PulsarSpout(spoutConf, clientConf, consumerConf);
```

## Pulsar Bolt

Pulsar Boltを利用して、Storm Topology内のデータをPulsarの{% popover_ja トピック %}に発行できます。Pulsar Boltは受け取ったStorm Tupleとクライアントが提供する`TupleToMessageMapper`をもとにメッセージを発行します。

異なるトピックにメッセージを発行するためにパーティションドトピックを利用することもできます。その場合`TupleToMessageMapper`の実装において、メッセージに「キー」を用意する必要があります。同じキーを持つメッセージは同じトピックに送信されるようになります。下記はBoltの例です:

```java
// Pulsarクライアントの設定
ClientConfiguration clientConf = new ClientConfiguration();

// Pulsar Producerの設定
ProducerConfiguration producerConf = new ProducerConfiguration();

@SuppressWarnings("serial")
TupleToMessageMapper tupleToMessageMapper = new TupleToMessageMapper() {

    @Override
    public Message toMessage(Tuple tuple) {
        String receivedMessage = tuple.getString(0);
        // メッセージを処理
        String processedMsg = receivedMessage + "-processed";
        return MessageBuilder.create().setContent(processedMsg.getBytes()).build();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // 出力フィールドを宣言
    }
};

// Pulsar Boltの設定
PulsarBoltConfiguration boltConf = new PulsarBoltConfiguration();
boltConf.setServiceUrl("pulsar://broker.messaging.usw.example.com:6650");
boltConf.setTopic("persistent://my-property/usw/my-ns/my-topic2");
boltConf.setTupleToMessageMapper(tupleToMessageMapper);

// Pulsar Boltインスタンスの生成
PulsarBolt bolt = new PulsarBolt(boltConf, clientConf);
```

## 実装例

完全な実装の例は[こちら]({{ site.pulsar_repo }}/pulsar-storm/src/test/java/org/apache/pulsar/storm/example/StormExample.java)を参照してください。
