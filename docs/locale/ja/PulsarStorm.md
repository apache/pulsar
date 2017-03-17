# Apache StormのためのPulsarアダプタ

<!-- TOC depthFrom:2 depthTo:3 withLinks:1 updateOnSave:1 orderedList:0 -->

- [概要](#概要)
- [Pulsar Stormアダプタの利用](#pulsar-stormアダプタの利用)
	- [Pulsar Spout](#pulsar-spout)
	- [Pulsar Bolt](#pulsar-bolt)
- [実装例](#実装例)

<!-- /TOC -->

## 概要
Pulsar StormはApache Storm Topologyと連携するためのアダプタであり、データの送受信を行うためのStromの実装を提供します。

アプリケーションはPulsar Spoutを通してStorm Topologyにデータを注入したり、Pulsar Boltを通してStorm Topologyからデータを取り出すことができます。

## Pulsar Stormアダプタの利用
Pulsar Stormアダプタの依存をincludeします:

```xml
<dependency>
  <groupId>com.yahoo.pulsar</groupId>
  <artifactId>pulsar-storm</artifactId>
  <version>${pulsar.version}</version>
</dependency>
```

## Pulsar Spout
Pulsar Spoutを使って、Storm Topologyは[Pulsarのトピック](Architecture.md#トピック)に発行されたデータをconsumeできます。Pulsar Spoutは受信したメッセージとクライアントが提供するMessageToValuesMapperをもとにStorm Tupleを発行します。

下流のBoltでの処理に失敗したTupleはSpoutから再注入されます。この再注入は指数バックオフアルゴリズムに従い、設定可能なタイムアウト (デフォルト: 60秒) または設定可能なリトライ回数のどちらかに達するまで行われます。その後、Consumerにより処理成功の応答が返されます。

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
spoutConf.setServiceUrl("http://broker.messaging.usw.example.com:8080");
spoutConf.setTopic("persistent://my-property/usw/my-ns/my-topic1");
spoutConf.setSubscriptionName("my-subscriber-name1");
spoutConf.setMessageToValuesMapper(messageToValuesMapper);

// Pulsar Spoutの作成
PulsarSpout spout = new PulsarSpout(spoutConf, clientConf, consumerConf);
```

## Pulsar Bolt
Pulsar Boltを使って、Storm Topology内のデータを[Pulsarのトピック](Architecture.md#トピック)に発行できます。Pulsar Boltは受信したStorm Tupleとクライアントが提供するTupleToMessageMapperをもとにメッセージを発行します。

異なるトピックのメッセージを発行するためにパーティションドトピックを利用できます。その場合TupleToMessageMapperの実装において、メッセージに「キー」を用意する必要があります。同じキーを持つメッセージは同じトピックに送信されるようになります。

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
boltConf.setServiceUrl("http://broker.messaging.usw.example.com:8080");
boltConf.setTopic("persistent://my-property/usw/my-ns/my-topic2");
boltConf.setTupleToMessageMapper(tupleToMessageMapper);
        
// Pulsar Boltの作成
PulsarBolt bolt = new PulsarBolt(boltConf, clientConf);
```

## 実装例
完全な実装の例は[StormExample.java](../../../pulsar-storm/src/test/java/com/yahoo/pulsar/storm/example/StormExample.java)を参照してください。
