---
title: Spark Streaming Pulsar Receiver
---

<!-- TOC depthFrom:2 depthTo:3 withLinks:1 updateOnSave:1 orderedList:0 -->

- [概要](#概要)
- [Spark Streaming Pulsar Receiverの利用](#spark-streaming-pulsar-receiverの利用)
- [実装例](#実装例)

<!-- /TOC -->

## 概要
Spark Streaming Pulsar ReceiverはApache Spark StreamingがPulsarからデータを受け取るためのCustom Receiverです。

アプリケーションはSpark Streaming Pulsar Receiverを通してRDD形式のデータを受け取り、様々な処理を行うことができます。

## Spark Streaming Pulsar Receiverの利用
Spark Streaming Pulsar Receiverの依存をincludeします:

```xml
<dependency>
  <groupId>org.apache.pulsar</groupId>
  <artifactId>pulsar-spark</artifactId>
  <version>${pulsar.version}</version>
</dependency>
```

JavaStreamingContextのreceiverStreamメソッドにSparkStreamingPulsarReceiverのインスタンスを渡します:
```java
SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("pulsar-spark");
JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

ClientConfiguration clientConf = new ClientConfiguration();
ConsumerConfiguration consConf = new ConsumerConfiguration();
String url = "pulsar://localhost:6650/";
String topic = "persistent://sample/standalone/ns1/topic1";
String subs = "sub1";

JavaReceiverInputDStream<byte[]> msgs = jssc
        .receiverStream(new SparkStreamingPulsarReceiver(clientConf, consConf, url, topic, subs));
```


## 実装例
完全な実装の例は[SparkStreamingPulsarReceiver.java](../../../pulsar-spark/src/test/java/org/apache/pulsar/spark/example/SparkStreamingPulsarReceiverExample.java)を参照してください。
この例では、受け取ったメッセージのうち"Pulsar"という文字列が含まれるものがいくつあるかを数えます。
