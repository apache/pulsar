---
title: Spark Streaming Pulsar Receiver
tags_ja: [spark, streaming, java]
---

Spark Streaming Pulsar ReceiverはApache [Spark Streaming](https://spark.apache.org/streaming/)がPulsarからデータを受け取るためのCustom Receiverです。

アプリケーションはSpark Streaming Pulsar Receiverを通して[Resilient Distributed Dataset](https://spark.apache.org/docs/latest/programming-guide.html#resilient-distributed-datasets-rdds) (RDD)形式のデータを受け取り、様々な処理を行うことができます。

## 前提条件
Receiverを利用するために、Javaの設定ファイルに`pulsar-spark`ライブラリの依存を含めます。

### Maven

Mavenの場合は、`pom.xml`に下記を追加します:

```xml
<!-- in your <properties> block -->
<pulsar.version>{{ site.current_version }}</pulsar.version>

<!-- in your <dependencies> block -->
<dependency>
  <groupId>org.apache.pulsar</groupId>
  <artifactId>pulsar-spark</artifactId>
  <version>${pulsar.version}</version>
</dependency>
```

### Gradle

Gradleの場合は、`build.gradle`に下記を追加します:

```groovy
def pulsarVersion = "{{ site.current_version }}"

dependencies {
    compile group: 'org.apache.pulsar', name: 'pulsar-spark', version: pulsarVersion
}
```

## 利用方法

`JavaStreamingContext`クラスの`receiverStream`メソッドに`SparkStreamingPulsarReceiver`インスタンスを渡します:

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

完全な実装の例は[こちら]({{ site.pulsar_repo }}/pulsar-spark/src/test/java/org/apache/pulsar/spark/example/SparkStreamingPulsarReceiverExample.java)を参照してください。
この例では、受け取ったメッセージのうち"Pulsar"という文字列が含まれているメッセージ数をカウントしています。
