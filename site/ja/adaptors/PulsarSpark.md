---
title: Spark Streaming Pulsar Receiver
tags_ja: [spark, streaming, java]
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

完全な実装の例は[こちら]({{ site.pulsar_repo }}/tests/pulsar-spark-test/src/test/java/org/apache/pulsar/spark/example/SparkStreamingPulsarReceiverExample.java)を参照してください。
この例では、受け取ったメッセージのうち"Pulsar"という文字列が含まれているメッセージ数をカウントしています。
