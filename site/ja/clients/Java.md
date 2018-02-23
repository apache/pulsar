---
title: Pulsar Javaクライアント
tags_ja: [client, java]
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

PulsarのJavaクライアントでは{% popover_ja Producer %}と{% popover_ja Consumer %}の両方が利用可能であるだけでなく、[管理タスク](../../admin/AdminInterface)も実行可能です。

Javaクライアントの現在のバージョンは **{{ site.current_version }}** です。

PulsarクライアントのJavadocはパッケージ名によって2つに分割されています:

パッケージ | 説明
:-------|:-----------
[`org.apache.pulsar.client.api`](/api/client) | {% popover_ja Producer %}と{% popover_ja Consumer %}のAPI
[`org.apache.pulsar.client.admin`](/api/admin) | Java [admin API](../../admin/AdminInterface)

このドキュメントはPulsarの{% popover_ja トピック %}に対して、メッセージのproduceとconsumeを行うクライアントAPIのみに焦点を当てています。Java admin APIについては、[Pulsar admin API](../../admin/AdminInterface)をご確認ください。

## インストール

最新バージョンのPulsar Javaクライアントライブラリは[Maven Central](http://search.maven.org/#artifactdetails%7Corg.apache.pulsar%7Cpulsar-client%7C{{ site.current_version }}%7Cjar)から利用可能です。 最新のバージョンを使うために、ビルド設定に`pulsar-client`ライブラリを追加してください。

### Maven

Mavenを利用している場合は、以下を`pom.xml`に記述してください:

```xml
<!-- <properties>ブロック内 -->
<pulsar.version>{{ site.current_version }}</pulsar.version>

<!-- <dependencies>ブロック内 -->
<dependency>
  <groupId>org.apache.pulsar</groupId>
  <artifactId>pulsar-client</artifactId>
  <version>${pulsar.version}</version>
</dependency>
```

### Gradle

Gradleを利用している場合は、`build.gradle`ファイルに以下を記述してください:

```groovy
def pulsarVersion = '{{ site.current_version }}'

dependencies {
    compile group: 'org.apache.pulsar', name: 'pulsar-client', version: pulsarVersion
}
```

## 接続URL

{% include explanations/ja/client-url.md %}

## クライアントの設定

以下のように、{% javadoc PulsarClient client org.apache.pulsar.client.api.PulsarClient %}オブジェクトを接続先のPulsar{% popover_ja クラスタ %}のURLのみを用いて生成できます:

```java
String pulsarBrokerRootUrl = "pulsar://localhost:6650";
PulsarClient client = PulsarClient.create(pulsarBrokerRootUrl);
```

この`PulsarClient`オブジェクトはデフォルトの設定を使用します。デフォルト以外の設定を適用する方法はJavadocの{% javadoc ClientConfiguration client org.apache.pulsar.client.api.ClientConfiguration %}をご確認ください。

{% include admonition.html type="info" content="
クライアントレベルの設定に加えて、以下のセクションで説明する[Producer](#producerの設定)あるいは[Consumer](#consumerの設定)固有の設定も可能です。
" %}

## Producer

Pulsarでは{% popover_ja Producer %}は{% popover_ja メッセージ %}を{% popover_ja トピック %}に書き込みます。Producerのオブジェクトを生成するため、まずは{% javadoc PulsarClient client org.apache.pulsar.client.api.PulsarClient %}オブジェクトをPulsar {% popover_ja Broker %}のURLを用いて生成します。

```java
String pulsarBrokerRootUrl = "pulsar://localhost:6650";
PulsarClient client = PulsarClient.create(pulsarBrokerRootUrl);
```

{% include admonition.html type='info' title='スタンドアローンクラスタのデフォルトのBroker URL' content="
Pulsarを[スタンドアローンモード](../../getting-started/LocalCluster)で起動している場合、デフォルトでは`pulsar://localhost:6650`というURLでBrokerが利用できます。" %}

{% javadoc PulsarClient client org.apache.pulsar.client.api.PulsarClient %}オブジェクトを生成したら、{% popover_ja トピック %}に対して{% javadoc Producer client org.apache.pulsar.client.api.Producer %}オブジェクトを生成できます。

```java
String topic = "persistent://sample/standalone/ns1/my-topic";
Producer producer = client.createProducer(topic);
```

指定したBroker, トピックに対してメッセージを送信できます。

```java
// トピックに対して10個のメッセージを発行
for (int i = 0; i < 10; i++) {
    producer.send("my-message".getBytes());
}
```

{% include admonition.html type='warning' content="
Producer, Consumer, クライアントはそれらが必要ではなくなった時にクローズしてください:

```java
producer.close();
consumer.close();
client.close();
```

クローズ処理は非同期でも可能です:

```java
producer.asyncClose();
consumer.asyncClose();
clioent.asyncClose();
```
" %}


### Producerの設定

上記の例のように、`Producer`オブジェクトをトピック名のみで生成すると、Producerはデフォルトの設定を利用します。デフォルト以外の設定を利用したい場合は、`Producer`を{% javadoc ProducerConfiguration client org.apache.pulsar.client.api.ProducerConfiguration %}を用いて生成してください。

以下は設定の例です:

```java
PulsarClient client = PulsarClient.create(pulsarBrokerRootUrl);
ProducerConfiguration config = new ProducerConfiguration();
config.setBatchingEnabled(true);
config.setSendTimeout(10, TimeUnit.SECONDS);
Producer producer = client.createProducer(topic, config);
```

### メッセージ・ルーティング

{% popover_ja パーティションドトピック %}を使用している場合は、{% popover_ja Producer %}を用いたメッセージの発行時のルーティングモードを指定できます。Javaクライアントを使用したルーティングモードの指定方法の詳細は、[パーティションドトピック](../../advanced/PartitionedTopics)のドキュメントを御覧ください。

### 非同期送信

Javaクライアントを使ってメッセージを[非同期](../../getting-started/ConceptsAndArchitecture#送信モード)で発行できます。非同期送信では、Producerはメッセージをブロッキングキューに入れ、制御を戻します。クライアントライブラリはバックグラウンドで{% popover_ja Broker %}に送信します。キューが最大量 (設定可能) に達した場合、Producerは送信APIを呼び出した時、Producerに渡される引数に応じてブロックされる、あるいは失敗する可能性があります。

非同期送信処理の実装例は以下のようになります:

```java
CompletableFuture<MessageId> future = producer.sendAsync("my-async-message".getBytes());
```

非同期送信処理では、[`CompletableFuture`](http://www.baeldung.com/java-completablefuture)によってラップされた{% javadoc MessageId client org.apache.pulsar.client.api.MessageId %}がリターンされます。

## Consumer

Pulsarでは{% popover_ja Consumer %}は{% popover_ja トピック %}を購読し、{% popover_ja Producer %}がトピックに発行した{% popover_ja メッセージ %}を処理します。Consumerのオブジェクトを生成するため、まずは{% javadoc PulsarClient client org.apache.pulsar.client.api.PulsarClient %}のオブジェクトをPulsar {% popover_ja Broker %}のURLを用いて生成します (上記のProducerの例のように`client`オブジェクトを使用) 。

{% javadoc PulsarClient client org.apache.pulsar.client.api.PulsarClient %}オブジェクトを生成したら、{% popover_ja トピック %}に対して{% javadoc Consumer client org.apache.pulsar.client.api.Consumer %}オブジェクトを生成できます。{% popover_ja サブスクリプション %}名の指定も必要です。

```java
String topic = "persistent://sample/standalone/ns1/my-topic"; // from above
String subscription = "my-subscription";
Consumer consumer = client.subscribe(topic, subscription);
```

トピック上のメッセージを取得するために`receive`メソッドを使用できます。この`while`ループは`persistent://sample/standalone/ns1/my-topic`トピックに対する長期のリスナーです。メッセージを受信するとその内容を出力し、メッセージが処理された後{% popover_ja Ack %} (確認応答) を送信します:

```java
while (true) {
  // メッセージを待ち受ける
  Message msg = consumer.receive();

  System.out.println("Received message: " + msg.getData());

  // Brokerがメッセージを削除できるようにAckを送信する
  consumer.acknowledge(msg);
}
```

### Consumerの設定

上記の例のように、`Consumer`オブジェクトをトピック名とサブスクリプション名のみで生成すると、Consumerはデフォルトの設定を利用します。デフォルト以外の設定を利用したい場合は、`Consumer`を{% javadoc ConsumerConfiguration client org.apache.pulsar.client.api.ConsumerConfiguration %}を用いて生成してください。

以下は設定の例です:

```java
PulsarClient client = PulsarClient.create(pulsarBrokerRootUrl);
ConsumerConfiguration config = new ConsumerConfiguration();
config.setSubscriptionType(SubscriptionType.Shared);
config.setReceiverQueueSize(10);
Consumer consumer = client.createConsumer(topic, config);
```

### 非同期受信

`receive`メソッドはメッセージを同期的 (メッセージが利用できるようになるまでConsumerプロセスがブロックされる) に受信します。[非同期受信](../../getting-started/ConceptsAndArchitecture#受信モード)も利用可能です。このメソッドは[`CompletableFuture`](http://www.baeldung.com/java-completablefuture)オブジェクトとしてすぐにリターンします。CompletableFutureオブジェクトは新しいメッセージが利用可能になった時、受信して完了します。

以下は実装例です:

```java
CompletableFuture<Message> asyncMessage = consumer.receiveAsync();
```

非同期受信では[`CompletableFuture`](http://www.baeldung.com/java-completablefuture)でラップした{% javadoc Message client org.apache.pulsar.client.api.Message %}がリターンされます。

## 認証

Pulsarは現在、[TLS](../../admin/Authz#tls認証)と[Athenz](../../admin/Authz#athenz)の2つの認証スキームをサポートしています。Pulsar Javaクライアントでは両方が利用可能です。

### TLS認証

[TLS](../../admin/Authz#tls認証)を利用するために、`setUseTls`メソッドで`true`をセットし、TLS通信を有効にする必要があります。また、CAの証明書、クライアントの証明書、秘密鍵のパスを指定する必要があります。

以下は設定の例です:

```java
ClientConfiguration conf = new ClientConfiguration();
conf.setUseTls(true);
conf.setTlsTrustCertsFilePath("/path/to/cacert.pem");

Map<String, String> authParams = new HashMap<>();
authParams.put("tlsCertFile", "/path/to/client-cert.pem");
authParams.put("tlsKeyFile", "/path/to/client-key.pem");
conf.setAuthentication(AuthenticationTls.class.getName(), authParams);

PulsarClient client = PulsarClient.create(
                        "pulsar+ssl://my-broker.com:6651", conf);
```

### Athenz

[Athenz](../../admin/Authz#athenz)を利用するために、[TLS通信を有効](#tls認証)にし、`Map`として以下の4つのパラメータを与える必要があります:

* `tenantDomain`
* `tenantService`
* `providerDomain`
* `privateKeyPath`

`keyId`というパラメータも任意で設定可能です。

以下は設定の例です:

```java
ClientConfiguration conf = new ClientConfiguration();

// TLSを有効に
conf.setUseTls(true);
conf.setTlsTrustCertsFilePath("/path/to/cacert.pem");

// Athenz認証プラグインのパラメータをセット
Map<String, String> authParams = new HashMap<>();
authParams.put("tenantDomain", "shopping"); // テナントドメイン名
authParams.put("tenantService", "some_app"); // テナントサービス名
authParams.put("providerDomain", "pulsar"); // プロバイダドメイン名
authParams.put("privateKeyPath", "/path/to/private.pem"); // テナントの秘密鍵のパス
authParams.put("keyId", "v1"); // テナントの秘密鍵のID (任意, デフォルト: "0")
conf.setAuthentication(AuthenticationAthenz.class.getName(), authParams);

PulsarClient client = PulsarClient.create(
        "pulsar+ssl://my-broker.com:6651", conf);
```
