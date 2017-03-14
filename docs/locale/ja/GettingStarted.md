
# Pulsar入門

<!-- TOC depthFrom:2 depthTo:4 withLinks:1 updateOnSave:1 orderedList:0 -->

- [基本コンセプト](#基本コンセプト)
	- [トピック名](#トピック名)
	- [サブスクリプションモード](#サブスクリプションモード)
	- [ソフトウェアの入手](#ソフトウェアの入手)
- [スタンドアローンなPulsarサーバの起動](#スタンドアローンなpulsarサーバの起動)
- [PulsarのJavaクライアントAPIの使用](#pulsarのjavaクライアントapiの使用)
	- [Consumer](#consumer)
	- [Producer](#producer)

<!-- /TOC -->

## 基本コンセプト

Pulsarはpub-subパラダイムに基づいて構築されたメッセージングシステムです。
**トピック**は**Producer**と**Consumer**を結びつける基本的なリソースです。

Producerはトピックに接続してメッセージを発行する事ができます。
Consumerはトピックを**購読**してメッセージを受信する事ができます。

一度サブスクリプションが作成されると、たとえConsumerが切断された場合でも、
Consumerから処理の成功を通知する**Ack (確認応答) **が返されるまで全てのメッセージは
システムによって*保持*されます。

### トピック名

トピック名は次のようになります:
```
persistent://my-property/us-west/my-namespace/my-topic
```

トピック名の構造は、Pulsarのマルチテナント性に関連しています。
この例では:
 * `persistent` →
    全てのメッセージが複数のディスクに永続化されるトピックである事を示します。
    これは現時点でサポートされている唯一のトピック形式です。
 * `my-property` →
    **プロパティ**はPulsarインスタンスにおける*テナント*を示す識別子です。
 * `us-west` →
    トピックが存在する**クラスタ**です。
    典型的には、地理的地域やデータセンタごとにクラスタが存在する事になります。
 * `my-namespace` →
    **ネームスペース**は管理単位であり、関連するトピックのグループを表します。
    ほとんどの設定はネームスペースレベルで行われます。各プロパティは複数のネームスペースを持つ事が可能です。
 * `my-topic` →
    トピック名の最後の部分です。この部分は自由形式であり、システム上の特別な意味は持ちません。

### サブスクリプションモード

各トピックは複数の**サブスクリプション**を持つ事ができます。
それぞれのサブスクリプションは異なる名前を持ち、またサブスクリプションごとに異なるタイプを指定できます:

 * **Exclusive** →
   1つのConsumerだけがこのサブスクリプションに所属する事を許されます。メッセージの順番は保証されます。
 * **Shared** →
   複数のConsumerが同じサブスクリプションに接続し、メッセージは利用可能なConsumerの間でラウンドロビンで配信されます。
   メッセージの順番は入れ替わる可能性があります。
 * **Failover** →
   1つのConsumerだけがアクティブにメッセージを受信し、他のConsumerはスタンバイ状態になります。
   メッセージの順番は保証されます。

詳しい説明は[アーキテクチャ](Architecture.md)のページを参照してください。

## ソフトウェアの入手

次の場所から最新のバイナリリリースをダウンロードしてください。

```
https://github.com/yahoo/pulsar/releases/latest
```

```shell
$ tar xvfz pulsar-X.Y-bin.tar.gz
$ cd pulsar-X.Y
```

## スタンドアローンなPulsarサーバの起動

アプリケーション開発や実際に稼働するサービスの迅速なセットアップのために、
Pulsarのスタンドアローンモードを使用する事ができます。
このモードでは、Broker, ZooKeeper, BookKeeperの3コンポーネントを
単一のJVMプロセスで起動します。

```shell
$ bin/pulsar standalone
```

Pulsarのサービスはすぐに利用可能となり、`http://localhost:8080/`
をサービスのURLとしてクライアントに使用させる事ができます。

サンプルのネームスペース `sample/standalone/ns1` が既に利用可能な状態になっています。

## PulsarのJavaクライアントAPIの使用

Pulsarクライアントライブラリの依存関係をインクルードしてください。

最新のバージョンは [![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.yahoo.pulsar/pulsar-client/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.yahoo.pulsar/pulsar-client) です。

```xml
<dependency>
  <groupId>com.yahoo.pulsar</groupId>
  <artifactId>pulsar-client</artifactId>
  <version>${pulsar.version}</version>
</dependency>
```

### Consumer

```java
PulsarClient client = PulsarClient.create("http://localhost:8080");

Consumer consumer = client.subscribe(
            "persistent://sample/standalone/ns1/my-topic",
            "my-subscribtion-name");

while (true) {
  // 1つのメッセージを待ち受け
  Message msg = consumer.receive();

  System.out.println("Received message: " + msg.getData());

  // Brokerがメッセージを削除できるようにするためのAck
  consumer.acknowledge(msg);
}

client.close();
```


### Producer

```java
PulsarClient client = PulsarClient.create("http://localhost:8080");

Producer producer = client.createProducer(
            "persistent://sample/standalone/ns1/my-topic");

// 10のメッセージをトピックに発行
for (int i = 0; i < 10; i++) {
    producer.send("my-message".getBytes());
}

client.close();
```
