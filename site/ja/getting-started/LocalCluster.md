---
title: ローカルスタンドアローンクラスタのセットアップ
lead: ローカル環境での開発のためにPulsarを単一のJVMプロセスとして起動
tags_ja:
- standalone
- local
next: ../ConceptsAndArchitecture
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

ローカル環境での開発やテストのために、 Pulsarの{% popover_ja スタンドアローン %}モードを使用する事ができます。

このモードでは、{% popover_ja Broker %}, {% popover_ja ZooKeeper %}, {% popover_ja BookKeeper %}の3コンポーネントを単一のJVMプロセスで起動します。

{% include admonition.html type="info" title='プロダクション環境でのPulsar利用方法は?' content="
プロダクション環境でのPulsarのインストール方法をお探しの場合は、[Pulsarインスタンスのデプロイ](../../deployment/InstanceSetup)をご確認ください。 " %}

{% include explanations/ja/install-package.md %}

## クラスタの起動

最新のリリースバイナリをローカルにコピーしたら、`bin`ディレクトリにある[`pulsar`](../../reference/CliTools#pulsar)コマンドでローカルクラスタを起動できます。{% popover_ja スタンドアローン %}モードで起動したい場合は以下のように入力します。

```bash
$ bin/pulsar standalone
```

Pulsarが正しく起動すると、以下のように`INFO`レベルのログが表示されます。

```
2017-06-01 14:46:29,192 - INFO  - [main:WebSocketService@95] - Configuration Store cache started
2017-06-01 14:46:29,192 - INFO  - [main:AuthenticationService@61] - Authentication is disabled
2017-06-01 14:46:29,192 - INFO  - [main:WebSocketService@108] - Pulsar WebSocket Service started
```

{% include admonition.html type="success" title='自動的に作成されるネームスペース' content='
ローカルスタンドアローンクラスタを起動する時、Pulsarは自動的に`sample/standalone/ns1`という開発目的に使用するためのネームスペースを作成します。全てのPulsarのトピックはネームスペース内で管理されます。詳細は[こちら](../ConceptsAndArchitecture#トピック)をご確認ください。' %}

## クラスタのテスト

Pulsarでは[`pulsar-client`](../../reference/CliTools#pulsar-client)コマンドを用いて起動中のPulsarクラスタの{% popover_ja トピック %}にメッセージを送ることができます。以下のコマンドは`persistent://sample/standalone/ns1/my-topic`というトピックに対して`hello-pulsar`というメッセージを送信します。

```bash
$ bin/pulsar-client produce \
  persistent://sample/standalone/ns1/my-topic \
  -m 'hello-pulsar'
```

メッセージの送信に成功した場合、`pulsar-client`から以下のようなログが得られます。

```
2017-06-01 18:18:57,094 - INFO  - [main:CmdProduce@189] - 1 messages successfully produced
```

{% include admonition.html type="success" title="明示的に新しいトピックを作る必要はありません"
content="`hello-pulsar`を送信するトピックである`my-topic`を作成しなかったことにお気付きかもしれません。もしまだ存在しないトピックにメッセージを送信しようとした場合、Pulsarはそのトピックを自動的に作成します。" %}

## ローカルでのPulsarクライアントライブラリの使用

Pulsarは現在、[Java](../../clients/Java), [Python](../../clients/Python), [C++](../../clients/Cpp)のクライアントライブラリを提供しています。ローカル{% popover_ja スタンドアローン %}クラスタを起動した場合、クラスタに接続するために以下のルートURLのいずれかを使用できます。

* `http://localhost:8080`
* `pulsar://localhost:6650`

以下は[Java](../../clients/Java)クライアントを用いたProducerの例です。

```java
String localClusterUrl = "pulsar://localhost:6650";
String namespace = "sample/standalone/ns1"; // このネームスペースは自動的に作成されます
String topic = String.format("persistent://%s/my-topic", namespace);

PulsarClient client = PulsarClient.create(localClusterUrl);
Producer producer = client.createProducer(topic);
```

以下は[Python](../../clients/Python)クライアントを用いたProducerの例です。

```python
import pulsar

TOPIC = 'persistent://sample/standalone/ns/my-topic'

client = pulsar.Client('pulsar://localhost:6650')
producer = client.create_producer(TOPIC)
```

以下は[C++](../../clients/Cpp)クライアントを用いたProducerの例です。

```cpp
Client client("pulsar://localhost:6650");
Producer producer;
Result result = client.createProducer("persistent://sample/standalone/ns1/my-topic", producer);
if (result != ResultOk) {
    LOG_ERROR("Error creating producer: " << result);
    return -1;
}
```
