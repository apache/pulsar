---
title: Pulsar Pythonクライアント
tags_ja:
- client
- python
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

Pythonクライアントライブラリは[C++クライアントライブラリ](../Cpp)のラッパーで全ての[同じ機能](../../../../api/cpp)を公開しています。C++クライアントコードの[`python`サブディレクトリ]({{ site.pulsar_repo }}/pulsar-client-cpp/python)内にコードがあります。

## インストール

[pip](https://pip.pypa.io/en/stable/)を利用するかソースコードからライブラリをビルドすることで`pulsar-client`をインストールできます。

pipを利用してのインストール:
```shell
$ pip install pulsar-client
```

ソースコードからビルドしてのインストールは、[説明](../Cpp#コンパイル)にしたがってC++クライアントライブラリをコンパイルします。これによりライブラリ用のPythonバインディングもビルドされます。

ビルドされたPythonバインディングのインストール:

```shell
$ git clone https://github.com/apache/pulsar
$ cd pulsar/pulsar-client-cpp/python
$ sudo python setup.py install
```

{% include admonition.html type="info" content="現在サポートされているPythonのバージョンは2.7のみです" %}

## APIリファレンス

完全なPython APIのリファレンスは[api/python]({{site.baseUrl}}/api/python)にあります。

## 実装例

以下に`pulsar-client`ライブラリのPythonコード例を示します。

### Producer

このコードは`persistent://sample/standalone/ns/my-topic`用のPython {% popover_ja Producer %}を作成し、10個のメッセージを送信します:

```python
import pulsar

client = pulsar.Client('pulsar://localhost:6650')

producer = client.create_producer(
                'persistent://sample/standalone/ns/my-topic')

for i in range(10):
    producer.send('Hello-%d' % i)

client.close()
```

### Consumer

このコードは`persistent://sample/standalone/ns/my-topic`トピック上に`my-sub`サブスクリプションで {% popover_ja Consumer %}を作成し、メッセージを待ち受けます。受け取ったメッセージの中身とIDを出力し、Pulsar {% popover_ja Broker %}に{% popover Ack %} (確認応答) を返します:

```python
import pulsar

client = pulsar.Client('pulsar://localhost:6650')
consumer = client.subscribe(
        'persistent://sample/standalone/ns/my-topic',
        'my-sub')

while True:
    msg = consumer.receive()
    print("Received message '%s' id='%s'", msg.data(), msg.message_id())
    consumer.acknowledge(msg)

client.close()
```

### Producer(非同期)

このコードは非同期でメッセージを送信する{% popover Producer %}を作成し、 {% popover Broker %}から{% popover Ack %} (確認応答) を受け取る度に`send_callback`コールバック関数を呼びます:

```python
import pulsar

client = pulsar.Client('pulsar://localhost:6650')

producer = client.create_producer(
                'persistent://sample/standalone/ns/my-topic',
                block_if_queue_full=True,
                batching_enabled=True,
                batching_max_publish_delay_ms=10
            )

def send_callback(res, msg):
    print('Message published res=%s', res)

while True:
    producer.send_async('Hello-%d' % i, send_callback)

client.close()
```
