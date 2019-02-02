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

Pulsar {% popover_ja Broker %}と接続する[クライアント](../../getting-started/Clients)は単一のURLを使用してPulsar{% popover_ja インスタンス %}全体と通信できる必要があります。Pulsarには[すぐ下にある](#サービスディスカバリのセットアップ)手順に従って設定できる組み込みのサービスディスカバリ機構が用意されています。

必要に応じて、独自のサービスディスカバリシステムを使用することもできます。独自のシステムを使用する場合、クライアントが`http://pulsar.us-west.example.com:8080`のようなPulsar{% popover_ja クラスタ %}の[エンドポイント](../../reference/Configuration)にHTTPリクエストを送る際に、DNS, HTTP, IPリダイレクト、その他の手段を介して目的のクラスタ内のアクティブなBrokerの*どれか1つ*にリダイレクトされる必要があります。

{% include admonition.html type="success" title="
多数のスケジューリングシステムによって既に提供されているサービスディスカバリ" content=" [Kubernetes](../../deployment/Kubernetes)などの多くの大規模展開システムには、サービスディスカバリシステムが組み込まれています。このようなシステムでPulsarを起動している場合は、独自のサービスディスカバリ機構は必要ありません。
" %}

### サービスディスカバリのセットアップ

Pulsarに含まれるサービスディスカバリ機構は、{% popover_ja ZooKeeper %}に格納されているアクティブなBrokerのリストを保持し、HTTPとPulsarの[バイナリプロトコル](../../project/BinaryProtocol)を使用したルックアップをサポートします。

Pulsarの組み込みサービスディスカバリを設定するには、設定ファイルである[`conf/discovery.conf`](../../reference/Configuration#サービスディスカバリ)のいくつかのパラメータを変更する必要があります。Zookeeperクォーラムに接続するための文字列である[`zookeeperServers`](../../reference/Configuration#サービスディスカバリ)とConfiguration Storeクォーラムに接続するための文字列である[`configurationStoreServers`](../../reference/Configuration#サービスディスカバリ)をセットしてください。

```properties
# Zookeeperクォーラムに接続するための文字列
zookeeperServers=zk1.us-west.example.com:2181,zk2.us-west.example.com:2181,zk3.us-west.example.com:2181

# Configuration Storeクォーラムに接続するための文字列
configurationStoreServers=zk1.us-west.example.com:2184,zk2.us-west.example.com:2184,zk3.us-west.example.com:2184
```

サービスディスカバリを起動する:

```shell
$ bin/pulsar-daemon start discovery
```
