---
title: Pulsarダッシュボード
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

Pulsarダッシュボードは、ユーザが全ての{% popover_ja トピック %}の現在の統計情報を表形式で閲覧できるウェブアプリケーションです。

ダッシュボードはPulsar{% popover_ja インスタンス %} (複数の{% popover_ja クラスタ %}に跨る) 内の全ての{% popover_ja Broker %}から統計情報をポーリングし、全ての情報を[PostgreSQL](https://www.postgresql.org/)データベースに保存するデータコレクタです。

収集したデータのレンダリングには[Django](https://www.djangoproject.com)というウェブアプリケーションフレームワークが使用されています。

## インストール

ダッシュボードを使用する最も簡単な方法は、[Docker](https://www.docker.com/products/docker)コンテナ内でそれを実行する事です。Dockerイメージを生成するための[`Dockerfile`]({{ site.pulsar_repo }}/dashboard/Dockerfile)が提供されています。

Dockerイメージを生成するには次のようにします:

```shell
$ docker build -t pulsar-dashboard dashboard
```

ダッシュボードを実行するには次のようにします:

```shell
$ SERVICE_URL=http://broker.example.com:8080/
$ docker run -p 80:80 \
  -e SERVICE_URL=$SERVICE_URL \
  pulsar-dashboard
```

ここではPulsarクラスタのサービスURLを1つだけ指定する必要があります。これだけで、内部的にコレクタは存在している全てのクラスタとメトリクスを取得する必要のあるBrokerを把握できます。ダッシュボードを{% popover_ja スタンドアローン %}モードで実行しているPulsarに接続している場合、URLはデフォルトで`http://localhost:8080`になります。

Dockerコンテナが起動すると、ウェブダッシュボードには`localhost`またはDockerが使用しているホストからアクセスできます。

### 既知の問題

現時点ではPulsarの[認証](../../admin/Authz#認証プロバイダ)はサポートされていません。ダッシュボードのデータコレクタは認証関連のいかなるデータも渡さず、Pulsar Brokerが認証を要求している場合にはアクセスが拒否されてしまいます。
