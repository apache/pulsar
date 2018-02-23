---
title: 監視
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

Pulsarクラスタを監視する方法はいくつか存在し、トピックの利用状況とクラスタの個々のコンポーネントの総合的な健康状態に関するメトリクスを公開しています。

## メトリクスの収集

### Brokerの統計情報

[`pulsar-admin`](../../reference/CliTools#pulsar-admin)ツール

Pulsar {% popover Broker %}のメトリクスはBrokerから収集し、JSON形式でエクスポートできます。メトリクスには次の2つのタイプがあります:

* 個々のトピックの統計情報を含む*デスティネーションダンプ*。それらは次のコマンドで取得できます:

  ```shell
  bin/pulsar-admin broker-stats destinations
  ```

* Brokerの情報とネームスペースレベルで集約されたトピックの統計情報を含むBrokerメトリクス:

  ```shell
  bin/pulsar-admin broker-stats monitoring-metrics
  ```

全てのメッセージレートは1分ごとに更新されます。

集約されたBrokerメトリクスは[Prometheus](https://prometheus.io)のデータ形式でも公開されています:

```shell
http://$BROKER_ADDRESS:8080/metrics
```

### ZooKeeperの統計情報

Pulsarに同梱されているZooKeeperサーバとクライアントは、Prometheusを通して詳細な統計情報を公開するように設定されています。

```shell
http://$ZK_SERVER:8080/metrics
```

### BookKeeperの統計情報

BookKeeperは`conf/bookkeeper.conf`の`statsProviderClass`を変更する事で統計情報のフレームワークを設定できます。

Pulsarディストリビューションに含まれるBookKeeperのデフォルトの設定ではPrometheusエクスポータが有効になります。

```shell
http://$BOOKIE_ADDRESS:8000/metrics
```

Bookieの場合、デフォルトのポートは`8000` (`8080`ではなく) であり、これは`conf/bookkeeper.conf`の`prometheusStatsHttpPort`を変更する事で設定可能です。

## Prometheusの設定

Prometheusの[入門](https://prometheus.io/docs/introduction/getting_started/)ガイドに従う事で、メトリクスデータを収集・保存するようにPrometheusを設定可能です。

ベアメタルで実行する場合、収集対象のノードのリストを設定できます。Kubernetesクラスタにデプロイする場合、監視は[指定された](../../deployment/Kubernetes)手順で自動的にセットアップされます。

## ダッシュボード

時系列順の統計情報を収集する場合の主な問題は、データの次元数が爆発的に増大しないかを確認する必要がある事です。

そのため、Pulsarではネームスペースレベルで集計されたメトリクスのみを時系列順に収集します。

### トピックごとのダッシュボード

トピックごとのダッシュボードの利用手順については[ダッシュボード](../../admin/Dashboard)を参照してください。

### Grafana

Grafanaを使用する事でPrometheusに保存されたデータに基づいたダッシュボードを簡単に作成できます。

主要なダッシュボードが既に用意されたDockerイメージ`pulsar-grafana`が利用可能です。これはKubernetesにPulsarをデプロイする際にはデフォルトで有効になります。

手動でダッシュボードを使用するには次のようにします:

```shell
docker run -p3000:3000 \
        -e PROMETHEUS_URL=http://$PROMETHEUS_HOST:9090/ \
        streamlio/pulsar-grafana:latest
```
