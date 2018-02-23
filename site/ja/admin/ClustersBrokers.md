---
title: PulsarクラスタとBrokerの管理
tags_ja: [admin, cluster, broker, pulsar-admin, REST API, Java]
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

Pulsar {% popover_ja クラスタ %}と{% popover_ja Broker %}はPulsarの[adminインターフェース](../AdminInterface)を用いて管理できます。

## adminセットアップ

{% include explanations/ja/admin-setup.md %}

## クラスタの管理

{% include explanations/ja/cluster-admin.md %}

Pulsarでは、{% popover_ja クラスタ %}は{% popover_ja Broker %}, {% popover_ja Bookie %}, その他のコンポーネントのグループです。Pulsar {% popover_ja インスタンス %}は{% popover_ja ジオレプリケーション %}により他のPulsarクラスタにレプリケートできる複数のPulsarクラスタから構成されます。

## Brokerの管理

Pulsarの{% popover_ja Broker %}は管理操作と{% popover_ja トピックルックアップ %}のための[RESTインターフェース](../../reference/RestApi)を提供するHTTPサーバと{% popover_ja ディスパッチャ %}の2つのコンポーネントで構成されています。

{% include explanations/ja/broker-admin.md %}
