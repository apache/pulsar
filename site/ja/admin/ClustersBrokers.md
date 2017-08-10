---
title: PulsarクラスタとBrokerの管理
tags_ja: [admin, cluster, broker, pulsar-admin, REST API, Java]
---

Pulsar {% popover_ja クラスタ %}と{% popover_ja Broker %}はPulsarの[adminインターフェース](../AdminInterface)を用いて管理できます。

## adminセットアップ

{% include explanations/ja/admin-setup.md %}

## クラスタの管理

{% include explanations/ja/cluster-admin.md %}

Pulsarでは、{% popover_ja クラスタ %}は{% popover_ja Broker %}, {% popover_ja Bookie %}, その他のコンポーネントのグループです。Pulsar {% popover_ja インスタンス %}は{% popover_ja ジオレプリケーション %}により他のPulsarクラスタにレプリケートできる複数のPulsarクラスタから構成されます。

## Brokerの管理

Pulsarの{% popover_ja Broker %}は管理操作と{% popover_ja トピックルックアップ %}のための[RESTインターフェース](../../reference/RestApi)を提供するHTTPサーバと{% popover_ja ディスパッチャ %}の2つのコンポーネントで構成されています。

{% include explanations/ja/broker-admin.md %}
