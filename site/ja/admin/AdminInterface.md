---
title: Pulsar adminインターフェース
tags_ja: [admin, cli, rest, java]
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

Pulsar adminインターフェースは、{% popover_ja プロパティ %}、{% popover_ja ネームスペース %}、{% popover_ja トピック %}のようなPulsar{% popover_ja インスタンス %}における重要なエンティティの管理を可能にします。

現在、以下の方法でadminインターフェースを利用できます:

1. Pulsar {% popover_ja Broker %}が提供するadmin [REST API](../../reference/RestApi)にHTTPリクエストを送信
1. [Pulsarをインストール](../../getting-started/LocalCluster)すると`bin`フォルダに配置される`pulsar-admin` CLIツールを使用

    ```shell
    $ bin/pulsar-admin
    ```

    このツールの完全なドキュメントは[Pulsarコマンドラインツール](../../reference/CliTools#pulsar-admin)で閲覧できます。

1. Javaクライアントインターフェース

{% include message-ja.html id="admin_rest_api" %}

このドキュメントでは、利用可能な3つのインターフェースそれぞれの例を示します。

## adminインターフェースのセットアップ

{% include explanations/ja/admin-setup.md %}

## クラスタの管理

{% include explanations/ja/cluster-admin.md %}

## Brokerの管理

{% include explanations/ja/broker-admin.md %}

## プロパティの管理

プロパティはアプリケーションドメインを識別するためのものです。例えばfinance, mail, sportsなどがプロパティの例にあたります。adminツールはPulsarのプロパティを管理するためのCRUD操作を可能にします。

#### 既存のプロパティのリストを取得

Pulsarシステムにおける既存のプロパティのリストを取得します。

###### CLI

```
$ pulsar-admin properties list
```

```
my-property
```

###### REST

{% endpoint GET /admin/properties %}

###### Java

```java
admin.properties().getProperties()
```


#### プロパティの作成


Pulsarシステムに新しいプロパティを作成します。プロパティに対しては、プロパティを管理可能なadminロール (カンマ区切りで複数指定可能) とプロパティが利用可能なクラスタ (カンマ区切りで複数指定可能) を設定できます。

###### CLI

```shell
$ pulsar-admin properties create my-property \
  --admin-roles admin1,admin2 \
  --allowed-clusters cl1,cl2
```

```
N/A
```

###### REST

{% endpoint PUT /admin/properties/:property %}

###### Java

```java
admin.properties().createProperty(property, propertyAdmin)
```


#### プロパティの取得

指定された既存プロパティの設定を取得します。

###### CLI

```
$pulsar-admin properties get my-property
```

```json
{
    "adminRoles": [
        "admin1",
        "admin2"
    ],
    "allowedClusters": [
        "cl1",
        "cl2"
    ]
}
```

###### REST

{% endpoint GET /admin/properties/:property %}

###### Java

```java
admin.properties().getPropertyAdmin(property)
```



#### プロパティの更新

既に作成済みのプロパティの設定を更新します。指定された既存プロパティのadminロールとクラスタ情報の更新が可能です。

###### CLI

```shell
$ pulsar-admin properties update my-property \
  --admin-roles admin-update-1,admin-update-2 \
  --allowed-clusters cl1,cl2
```

###### REST

{% endpoint POST /admin/properties/:property %}

###### Java

```java
admin.properties().updateProperty(property, propertyAdmin)
```


#### プロパティの削除


Pulsarシステムから既存のプロパティを削除します。
###### CLI

```
$ pulsar-admin properties delete my-property
```

```
N/A
```

###### REST

{% endpoint DELETE /admin/properties/:property %}

###### Java

```java
admin.properties().deleteProperty(property);
```

## ネームスペースの管理

{% include explanations/ja/namespace-admin.md %}

### リソース割り当て

#### ネームスペースへのリソース割り当て

指定されたネームスペースバンドルに対してリソース割り当て情報をセットします。

###### CLI

```
$ pulsar-admin resource-quotas set --bandwidthIn 10 --bandwidthOut 10 --bundle 0x00000000_0xffffffff --memory 10 --msgRateIn 10 --msgRateOut 10 --namespace test-property/cl1/ns1
```

```
N/A
```

###### REST

```
POST /admin/resource-quotas/{property}/{cluster}/{namespace}/{bundle}
```

###### Java

```java
admin.resourceQuotas().setNamespaceBundleResourceQuota(namespace, bundle, quota)
```

#### ネームスペースに対するリソース割り当て情報を取得

設定されたリソース割り当て情報を表示します。

###### CLI

```
$ pulsar-admin resource-quotas get  --bundle 0x00000000_0xffffffff --namespace test-property/cl1/my-topic
```

```json
{
    "msgRateIn": 80.40352101165782,
    "msgRateOut": 132.58187392933146,
    "bandwidthIn": 144273.8819600397,
    "bandwidthOut": 234497.9190227951,
    "memory": 199.91739142481595,
    "dynamic": true
}          
```

###### REST
```
GET /admin/resource-quotas/{property}/{cluster}/{namespace}/{bundle}
```

###### Java

```java
admin.resourceQuotas().getNamespaceBundleResourceQuota(namespace, bundle)
```

#### ネームスペースに対するリソース割り当てをリセット

設定されたリソース割り当てを元に戻し、デフォルトのリソース割り当て情報をセットします。

###### CLI

```
$ pulsar-admin resource-quotas reset-namespace-bundle-quota --bundle 0x00000000_0xffffffff --namespace test-property/cl1/my-topic
```

```
N/A
```

###### REST

```
DELETE /admin/resource-quotas/{property}/{cluster}/{namespace}/{bundle}
```

###### Java

```java
admin.resourceQuotas().resetNamespaceBundleResourceQuota(namespace, bundle)
```

## パーシステントトピックの管理

`persistent`はメッセージを送受信するための論理的なエンドポイントであるトピックにアクセスするのに役立ちます。Producerはトピックに対してメッセージを発行し、Consumerはそのトピックを購読して発行されたメッセージをconsumeします。

以下の解説におけるパーシステントトピックのフォーマットは次のようになります:

{% include topic.html p="property" c="cluster" n="namespace" t="topic" %}

{% include explanations/ja/persistent-topic-admin.md %}

### ネームスペース隔離ポリシー

#### ネームスペース隔離ポリシーの作成と更新

次のようなパラメータを指定する事でネームスペース隔離ポリシーを作成します。

  -   auto-failover-policy-params: カンマで区切られたname=value形式の自動フェイルオーバーポリシーのパラメータ

  -   auto-failover-policy-type: 自動フェイルオーバーポリシーのタイプ名 ['min_available']

  -   namespaces: カンマで区切られたネームスペースの正規表現リスト

  -   primary: カンマで区切られたプライマリBrokerの正規表現リスト

  -   secondary: カンマで区切られたセカンダリBrokerの正規表現リスト

###### CLI

```
$ pulsar-admin ns-isolation-policy --auto-failover-policy-params min_limit=0 --auto-failover-policy-type min_available --namespaces test-property/cl1/ns.*|test-property/cl1/test-ns*.* --secondary broker2.* --primary broker1.* cl1 ns-is-policy1
```

```
N/A
```

###### REST

```
POST /admin/clusters/{cluster}/namespaceIsolationPolicies/{policyName}
```

###### Java
```java
admin.clusters().createNamespaceIsolationPolicy(clusterName, policyName, namespaceIsolationData);
```


#### ネームスペース隔離ポリシーの取得


作成されたネームスペース隔離ポリシーを表示します。

###### CLI

```
$ pulsar-admin ns-isolation-policy get cl1 ns-is-policy1
```

```json
{
    "namespaces": [
        "test-property/cl1/ns.*|test-property/cl1/test-ns*.*"
    ],
    "primary": [
        "broker1.*"
    ],
    "secondary": [
        "broker2.*"
    ],
    "auto_failover_policy": {
        "policy_type": "min_available",
        "parameters": {
            "min_limit": "0"
        }
    }
}
```

###### REST
```
GET /admin/clusters/{cluster}/namespaceIsolationPolicies/{policyName}
```

###### Java

```java
admin.clusters().getNamespaceIsolationPolicy(clusterName, policyName)
```


#### ネームスペース隔離ポリシーの削除

ネームスペース隔離ポリシーを削除します。

###### CLI

```
$ pulsar-admin ns-isolation-policy delete ns-is-policy1
```

```
N/A
```

###### REST

```
DELETE /admin/clusters/{cluster}/namespaceIsolationPolicies/{policyName}
```

###### Java

```java
admin.clusters().deleteNamespaceIsolationPolicy(clusterName, policyName)
```


#### 全てのネームスペース隔離ポリシーのリストを取得


指定されたクラスタで提供されている全てのネームスペース隔離ポリシーを表示します。

###### CLI

```
$ pulsar-admin ns-isolation-policy list cl1
```

```json
{
    "ns-is-policy1": {
        "namespaces": [
            "test-property/cl1/ns.*|test-property/cl1/test-ns*.*"
        ],
        "primary": [
            "broker1.*"
        ],
        "secondary": [
            "broker2.*"
        ],
        "auto_failover_policy": {
            "policy_type": "min_available",
            "parameters": {
                "min_limit": "0"
            }
        }
    }
}
```

###### REST
```
GET /admin/clusters/{cluster}/namespaceIsolationPolicies
```

###### Java

```java
admin.clusters().getNamespaceIsolationPolicies(clusterName)
```

## ノンパーシステントトピックの管理

`non-persistent`は、発行されたメッセージをリアルタイムにconsumeするだけで、永続性の保証を必要としないアプリケーションで利用できます。メッセージの永続化に伴うオーバーヘッドを排除する事により、メッセージの発行にかかるレイテンシの短縮が可能です。

{% include explanations/ja/non-persistent-topic-admin.md %}


## パーティションドトピックの管理

パーティションドトピックは実際にはN個の内部トピックとして実現されます。ここでのNはパーティション数です。パーティションドトピックにメッセージを送信すると、それぞれのメッセージは複数のBrokerの内の1つにルーティングされます。Broker間でのパーティションの分散はPulsarによって自動的に処理されます。

以下の解説におけるパーティションドトピックのフォーマットは次のようになります:

{% include topic.html p="property" c="cluster" n="namespace" t="topic" %}

{% include explanations/ja/partitioned-topics.md %}


### パーティションドトピック

{% include explanations/ja/partitioned-topic-admin.md %}
