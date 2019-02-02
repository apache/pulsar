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

Pulsarのクラスタは1つ以上のPulsar {% popover_ja Broker %}、1つ以上の{% popover_ja BookKeeper %}サーバ ({% popover_ja Bookie %}) 、設定と調整管理の機能を提供する{% popover_ja ZooKeeper %}クラスタから構成されます。

クラスタは次の方法で管理できます:

* [`pulsar-admin`](../../reference/CliTools#pulsar-admin)ツールの[`clusters`](../../reference/CliTools#pulsar-admin-clusters)コマンド
* admin [REST API](../../reference/RestApi)の`/admin/clusters`エンドポイント
* Java [admin API](../../admin/AdminInterface)の{% javadoc PulsarAdmin admin org.apache.pulsar.client.admin.PulsarAdmin %}オブジェクトの`clusters`メソッド

### 準備

新しいクラスタはadminインターフェースを使用して準備できます。

{% include message-ja.html id="superuser" %}

#### pulsar-admin

[`create`](../../reference/CliTools#pulsar-admin-clusters-create)サブコマンドを使用する事で新しいクラスタを準備できます。以下はその例です:

```shell
$ pulsar-admin clusters create cluster-1 \
  --url http://my-cluster.org.com:8080 \
  --broker-url pulsar://my-cluster.org.com:6650
```

#### REST API

{% endpoint PUT /admin/clusters/:cluster %}

[詳細](../../reference/RestApi#/admin/clusters/:cluster)

#### Java

```java
ClusterData clusterData = new ClusterData(
        serviceUrl,
        serviceUrlTls,
        brokerServiceUrl,
        brokerServiceUrlTls
);
admin.clusters().createCluster(clusterName, clusterData);
```

### クラスタメタデータの初期化

新しいクラスタを準備する際には、そのクラスタの[メタデータ](../../getting-started/ConceptsAndArchitecture#メタデータストア)を初期化する必要があります。クラスタメタデータを初期化する場合、次の全てのパラメータを指定しなければなりません:

* クラスタの名前
* そのクラスタのLocal ZooKeeperをカンマで連結した文字列
* インスタンス全体のConfiguration Storeをカンマで連結した文字列
* クラスタのウェブサービスのURL
* クラスタ内の{% popover_ja Broker %}との対話を可能にするBrokerサービスのURL

クラスタに属する[Broker](../../admin/ClustersBrokers#brokerの管理)を起動する*前に*クラスタメタデータは初期化しなければなりません。

{% include admonition.html type="warning" title="REST APIやJava admin APIを使用したクラスタメタデータの初期化はできません" content='
Pulsarの他の多くの管理機能とは異なり、クラスタメタデータの初期化はZooKeeperと直接通信するためadmin REST APIやadmin Javaクライアントを使用する事はできません。代わりに、[`pulsar`](../../reference/CliTools#pulsar) CLIツール、特に[`initialize-cluster-metadata`](../../reference/CliTools#pulsar-initialize-cluster-metadata)コマンドが利用できます。
' %}

以下はクラスタメタデータの初期化コマンドの例です:

```shell
bin/pulsar initialize-cluster-metadata \
  --cluster us-west \
  --zookeeper zk1.us-west.example.com:2181 \
  --configuration-store zk1.us-west.example.com:2184 \
  --web-service-url http://pulsar.us-west.example.com:8080/ \
  --web-service-url-tls https://pulsar.us-west.example.com:8443/ \
  --broker-service-url pulsar://pulsar.us-west.example.com:6650/ \
  --broker-service-url-tls pulsar+ssl://pulsar.us-west.example.com:6651/
```

インスタンスで[TLS認証](../../admin/Authz#tlsクライアント認証)を使用している場合のみ、`--*-tls`オプションを使用する必要があります。

### 設定の取得

既存のクラスタの[設定](../../reference/Configuration)はいつでも取得が可能です。

#### pulsar-admin

[`get`](../../reference/CliTools#pulsar-admin-clusters-get)サブコマンドを使用し、クラスタの名前を指定してください。以下はその例です:

```shell
$ pulsar-admin clusters get cluster-1
{
    "serviceUrl": "http://my-cluster.org.com:8080/",
    "serviceUrlTls": null,
    "brokerServiceUrl": "pulsar://my-cluster.org.com:6650/",
    "brokerServiceUrlTls": null
}
```

#### REST API

{% endpoint GET /admin/clusters/:cluster %}

[詳細](../../reference/RestApi#/admin/clusters/:cluster)

#### Java

```java
admin.clusters().getCluster(clusterName);
```

### 更新

既存クラスタの設定変更はいつでも可能です。

#### pulsar-admin

[`update`](../../reference/CliTools#pulsar-admin-clusters-update)サブコマンドを使用し、新しい設定値を指定してください:

```shell
$ pulsar-admin clusters update cluster-1 \
  --url http://my-cluster.org.com:4081 \
  --broker-url pulsar://my-cluster.org.com:3350
```

#### REST

{% endpoint POST /admin/clusters/:cluster %}

[詳細](../../reference/RestApi#/admin/clusters/:cluster)

#### Java

```java
ClusterData clusterData = new ClusterData(
        serviceUrl,
        serviceUrlTls,
        brokerServiceUrl,
        brokerServiceUrlTls
);
admin.clusters().updateCluster(clusterName, clusterData);
```

### 削除

クラスタをPulsar{% popover_ja インスタンス %}から削除できます。

#### pulsar-admin

[`delete`](../../reference/CliTools#pulsar-admin-clusters-delete)サブコマンドを使用し、クラスタの名前を指定してください:

```
$ pulsar-admin clusters delete cluster-1
```

#### REST API

{% endpoint DELETE /admin/clusters/:cluster %}

[詳細](../../reference/RestApi#/admin/clusters/:cluster)

#### Java

```java
admin.clusters().deleteCluster(clusterName);
```

### リストを取得

Pulsar {% popover_ja インスタンス %}内の全てのクラスタのリストを取得できます。

#### pulsar-admin

[`list`](../../reference/CliTools#pulsar-admin-clusters-list)サブコマンドを使用してください。

```shell
$ pulsar-admin clusters list
cluster-1
cluster-2
```

#### REST API

{% endpoint GET /admin/clusters %}

[詳細](../../reference/RestApi#/admin/clusters)

###### Java

```java
admin.clusters().getClusters();
```
