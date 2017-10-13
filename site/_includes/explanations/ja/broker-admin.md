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

Pulsar Brokerは2つのコンポーネントから構成されます:

1. 管理操作と{% popover_ja トピック %}ルックアップのための[RESTインターフェース](../../reference/RestApi)を提供するHTTPサーバ
2. Pulsarの全ての{% popover_ja メッセージ %}の転送を処理するディスパッチャ

{% popover_ja Broker %}は以下を経由して管理できます:

* [`pulsar-admin`](../../reference/CliTools#pulsar-admin)ツールの[`brokers`](../../reference/CliTools#pulsar-admin-brokers)コマンド
* admin [REST API](../../reference/RestApi)の`/admin/brokers`エンドポイント
* Java [admin API](../../admin/AdminInterface)の{% javadoc PulsarAdmin admin org.apache.pulsar.client.admin.PulsarAdmin %}オブジェクトの`brokers`メソッド

起動時に設定可能であることに加えて、[動的に設定](#動的なBrokerの設定)可能です。

{% include admonition.html type="info" content="
Brokerの全パラメータのリストは[設定](../../reference/Configuration#broker)ページを参照してください。" %}

### アクティブなBrokerリストの取得

トラフィックを処理している利用可能な全てのアクティブなBrokerリストを取得します。  

#### pulsar-admin


```shell
$ pulsar-admin brokers list use
```

```
broker1.use.org.com:8080
```

###### REST

{% endpoint GET /admin/brokers/:cluster %}

[詳細](../../reference/RestApi#/admin/brokers/:cluster)

###### Java

```java
admin.brokers().getActiveBrokers(clusterName)
```

#### 指定したBrokerが担当しているネームスペースリストの取得

指定されたBrokerが担当、処理している全ネームスペースを取得します。 

###### CLI

```shell
$ pulsar-admin brokers namespaces use \
  --url broker1.use.org.com:8080
```

```json
{
  "my-property/use/my-ns/0x00000000_0xffffffff": {
    "broker_assignment": "shared",
    "is_controlled": false,
    "is_active": true
  }
}
```
###### REST

{% endpoint GET /admin/brokers/:cluster/:broker:/ownedNamespaces %}

###### Java

```java
admin.brokers().getOwnedNamespaces(cluster,brokerUrl);
```

### 動的なBrokerの設定

Pulsar {% popover_ja Broker %}を設定する1つの方法はBroker[起動](../../reference/CliTools#pulsar-broker)時に[設定](../../reference/Configuration#broker)する方法です。

しかしPulsarでは全てのBrokerの設定は{% popover_ja ZooKeeper %}に保存されているので、*Brokerが起動していれば*設定値を動的に更新することも可能です。Brokerの設定を動的に更新したら、ZooKeeperはBrokerに変更を通知しBrokerは現在の設定値を上書きします。 

* [`pulsar-admin`](../../reference/CliTools#pulsar-admin)ツールの[`brokers`](../../reference/CliTools#pulsar-admin-brokers)コマンドは、Brokerの設定を動的に操作したり、[設定値](#動的な設定の更新)の更新などを可能にする様々なサブコマンドを持っています。 
* Pulsarのadmin [REST API](../../reference/RestApi)では、動的な設定は`/admin/brokers/configuration`エンドポイントを通して管理されます。

### 動的な設定の更新

#### pulsar-admin

[`update-dynamic-config`](../../reference/CliTools#pulsar-admin-brokers-update-dynamic-config)サブコマンドは既存の設定を更新します。パラメータ名と新しい値の2つの引数を取ります。以下は[`brokerShutdownTimeoutMs`](../../reference/Configuration#broker-brokerShutdownTimeoutMs)パラメータの例です:

```shell
$ pulsar-admin brokers update-dynamic-config brokerShutdownTimeoutMs 100
```

#### REST API

{% endpoint POST /admin/brokers/configuration/:configName/:configValue %}

[詳細](../../reference/RestApi#/admin/brokers/configuration/:configName/:configValue)

#### Java

```java
admin.brokers().updateDynamicConfiguration(configName, configValue);
```

### アップデート可能な値のリストを取得

全ての更新可能な設定パラメータリストを取得します。

#### pulsar-admin

```shell
$ pulsar-admin brokers list-dynamic-config
brokerShutdownTimeoutMs
```

#### REST API

{% endpoint GET /admin/brokers/configuration %}

[詳細](../../reference/RestApi#/admin/brokers/configuration)

#### Java

```java
admin.brokers().getDynamicConfigurationNames();
```

### 全てのリストを取得

動的に更新された全てのパラメータリストを取得します。

#### pulsar-admin

```shell
$ pulsar-admin brokers get-all-dynamic-config
brokerShutdownTimeoutMs:100
```

#### REST API

{% endpoint GET /admin/brokers/configuration/values %}

[詳細](../../reference/RestApi#/admin/brokers/configuration/values)

#### Java

```java
admin.brokers().getAllDynamicConfigurations();
```
