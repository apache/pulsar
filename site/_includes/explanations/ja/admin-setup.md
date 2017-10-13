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

Pulsarの3つのadminインターフェース---[`pulsar-admin`](../../reference/CliTools#pulsar-admin)CLIツール, [Java admin API](/api/admin), [REST API](../../reference/RestApi)---は、Pulsar{% popover_ja インスタンス %}で[認証](../../admin/Authz#認証プロバイダ)を有効にしている場合にいくつかの特別なセットアップを必要とします。

### pulsar-admin

もし[認証](../../admin/Authz#認証プロバイダ)を有効にしている場合、[`pulsar-admin`](../../reference/CliTools#pulsar-admin)ツールを利用するために認証の設定をする必要があります。デフォルトでは、`pulsar-admin`ツールの設定は[`conf/client.conf`](../../reference/Configuration#クライアント)にあります。以下は設定可能なパラメータです:

{% include config.html id="client" %}

### REST API

この[リファレンス](../../reference/RestApi)上でPulsar {% popover_ja Broker %}が提供しているREST APIのドキュメントを参照できます。

### Java adminクライアント

Java admin APIを利用するために、Pulsar {% popover_ja Broker %}のURLと{% javadoc ClientConfiguration admin org.apache.pulsar.client.admin.ClientConfiguration %}を指定して{% javadoc PulsarAdmin admin org.apache.pulsar.client.admin.PulsarAdmin %}オブジェクトをインスタンス化します。 以下は`localhost`を利用した最小限の例です:

```java
URL url = new URL("http://localhost:8080");
String authPluginClassName = "com.org.MyAuthPluginClass"; //認証が有効な場合にauth-pluginクラスの完全修飾名を渡します。
String authParams = "param1:value1"; //auth-pluginクラスが要求しているパラメータを渡します。
boolean useTls = false;
boolean tlsAllowInsecureConnection = false;
String tlsTrustCertsFilePath = null;

ClientConfiguration config = new ClientConfiguration();
config.setAuthentication(authPluginClassName, authParams);
config.setUseTls(useTls);
config.setTlsAllowInsecureConnection(tlsAllowInsecureConnection);
config.setTlsTrustCertsFilePath(tlsTrustCertsFilePath);

PulsarAdmin admin = new PulsarAdmin(url, config);
```
