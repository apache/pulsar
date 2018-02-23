---
title: Pulsarにおける認証と認可
tags_ja: [admin, authentication, authorization, athenz, tls, java, cpp]
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

Pulsarは、クライアントが{% popover_ja Broker %}との認証で使用できるプラガブルな認証機構をサポートしています。また、Pulsarは複数の認証ソースをサポートするように設定する事も可能です。

## ロールトークン

Pulsarでは*ロール*は`admin`や`app1`といった文字列であり、単一もしくは複数のクライアントを表します。ロールはクライアントが特定のトピックをproduce/consumeしたり、{% popover_ja プロパティ %}の設定を管理したりする際のアクセス制御に使用されます。

[認証プロバイダ](#認証プロバイダ)の目的は、クライアントの身元を確認し、そのクライアントに*ロールトークン*を割り当てる事です。このロールトークンを使用してクライアントに認可された動作を決定します。

## 認証プロバイダ

Pulsarはすぐに利用可能な2つの認証プロバイダをサポートしています:

* [TLSクライアント認証](#tlsクライアント認証)
* [Athenz](#athenz)

### TLSクライアント認証

[Transport Layer Security](https://ja.wikipedia.org/wiki/Transport_Layer_Security) (TLS) は、Pulsarクライアントと{% popover_ja Broker %}の間の接続の暗号化を提供するのに加えて、信頼できる認証局によって署名された証明書を使用する事でクライアントを識別する事にも利用できます。

#### 証明書の作成

Pulsar用のTLS証明書を作成するという事は、[認証局](#認証局) (CA) 、[Broker証明書](#broker証明書]、[クライアント証明書](#クライアント証明書)の作成を意味します。

##### 認証局

最初の手順はCA用の証明書を作成する事です。CAは、各当事者が他者を信頼できるようにBrokerとクライアント双方の証明書に署名を行うために使用されます。

###### Linux

```shell
$ CA.pl -newca
```

###### MacOS

```
$ /System/Library/OpenSSL/misc/CA.pl -newca
```

質問のプロンプトに回答すると、CA関連のファイルが`./demoCA`ディレクトリに保存されます。このディレクトリには次のファイルが含まれます:

* `demoCA/cacert.pem`は公開証明書です。これは全ての当事者に配布されます。
* `demoCA/private/cakey.pem`は秘密鍵です。Brokerまたはクライアントの新しい証明書に署名する際にのみ必要であり、安全に保護されなければなりません。

##### Broker証明書

CA証明書が作成されたら、証明書署名要求を作成しCAによって署名する事が可能になります。

次のコマンドを実行するといくつかの質問が表示されてから証明書が作成されます。コモンネームを尋ねられた際にはBrokerのホスト名と一致するものにする必要があります。また、Brokerのホスト名のグループにマッチさせるためにワイルドカードを使用する事もできます (例えば`*.broker.usw.example.com`など) 。これによって、複数のマシンで同じ証明書の再利用が可能になります。

```shell
$ openssl req \
  -newkey rsa:2048 \
  -sha256 \
  -nodes \
  -out broker-cert.csr \
  -outform PEM
```

鍵は[PKCS 8](https://ja.wikipedia.org/wiki/PKCS)フォーマットに変換してください:

```shell
$ openssl pkcs8 \
  -topk8 \
  -inform PEM \
  -outform PEM \
  -in privkey.pem \
  -out broker-key.pem \
  -nocrypt
```

これによって、Broker証明書用の2つのファイル`broker-cert.csr`と`broker-key.pem`が作成されます。これで署名付き証明書が作成できるようになります:

```shell
$ openssl ca \
  -out broker-cert.pem \
  -infiles broker-cert.csr
```

この時点で`broker-cert.pem`と`broker-key.pem`というファイルができたはずです。これらはBrokerに必要となります。

##### クライアント証明書

クライアント証明書を作成するには前のセクションの手順を繰り返しますが、代わりに`client-cert.pem`と`client-key.pem`というファイルを作成します。

クライアントのコモンネームは、このクライアントの[ロールトークン](#ロールトークン)として使用したい文字列とする必要がありますが、クライアントのホスト名と一致させる必要はありません。

#### TLSのためのBrokerの設定

TLS認証を使用するようにPulsar {% popover_ja Broker %}を設定するためには、[Pulsarをインストール](../../getting-started/LocalCluster)した際に`conf`ディレクトリに配置される設定ファイル`broker.conf`にいくつかの変更を加える必要があります。

これらの値を設定ファイルに追加してください (必要であれば適切な証明書のパスに置き換えてください) :

```properties
# TLSを有効化しBrokerに正しい証明書を指定
tlsEnabled=true
tlsCertificateFilePath=/path/to/broker-cert.pem
tlsKeyFilePath=/path/to/broker-key.pem
tlsTrustCertsFilePath=/path/to/cacert.pem

# TLSの認証プロバイダを有効化
authenticationEnabled=true
authorizationEnabled=true
authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderTls
```

{% include message-ja.html id="broker_conf_doc" %}

#### ディスカバリサービスの設定

Pulsar Brokerが使用する{% popover_ja ディスカバリ %}サービスは全てのHTTPSリクエストをリダイレクトする必要があり、Brokerと同様にクライアントに信頼される必要があります。次の設定を`conf/discovery.conf`に追加してください:

```properties
tlsEnabled=true
tlsCertificateFilePath=/path/to/broker-cert.pem
tlsKeyFilePath=/path/to/broker-key.pem
```

#### クライアントの設定

TLSを使用したPulsarのクライアント認証に関する詳細は、各言語ごとのドキュメントを参照してください:

* [Javaクライアント](../../clients/Java)
* [C++クライアント](../../clients/Cpp)

#### CLIツールの設定

[`pulsar-admin`](../../reference/CliTools#pulsar-admin)や[`pulsar-perf`](../../reference/CliTools#pulsar-perf)、[`pulsar-client`](../../reference/CliTools#pulsar-client)のような[コマンドラインツール](../../reference/CliTools)は設定ファイル`conf/client.conf`を使用します。

PulsarのCLIツールでTLSを使用するには、そのファイルに次の認証パラメータを追加する必要があります:

```properties
serviceUrl=https://broker.example.com:8443/
authPlugin=org.apache.pulsar.client.impl.auth.AuthenticationTls
authParams=tlsCertFile:/path/to/client-cert.pem,tlsKeyFile:/path/to/client-key.pem
useTls=true
tlsAllowInsecureConnection=false
tlsTrustCertsFilePath=/path/to/cacert.pem
```

### Athenz

[Athenz](https://github.com/yahoo/athenz)はロールベースの認証・認可システムです。Pulsarでは、Athenzの[ロールトークン](#ロールトークン) (*Zトークン*) がクライアントを識別するために使用できます。

#### Athenz認証の設定

[分散型のAthenzシステム](https://github.com/yahoo/athenz/blob/master/docs/dev_decentralized_access.md)では、[authori**Z**ation **M**anagement **S**ystem](https://github.com/yahoo/athenz/blob/master/docs/setup_zms.md) (ZMS) サーバと[authori**Z**ation **T**oken **S**ystem](https://github.com/yahoo/athenz/blob/master/docs/setup_zts.md) (ZTS) サーバが存在します。

まず、Athenzのサービスアクセス制御をセットアップする必要があります。*プロバイダ* (認証・認可ポリシーに基づいて他のサービスに何らかのリソースを提供します) と*テナント* (プロバイダの何らかのリソースにアクセスするために用意されます) のドメインを作成する必要があります。この場合、プロバイダはPulsarサービス自体に対応し、テナントはPulsarを利用する各アプリケーション (通常、Pulsarにおける{% popover_ja プロパティ %}) に対応します。

##### テナントドメインとテナントサービスの作成

{% popover_ja テナント %}側では、次の手順を踏む必要があります:

1. `shopping`のようなドメインを作成
2. 秘密鍵と公開鍵のペアを生成
3. 公開鍵を使用してドメイン上に`some_app`のようなサービスを作成

Pulsarクライアントが{% popover_ja Broker %}に接続する際に、手順2で生成した秘密鍵が必要になる事にご注意ください (クライアントの設定例は[Java](../../clients/Java#athenz)を参照してください) 。

Athenz UIに関するより詳細な手順は[こちらのドキュメント](https://github.com/yahoo/athenz/blob/master/docs/example_service_athenz_setup.md#client-tenant-domain)をご覧ください。

##### プロバイダドメインの作成とテナントサービスのロールメンバーへの追加

プロバイダ側では、次の手順を踏む必要があります:

1. `pulsar`のようなドメインを作成
2. ロールを作成
3. テナントサービスをロールのメンバーに追加

手順2ではどんなアクションとリソースでも指定できますが、それらはPulsarでは使用されません。言い換えれば、PulsarはAthenzのロールトークンを認証のみに使用し、認可には使用*しません*。

UIに関するより詳細な手順は[こちらのドキュメント](https://github.com/yahoo/athenz/blob/master/docs/example_service_athenz_setup.md#server-provider-domain)をご覧ください。

#### AthenzのためのBrokerの設定

{% include message-ja.html id="tls_role_tokens" %}

設定ファイル`conf/broker.conf`において、Athenzの認証プロバイダのクラス名と、カンマで区切られたプロバイダドメイン名のリストを指定する必要があります。

```properties
# Add the Athenz auth provider
authenticationEnabled=true
authorizationEnabled=true
authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderAthenz
athenzDomainNames=pulsar

# Enable TLS
tlsEnabled=true
tlsCertificateFilePath=/path/to/broker-cert.pem
tlsKeyFilePath=/path/to/broker-key.pem
```

{% include message-ja.html id="broker_conf_doc" %}

#### Athenzのためのクライアントの設定

Athenzを使用したPulsarのクライアントの認証の詳細は、各言語ごとのドキュメントを参照してください:

* [Javaクライアント](../../clients/Java#athenz)

#### AthenzのためのCLIツールの設定

[`pulsar-admin`](../../reference/CliTools#pulsar-admin)や[`pulsar-perf`](../../reference/CliTools#pulsar-perf)、[`pulsar-client`](../../reference/CliTools#pulsar-client)のような[コマンドラインツール](../../reference/CliTools)は設定ファイル`conf/client.conf`を使用します。

PulsarのCLIツールでAthenzを使用するには、そのファイルに次の認証パラメータを追加する必要があります:

```properties
# BrokerのURL
serviceUrl=https://broker.example.com:8443/

# Athenzの認証プラグインとそのパラメータをセット
authPlugin=org.apache.pulsar.client.impl.auth.AuthenticationAthenz
authParams=tenantDomain:shopping,tenantService:some_app,providerDomain:pulsar,privateKeyPath:/path/to/private.pem,keyId:v1

# TLSを有効化
useTls=true
tlsAllowInsecureConnection=false
tlsTrustCertsFilePath=/path/to/cacert.pem
```

## 認可

Pulsarでは、[認証プロバイダ](#認証プロバイダ)はクライアントを正確に識別し、それを[ロールトークン](#ロールトークン)に関連付ける役目を負います。*認可*はクライアントが*何を*実行可能かを決定するプロセスです。

Pulsarにおける認可は{% popover_ja プロパティ %}レベルで管理されます。つまり、単一のPulsarインスタンスで複数の認可スキームを有効にできます。例えば、あるセットの[ロール](#ロールトークン)を持つ`shopping`プロパティを作成し、あなたの会社のショッピングアプリケーションに適用する事が可能です。同時に、`inventory`プロパティは在庫管理のアプリケーションからのみ使用されます。

{% include message-ja.html id="properties_multiple_clusters" %}

## 新しいプロパティの作成

Pulsarの{% popover_ja プロパティ %}は{% popover_ja テナント %}を識別し、通常Pulsarの{% popover_ja インスタンス %}管理者か何らかのセルフサービスポータルによって用意されます。

プロパティは[`pulsar-admin`](../../reference/CliTools#pulsar-admin)ツールを使用する事で管理できます。以下はプロパティを作成するコマンドの例です:

```shell
$ bin/pulsar-admin properties create my-property \
  --admin-roles my-admin-role \
  --allowed-clusters us-west,us-east
```

このコマンドは、クラスタ`us-west`と`us-east`を利用可能な新しいプロパティ`my-property`を作成します。

ロール`my-admin-role`に属していると正しく識別されたクライアントは、このプロパティに対する全ての管理タスクの実行が許可されます。

Pulsarのトピック名の構造はプロパティ、クラスタ、[ネームスペース](#ネームスペースの管理)の階層関係を反映しています:

{% include topic.html p="property" c="cluster" n="namespace" t="topic" %}

## パーミッションの管理

{% include explanations/ja/permissions.md %}

## スーパーユーザ

Pulsarでは特定のロールをシステムの*スーパーユーザ*に割り当てる事ができます。スーパーユーザは全てのプロパティとネームスペースで管理タスクの実行が許可される他、全てのトピックに対するメッセージの発行・購読も可能です。

スーパーユーザは、Brokerの設定ファイル[`conf/broker.conf`](../../reference/Configuration#broker)の中で[`superUserRoles`](../../reference/Configuration#broker-superUserRoles)パラメータを使用して設定できます。

```properties
superUserRoles=my-super-user-1,my-super-user-2
```

{% include message-ja.html id="broker_conf_doc" %}

通常、スーパーユーザロールは管理者やクライアントだけではなく、BrokerとBrokerの間の認可にも使用されます。[ジオレプリケーション](../GeoReplication)を使用する場合、全てのBrokerは他のクラスタのトピックに対してメッセージの発行が可能である必要があります。

## Pulsar adminの認証

```java
String authPluginClassName = "com.org.MyAuthPluginClass";
String authParams = "param1=value1";
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

TLSを使用するには次のようにします:

```java
String authPluginClassName = "com.org.MyAuthPluginClass";
String authParams = "param1=value1";
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
