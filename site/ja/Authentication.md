---
title: Pulsarにおける認証
---

## 認証モデル

Pulsarはプラガブル認証メカニズムをサポートし、Brokerは複数の認証ソースをサポートするように設定できます。

認証プロバイダ実装の役割は、 クライアントのアイデンティティを *ロール* トークンの形式で確立することです。  
このロールトークンを使用して、このクライアントが特定のトピックに対してproduceまたはconsumeを許可されているかどうかを検証します。

## 認証プロバイダ

### TLSクライアント認証

PulsarクライアントとBroker間の接続暗号化を提供することに加えて、  
TLSは信頼された認証局 (CA) によって署名された証明書を通してクライアントを識別できます。

**注**: 他のPulsarコードとは異なり、TLS認証プロバイダはYahooのプロダクションでは使用されていません。  
使用する際に発生した問題があれば報告してください。


#### 証明書の作成

##### 認証局 (CA)

最初のステップは、CAの証明書を作成することです。  
CAはBrokerとクライアント両方の証明書に署名するために用いられ、お互いを信頼できるようにします。

```shell
# Linuxシステム上で:
$ CA.pl -newca

# MacOSX上で
$ /System/Library/OpenSSL/misc/CA.pl -newca
```

 コマンドライン上の質問に回答後、CA関連のファイルが`./demoCA`配下に作成されます。
 * `demoCA/cacert.pem` は公開鍵証明書です。全ての関係者に配布されます。
 * `demoCA/private/cakey.pem` は秘密鍵です。Brokerまたはクライアントの新規の証明書に署名するときのみ必要になります。安全な場所に保管してください。

##### Brokerの証明書

証明書リクエストを作成し、CAの公開鍵証明書で署名します。

これらのコマンドはいくつかの質問をし、証明書を作成します。  
コモンネームは、Brokerのホスト名と一致させる必要があります。  
Brokerのホスト名のグループにマッチするワイルドカードを利用することも可能です。  
例えば`*.broker.usw.example.com`のようにすることで、同じ証明書を複数マシンで再利用できます。

```shell
$ openssl req -newkey rsa:2048 -sha256 -nodes -out broker-cert.csr -outform PEM

# 鍵をPKCS#8フォーマットに変換
$ openssl pkcs8 -topk8 -inform PEM -outform PEM -in privkey.pem -out broker-key.pem -nocrypt
```

このコマンドによりBrokerの証明書リクエストファイル (`broker-cert.csr`と`broker-key.pem`) が生成されます。

これで署名付き証明書の作成に進むことができます:

```shell
$ openssl ca -out broker-cert.pem -infiles broker-cert.csr
```

この時点で、Brokerに必要な`broker-cert.pem`と`broker-key.pem`が用意できました。

##### クライアントの証明書

Brokerと同じステップを繰り返して、`client-cert.pem` と `client-key.pem`を作成してください。

クライアントのコモンネームは、クライアントのホスト名と一致させる必要はありませんが、  
*ロール*トークンで使用予定の文字列を用いる必要があります。

#### Brokerの設定

`conf/broker.conf`にPulsar BrokerのTLS認証を設定:  

```shell
tlsEnabled=true
tlsCertificateFilePath=/path/to/broker-cert.pem
tlsKeyFilePath=/path/to/broker-key.pem
tlsTrustCertsFilePath=/path/to/cacert.pem

# Add TLS auth provider
authenticationEnabled=true
authorizationEnabled=true
authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderTls
```

#### ディスカバリサービスの設定

ディスカバリサービスはHTTPSリクエストのリダイレクト処理を行うため、同様にクライアントから信頼される必要があります。  
`conf/discovery.conf`にTLS認証の設定を追加：  
```shell
tlsEnabled=true
tlsCertificateFilePath=/path/to/broker-cert.pem
tlsKeyFilePath=/path/to/broker-key.pem
```

#### Javaクライアントの設定

```java
ClientConfiguration conf = new ClientConfiguration();
conf.setUseTls(true);
conf.setTlsTrustCertsFilePath("/path/to/cacert.pem");

Map<String, String> authParams = new HashMap<>();
authParams.put("tlsCertFile", "/path/to/client-cert.pem");
authParams.put("tlsKeyFile", "/path/to/client-cert.pem");
conf.setAuthentication(AuthenticationTls.class.getName(), authParams);

PulsarClient client = PulsarClient.create(
                        "https://my-broker.com:4443", conf);
```

#### CLIツールの設定

`pulsar-admin`, `pulsar-perf`や`pulsar-client`のようなコマンドラインツールは設定ファイル`conf/client.conf`を利用します。  
認証パラメータの追加:

```shell
serviceUrl=https://broker.example.com:8443/
authPlugin=org.apache.pulsar.client.impl.auth.AuthenticationTls
authParams=tlsCertFile:/path/to/client-cert.pem,tlsKeyFile:/path/to/client-cert.pem
useTls=true
tlsAllowInsecureConnection=false
tlsTrustCertsFilePath=/path/to/cacert.pem
```
