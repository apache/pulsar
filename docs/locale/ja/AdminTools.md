
# Pulsar adminツール

<!-- TOC depthFrom:1 depthTo:5 withLinks:1 updateOnSave:1 orderedList:0 -->

- [Pulsar adminツール](#pulsar-adminツール)
	- [Pulsar adminツールとは？](#pulsar-adminツールとは)
		- [どのように動作するか？](#どのように動作するか)
	- [利用準備](#利用準備)
		- [Pulsar admin CLIツール](#pulsar-admin-cliツール)
		- [REST API](#rest-api)
		- [Java API](#java-api)
		- [Pulsarエンティティ](#pulsarエンティティ)
		- [Brokers](#brokers)
			- [アクティブなBrokerリストの取得](#アクティブなbrokerリストの取得)
			- [Brokerが所有するネームスペースリストの取得](#brokerが所有するネームスペースリストの取得)
		- [Properties](#properties)
			- [存在するプロパティリストの取得](#存在するプロパティリストの取得)
			- [プロパティの作成](#プロパティの作成)
			- [プロパティの取得](#プロパティの取得)
			- [プロパティの更新](#プロパティの更新)
			- [プロパティの削除](#プロパティの削除)
		- [Clusters](#clusters)
			- [クラスタの作成](#クラスタの作成)
			- [クラスタの取得](#クラスタの取得)
			- [クラスタの更新](#クラスタの更新)
			- [クラスタの削除](#クラスタの削除)
			- [全クラスタリストの取得](#全クラスタリストの取得)
		- [Namespace](#namespace)
			- [ネームスペースの作成](#ネームスペースの作成)
			- [ネームスペースの取得](#ネームスペースの取得)
			- [プロパティ内の全ネームスペースリストの取得](#プロパティ内の全ネームスペースリストの取得)
			- [クラスタ内の全ネームスペースリストの取得](#クラスタ内の全ネームスペースリストの取得)
			- [ネームスペースの削除](#ネームスペースの削除)
			- [パーミッションの付与](#パーミッションの付与)
			- [パーミッションの取得](#パーミッションの取得)
			- [パーミッションの剥奪](#パーミッションの剥奪)
			- [レプリケーションクラスタの設定](#レプリケーションクラスタの設定)
			- [レプリケーションクラスタの取得](#レプリケーションクラスタの取得)
			- [バックログクォータポリシーの設定](#バックログクォータポリシーの設定)
			- [バックログクォータポリシーの取得](#バックログクォータポリシーの取得)
			- [バックログクォータポリシーの削除](#バックログクォータポリシーの削除)
			- [永続性ポリシーの設定](#永続性ポリシーの設定)
			- [永続性ポリシーの取得](#永続性ポリシーの取得)
			- [ネームスペースBundleのアンロード](#ネームスペースbundleのアンロード)
			- [メッセージTTLの設定](#メッセージttlの設定)
			- [メッセージTTLの取得](#メッセージttlの取得)
			- [Bundleの分割](#bundleの分割)
			- [バックログの削除](#バックログの削除)
			- [Bundleバックログの削除](#bundleバックログの削除)
			- [リテンションの設定](#リテンションの設定)
			- [リテンションの取得](#リテンションの取得)
		- [Persistent](#persistent)
			- [パーティションドトピックの作成](#パーティションドトピックの作成)
			- [パーティションドトピックの取得](#パーティションドトピックの取得)
			- [パーティションドトピックの削除](#パーティションドトピックの削除)
			- [トピックの削除](#トピックの削除)
			- [トピックリストの取得](#トピックリストの取得)
			- [パーミッションの付与](#パーミッションの付与)
			- [パーミッションの取得](#パーミッションの取得)
			- [パーミッションの剥奪](#パーミッションの剥奪)
			- [パーティションドトピックの統計情報の取得](#パーティションドトピックの統計情報の取得)
			- [統計情報の取得](#統計情報の取得)
			- [詳細な統計情報の取得](#詳細な統計情報の取得)
			- [メッセージを見る](#メッセージを見る)
			- [メッセージのスキップ](#メッセージのスキップ)
			- [全メッセージのスキップ](#全メッセージのスキップ)
			- [メッセージを有効期限切れにする](#メッセージを有効期限切れにする)
			- [全メッセージを有効期限切れにする](#全メッセージを有効期限切れにする)
			- [カーソルのリセット](#カーソルのリセット)
			- [トピックのルックアップ](#トピックのルックアップ)
			- [サブスクリプションリストの取得](#サブスクリプションリストの取得)
			- [購読解除](#購読解除)
		- [ネームスペースの隔離ポリシー](#ネームスペースの隔離ポリシー)
			- [ネームスペースの隔離ポリシーの作成/更新](#ネームスペースの隔離ポリシーの作成更新)
			- [ネームスペースの隔離ポリシーの取得](#ネームスペースの隔離ポリシーの取得)
			- [ネームスペースの隔離ポリシーの削除](#ネームスペースの隔離ポリシーの削除)
			- [ネームスペースの隔離ポリシー全リストの取得](#ネームスペースの隔離ポリシー全リストの取得)
		- [リソース割り当て](#リソース割り当て)
			- [ネームスペースへのリソース割り当ての設定](#ネームスペースへのリソースの割り当ての設定)
			- [ネームスペースへのリソース割り当ての取得](#ネームスペースへのリソースの割り当ての取得)
			- [ネームスペースへのリソース割り当てのリセット](#ネームスペースへのリソースの割り当てのリセット)
		- [Pulsarクライアントツール](#pulsarクライアントツール)
			- [メッセージをproduceするコマンド](#メッセージをproduceするコマンド)
			- [メッセージをconsumeするコマンド](#メッセージをconsumeするコマンド)

<!-- /TOC -->

## Pulsar adminツールとは？

Pulsar adminツールはコマンドラインユーティリティであり、プロパティ、クラスタ、トピック、ネームスペースなどPulsar Brokerのさまざまなエンティティを設定および監視するための、Brokerと管理の間のインターフェースを提供します。これはプロパティ、クラスタ、ネームスペースを作成できるようにし、アプリケーションをPulsarに載せるのに便利な管理ツールです。またトピックの状態と使用方法を管理する際にも役立ちます。CLI (command line interface) の他に、上述のエンティティを管理および監視するためにREST APIとJava APIの2つの代替手段を使うこともできます。そこでこのドキュメントではCLI, REST API, Java APIを使ってPulsarのエンティティを管理する方法について説明します。

### どのように動作するか？

前述の通り、Pulsar BrokerはPulsarのさまざまなエンティティを設定および管理するためのREST APIを公開しています。Pulsar admin CLIツールはadminコマンドを実行するために、Javaクライアントを使ってこれらのREST APIをコールします。利用可能なREST APIのリストについての詳細は*swaggerドキュメント*から学ぶことができます。

## 利用準備

### Pulsar admin CLIツール

Pulsar Brokerが受け取ったリクエストに対して認証認可を行うことが可能である*Pulsarのセキュリティ*機能については他の章で述べました。adminツールはコマンドのリストを実行するためにPulsar BrokerのREST APIを呼びます。しかし、Pulsar Brokerのセキュリティが有効になっている場合、認証されたリクエストを得るためにBrokerのREST APIを呼ぶ際にadminツールは追加の情報を渡す必要があります。そこでこの情報が`conf/client.conf`ファイルにおいて正しく設定されていることを確認してください。

これでPulsar admin CLIツールを使う準備が整いました。adminツールの利用を開始するため、下記のコマンドを実行してください。

```shell
$ bin/pulsar-admin --help
```

前章でPulsarのエンティティを管理するための他の代替手段 - REST APIとJava API - について述べました。このドキュメントではPulsarのエンティティを管理するためのREST APIのエンドポイントとJava APIのスニペットについても説明します。

### REST API

Pulsar admin REST APIの定義については*swaggerドキュメント*から学ぶことができます。このドキュメントでは各APIの利用方法とadmin CLIコマンドがどのようにREST APIに対応するかについて説明します。

### Java API

Java APIは```com.yahoo.pulsar.client.admin.PulsarAdmin```によりアクセスできます。

下記のコードスニペットは*PulsarAdmin*の初期化の方法を示しています。後のドキュメントでPulsarのエンティティを管理するための使用方法について説明します。


  ```java
  URL url = new URL("http://localhost:8080");
  String authPluginClassName = "com.org.MyAuthPluginClass"; //Pass auth-plugin class fully-qualified name if Pulsar-security enabled
  String authParams = "param1=value1";//Pass auth-param if auth-plugin class requires it
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

### Pulsarエンティティ

主にadminコマンドラインツールを使って下記のBrokerのエンティティにアクセスします。もしまだ下記のエンティティに対する理解が不十分なのであれば、[Pulsar入門](GettingStarted.md)で詳細を読むことができます。

### Brokers

アクティブなBrokerのリストおよび指定されたBrokerが所有するネームスペースのリストを取得できます。

#### アクティブなBrokerリストの取得
トラフィックを処理している利用可能でアクティブなBrokerを取得します。

###### CLI

```
$ pulsar-admin brokers list use
```

```
broker1.use.org.com:8080
```

###### REST

```
GET /admin/brokers/{cluster}
```

###### Java

```java
admin.brokers().getActiveBrokers(clusterName)
```


#### Brokerが所有するネームスペースリストの取得

指定したBrokerが所有し処理しているすべてのネームスペースを取得します。

###### CLI

```
$ pulsar-admin brokers namespaces --url broker1.use.org.com:8080 use
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

```
GET /admin/brokers/{cluster}/{broker}/ownedNamespaces
```

###### Java

```java
admin.brokers().getOwnedNamespaces(cluster,brokerUrl)
```


### Properties

プロパティはアプリケーションのドメインを表します。例えばfinance, mail, sportsなどがプロパティの例です。ツールを使ってPulsarのプロパティを管理するためのCRUD操作を行うことができます。

#### 存在するプロパティリストの取得

Pulsarシステムに存在しているすべてのプロパティのリストを表示します。

###### CLI

```
$ pulsar-admin properties list
```

```
my-property
```

###### REST

```
GET /admin/properties
```

###### Java

```java
admin.properties().getProperties()
```


#### プロパティの作成


Pulsarシステムに新しいプロパティを作成します。プロパティに対し、プロパティの管理権限を持つadminロール（コンマ区切り）とプロパティが利用可能なクラスタ（コンマ区切り）を設定できます。

###### CLI

```
pulsar-admin properties create my-property --admin-roles admin1,admin2 --allowed-clusters cl1,cl2
```

```
N/A
```

###### REST

```
PUT /admin/properties/{property}
```

###### Java

```java
admin.properties().createProperty(property, propertyAdmin)
```


#### プロパティの取得

指定した既存プロパティの設定を取得します。

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

```
GET /admin/properties/{property}
```

###### Java

```java
admin.properties().getPropertyAdmin(property)
```



#### プロパティの更新

既に作成されたプロパティの設定を更新します。指定した既存プロパティのadminロールとクラスタの情報を更新できます。

###### CLI

```$ pulsar-admin properties update my-property --admin-roles admin-update-1,admin-update-2 --allowed-clusters cl1,cl2```

```
N/A
```

###### REST

```
POST /admin/properties/{property}
```

###### Java

```java
admin.properties().updateProperty(property, propertyAdmin)
```


#### プロパティの削除

既存プロパティをPulsarシステムから削除します。
###### CLI

```
$ pulsar-admin properties delete my-property
```

```
N/A
```

###### REST

```
DELETE /admin/properties/{property}
```

###### Java

```java
admin.properties().deleteProperty(property)
```




### Clusters

クラスタは一つ以上の地理的位置においてプロパティとそのネームスペースを利用可能にします。クラスタは一般的にはuse, uswなどの地域のコロケーション名に対応します。ツールを使ってPulsarのクラスタを管理するためのCRUD操作を行うことができます。

#### クラスタの作成

Pulsarに新しいクラスタを作成します。このようなシステムレベルの操作はスーパーユーザ権限でのみ実行できます。

###### CLI

```
$ pulsar-admin clusters create --url http://my-cluster.org.com:8080/ --broker-url pulsar://my-cluster.org.com:6650/ cl1
```

```
N/A
```

###### REST

```
PUT /admin/clusters/{cluster}
```

###### Java

```java
admin.clusters().createCluster(cluster, new ClusterData(serviceUrl, serviceUrlTls, brokerServiceUrl, brokerServiceUrlTls))
```



#### クラスタの取得

指定した既存クラスタの設定を取得します。

###### CLI

```
$ pulsar-admin clusters get cl1
```

```json
{
    "serviceUrl": "http://my-cluster.org.com:8080/",
    "serviceUrlTls": null,
    "brokerServiceUrl": "pulsar://my-cluster.org.com:6650/",
    "brokerServiceUrlTls": null
}
```

###### REST

```
GET /admin/clusters/{cluster}
```

###### Java

```java
admin.clusters().getCluster(cluster)
```


#### クラスタの更新

指定した既存クラスタの設定を更新します。

###### CLI

```
$ pulsar-admin clusters update --url http://my-cluster.org.com:4081/ --broker-url pulsar://my-cluster.org.com:3350/ cl1
```

```
N/A
```

###### REST

```
POST /admin/clusters/{cluster}
```

###### Java

```java
admin.clusters().updateCluster(cluster, new ClusterData(serviceUrl, serviceUrlTls, brokerServiceUrl, brokerServiceUrlTls))
```


#### クラスタの削除

Pulsarシステムから既存クラスタを削除します。

###### CLI

```
$ pulsar-admin clusters delete cl1
```

```
N/A
```

###### REST

```
DELETE /admin/clusters/{cluster}
```

###### Java

```java
admin.clusters().deleteCluster(cluster)
```


#### 全クラスタリストの取得

Pulsarシステム内に作成されたすべてのクラスタのリストを取得します。

###### CLI

```
$ pulsar-admin clusters list
```

```
cl1
```

###### REST

```
GET /admin/clusters
```

###### Java

```java
admin.clusters().getClusters()
```


### Namespace

ネームスペースはプロパティ内の論理的な区切りの名称です。一つのプロパティはそのプロパティの下で異なるアプリケーションを管理するために複数のネームスペースを持つことができます。

#### ネームスペースの作成

指定した既存クラスタ内のプロパティにネームスペースを作成します。

###### CLI

```
$ pulsar-admin namespaces create test-property/cl1/ns1
```

```
N/A
```

###### REST

```
PUT /admin/namespaces/{property}/{cluster}/{namespace}
```

###### Java

```java
admin.namespaces().createNamespace(namespace)
```

#### ネームスペースの取得

作成済みネームスペースのポリシー情報を取得します。

###### CLI

```
$pulsar-admin namespaces policies test-property/cl1/ns1
```

```json
{
    "auth_policies": {
        "namespace_auth": {},
        "destination_auth": {}
    },
    "replication_clusters": [],
    "bundles_activated": true,
    "bundles": {
        "boundaries": [
            "0x00000000",
            "0xffffffff"
        ],
        "numBundles": 1
    },
    "backlog_quota_map": {},
    "persistence": null,
    "latency_stats_sample_rate": {},
    "message_ttl_in_seconds": 0,
    "retention_policies": null,
    "deleted": false
}
```

###### REST

```
GET /admin/namespaces/{property}/{cluster}/{namespace}
```

###### Java

```java
admin.namespaces().getPolicies(namespace)
```


#### プロパティ内の全ネームスペースリストの取得

指定したプロパティ内にあるすべての作成済みネームスペースのリストを取得します。

###### CLI

```
$ pulsar-admin namespaces list test-property
```

```
test-property/cl1/ns1
```

###### REST

```
GET /admin/namespaces/{property}
```

###### Java

```java
admin.namespaces().getNamespaces(property)
```


#### クラスタ内の全ネームスペースリストの取得

指定したクラスタのプロパティ内にあるすべての作成済みネームスペースのリストを取得します。

###### CLI

```
$ pulsar-admin namespaces list-cluster test-property/cl1
```

```
test-property/cl1/ns1
```

###### REST

```
GET /admin/namespaces/{property}/{cluster}
```

###### Java

```java
admin.namespaces().getNamespaces(property, cluster)
```

#### ネームスペースの削除

既存のネームスペースを削除します。

###### CLI

```
$ pulsar-admin namespaces delete test-property/cl1/ns1
```

```
N/A
```

###### REST

```
DELETE /admin/namespaces/{property}/{cluster}/{namespace}
```

###### Java

```java
admin.namespaces().deleteNamespace(namespace)
```

#### パーミッションの付与

特定のクライアントロールに対して、produceやconsumeのような必要な操作のリストを許可します。

###### CLI

```
$ pulsar-admin namespaces grant-permission --actions produce,consume --role admin10 test-property/cl1/ns1
```

```
N/A
```

###### REST

```
POST /admin/namespaces/{property}/{cluster}/{namespace}/permissions/{role}
```

###### Java

```java
admin.namespaces().grantPermissionOnNamespace(namespace, role, getAuthActions(actions))
```


#### パーミッションの取得

指定したネームスペースに対して作成されたパーミッションルールを表示します。

###### CLI

```
$ pulsar-admin namespaces permissions test-property/cl1/ns1
```

```json
{
    "admin10": [
        "produce",
        "consume"
    ]
}   
```

###### REST

```
GET /admin/namespaces/{property}/{cluster}/{namespace}/permissions
```

###### Java

```java
admin.namespaces().getPermissions(namespace)
```

#### パーミッションの剥奪

特定のクライアントロールのパーミッションを剥奪し、指定したネームスペースにアクセスできないようにします。

###### CLI

```
$ pulsar-admin namespaces revoke-permission --role admin10 test-property/cl1/ns1
```

```
N/A
```

###### REST

```
DELETE /admin/namespaces/{property}/{cluster}/{namespace}/permissions/{role}
```

###### Java

```java
admin.namespaces().revokePermissionsOnNamespace(namespace, role)
```


#### レプリケーションクラスタの設定

ネームスペースにレプリケーションクラスタを設定し、Pulsarが内部的に発行されたメッセージを一つのコロケーションから別のコロケーションにレプリケートできるようにします。しかし、レプリケーションクラスタをセットするためにはネームスペースは*test-property/**global**/ns1.*のようにグローバルである必要があります。つまりクラスタ名は*“global”*でなければなりません。

###### CLI

```
$ pulsar-admin namespaces set-clusters --clusters cl2 test-property/cl1/ns1
```

```
N/A
```

###### REST

```
POST /admin/namespaces/{property}/{cluster}/{namespace}/replication
```

###### Java

```java
admin.namespaces().setNamespaceReplicationClusters(namespace, clusters)
```    

#### レプリケーションクラスタの取得

指定したネームスペースのレプリケーションクラスタのリストを取得します。

###### CLI

```
$ pulsar-admin namespaces get-clusters test-property/cl1/ns1
```

```
cl2
```

###### REST

```
GET /admin/namespaces/{property}/{cluster}/{namespace}/replication
```

###### Java

```java
admin.namespaces().getNamespaceReplicationClusters(namespace)
```

#### バックログクォータポリシーの設定

バックログクォータはBrokerが特定の閾値に達したとき、ネームスペースの帯域幅/ストレージを制限するのに役立ちます。管理者はこの制限と制限に達したときに行う下記のアクションの内一つを設定できます。

  1.  producer_request_hold: Brokerはproduceリクエストのペイロードをホールドし、永続化しないようになります

  2.  producer_exception: Brokerは例外を発生させてクライアントとの接続を切断します

  3.  consumer_backlog_eviction: Brokerはバックログメッセージの破棄を開始します

  バックログクォータ制限はバックログクォータタイプ: destination_storageを定義することによって考慮されるようになります。

###### CLI

```
$ pulsar-admin namespaces set-backlog-quota --limit 10 --policy producer_request_hold test-property/cl1/ns1
```

```
N/A
```

###### REST

```
POST /admin/namespaces/{property}/{cluster}/{namespace}/backlogQuota
```

###### Java

```java
admin.namespaces().setBacklogQuota(namespace, new BacklogQuota(limit, policy))
```

#### バックログクォータポリシーの取得

指定したネームスペースのバックログクォータ設定を表示します。

###### CLI

```
$ pulsar-admin namespaces get-backlog-quotas test-property/cl1/ns1
```

```json
{
    "destination_storage": {
        "limit": 10,
        "policy": "producer_request_hold"
    }
}          
```

###### REST

```
GET /admin/namespaces/{property}/{cluster}/{namespace}/backlogQuotaMap
```

###### Java

```java
admin.namespaces().getBacklogQuotaMap(namespace)
```




#### バックログクォータポリシーの削除  

指定したネームスペースのバックログクォータポリシーを削除します。

###### CLI

```
$ pulsar-admin namespaces remove-backlog-quota test-property/cl1/ns1
```

```
N/A
```

###### REST

```
DELETE /admin/namespaces/{property}/{cluster}/{namespace}/backlogQuota
```

###### Java

```java
admin.namespaces().removeBacklogQuota(namespace, backlogQuotaType)
```


#### 永続性ポリシーの設定

永続性ポリシーは指定したネームスペースにあるすべてのトピックの永続性レベルを設定できます。

  -   Bookkeeper-ack-quorum: 各エントリに対して書き込み成功のAckを待機するBookieの数（保証されるコピーの数）、デフォルト: 0

  -   Bookkeeper-ensemble: 一つのトピックに対して使用されるBookieの数、デフォルト: 0

  -   Bookkeeper-write-quorum: 各エントリに対して書き込みを行うBookieの数、デフォルト: 0

  -   Ml-mark-delete-max-rate: mark-delete操作のスロットル率 (0は無制限)、デフォルト: 0.0  

###### CLI

```
$ pulsar-admin namespaces set-persistence --bookkeeper-ack-quorum 2 --bookkeeper-ensemble 3 --bookkeeper-write-quorum 2 --ml-mark-delete-max-rate 0 test-property/cl1/ns1
```

```
N/A
```

###### REST

```
POST /admin/persistent/{property}/{cluster}/{namespace}/persistence
```

###### Java

```java
admin.namespaces().setPersistence(namespace,new PersistencePolicies(bookkeeperEnsemble, bookkeeperWriteQuorum,bookkeeperAckQuorum,managedLedgerMaxMarkDeleteRate))
```


#### 永続性ポリシーの取得

指定したネームスペースの永続性ポリシーの設定を表示します。

###### CLI

```
$ pulsar-admin namespaces get-persistence test-property/cl1/ns1
```

```json
{
    "bookkeeperEnsemble": 3,
    "bookkeeperWriteQuorum": 2,
    "bookkeeperAckQuorum": 2,
    "managedLedgerMaxMarkDeleteRate": 0
}
```

###### REST

```
GET /admin/namespaces/{property}/{cluster}/{namespace}/persistence
```

###### Java

```java
admin.namespaces().getPersistence(namespace)
```


#### ネームスペースBundleのアンロード

ネームスペースBundleは同じネームスペースに属するトピックの仮想的なグループです。多数のBundleによりBrokerの負荷が高まった場合、このコマンドを使って処理が重いBundleをそのBrokerから取り外し、より負荷が小さい他のBrokerに扱わせることができます。ネームスペースBundleは0x00000000と0xffffffffのように開始と終了のレンジによって定義されます。

###### CLI

```
$ pulsar-admin namespaces unload --bundle 0x00000000_0xffffffff test-property/pstg-gq1/ns1
```

```
N/A
```

###### REST

```
PUT /admin/namespaces/{property}/{cluster}/{namespace}/unload
```

###### Java

```java
admin.namespaces().unloadNamespaceBundle(namespace, bundle)
```


#### メッセージTTLの設定

メッセージの生存時間（秒）を設定します。

###### CLI

```
$ pulsar-admin namespaces set-message-ttl --messageTTL 100 test-property/cl1/ns1
```

```
N/A
```

###### REST

```
POST /admin/namespaces/{property}/{cluster}/{namespace}/messageTTL
```

###### Java

```java
admin.namespaces().setNamespaceMessageTTL(namespace, messageTTL)
```

#### メッセージTTLの取得

ネームスペースに対して設定されたメッセージTTLを取得します。

###### CLI

```
$ pulsar-admin namespaces get-message-ttl test-property/cl1/ns1
```

```
100
```


###### REST

```
GET /admin/namespaces/{property}/{cluster}/{namespace}/messageTTL
```

###### Java

```java
admin.namespaces().getNamespaceReplicationClusters(namespace)
```


#### Bundleの分割

各ネームスペースのBundleは複数のトピックを含み、各Bundleはただ一つのBrokerによって扱われます。Bundleがそれに含まれる複数のトピックの処理で重くなった場合、Brokerに対し負荷を発生させます。これを解決するため、管理者はこのコマンドを用いてBundleを分割できます。

###### CLI

```
$ pulsar-admin namespaces split-bundle --bundle 0x00000000_0xffffffff test-property/cl1/ns1
```

```
N/A
```

###### REST

```
PUT /admin/namespaces/{property}/{cluster}/{namespace}/{bundle}/split
```

###### Java

```java
admin.namespaces().splitNamespaceBundle(namespace, bundle)
```


#### バックログの削除

指定したネームスペースに属するすべてのトピックのすべてのメッセージバックログを削除します。特定のサブスクリプションのバックログのみを削除することも可能です。

###### CLI

```
$ pulsar-admin namespaces clear-backlog --sub my-subscription test-property/pstg-gq1/ns1
```

```
N/A
```

###### REST

```
POST /admin/namespaces/{property}/{cluster}/{namespace}/clearBacklog
```

###### Java

```java
admin.namespaces().clearNamespaceBacklogForSubscription(namespace, subscription)
```


#### Bundleバックログの削除

特定のネームスペースBundleに属するすべてのトピックのすべてのメッセージバックログを削除します。特定のサブスクリプションのバックログのみを削除することも可能です。

###### CLI

```
$ pulsar-admin namespaces clear-backlog  --bundle 0x00000000_0xffffffff  --sub my-subscription test-property/pstg-gq1/ns1
```

```
N/A
```

###### REST

```
POST /admin/namespaces/{property}/{cluster}/{namespace}/{bundle}/clearBacklog
```

###### Java

```java
admin.namespaces().clearNamespaceBundleBacklogForSubscription(namespace, bundle, subscription)
```


#### リテンションの設定

各ネームスペースは複数のトピックを含み、各トピックのリテンションサイズ（ストレージサイズ）は特定の閾値を超えるべきではなく、特定の期間まで保持されるべきです。このコマンドを使って、指定したネームスペース内のトピックのリテンションサイズと時間を設定できます。

###### CLI

```
$ pulsar-admin set-retention --size 10 --time 100 test-property/cl1/ns1
```

```
N/A
```

###### REST

```
POST /admin/namespaces/{property}/{cluster}/{namespace}/retention
```

###### Java

```java
admin.namespaces().setRetention(namespace, new RetentionPolicies(retentionTimeInMin, retentionSizeInMB))
```


#### リテンションの取得

指定したネームスペースのリテンション情報を表示します。

###### CLI

```
$ pulsar-admin namespaces get-retention test-property/cl1/ns1
```

```json
{
    "retentionTimeInMinutes": 10,
    "retentionSizeInMB": 100
}
```

###### REST

```
GET /admin/namespaces/{property}/{cluster}/{namespace}/retention
```

###### Java

```java
admin.namespaces().getRetention(namespace)
```



### Persistent

persistentコマンドは、メッセージをproduce/consumeするための論理的なエンドポイントであるトピックにアクセスする際に役立ちます。  
Producerはトピックにメッセージをproduceし、Consumerはトピックにproduceされたメッセージをconsumeするためにトピックを購読します。

以降に説明とコマンドを記載します - パーシステントトピックのフォーマットは次の通りです:

```persistent://<property_name> <cluster_name> <namespace_name> <topic-name>```

#### パーティションドトピックの作成


指定されたネームスペース下にパーティションドトピックを作成します。作成のためには、パーティション数は1より大きくなくてはいけません。


###### CLI


```
$ pulsar-admin persistent create-partitioned-topic --partitions 2 persistent://test-property/cl1/ns1/pt1
```

```
N/A
```

###### REST

```
PUT /admin/persistent/{property}/{cluster}/{namespace}/{destination}/partitions
```

###### Java

```java
admin.persistentTopics().createPartitionedTopic(persistentTopic, numPartitions)
```

#### パーティションドトピックの取得

作成されたパーティションドトピックのメタデータを提供します。

###### CLI

```
$ pulsar-admin persistent get-partitioned-topic-metadata persistent://test-property/cl1/ns1/pt1
```

```json
{
    "partitions": 2
}
```

###### REST

```
GET /admin/persistent/{property}/{cluster}/{namespace}/{destination}/partitions
```

###### Java

```java
admin.persistentTopics().getPartitionedTopicMetadata(persistentTopic)
```


#### パーティションドトピックの削除

作成されたパーティションドトピックを削除します。

###### CLI

```
$ pulsar-admin persistent delete-partitioned-topic persistent://test-property/cl1/ns1/pt1
```

```
N/A
```

###### REST
```
DELETE /admin/persistent/{property}/{cluster}/{namespace}/{destination}/partitions
```

###### Java

```java
admin.persistentTopics().deletePartitionedTopic(persistentTopic)
```


#### トピックの削除

トピックを削除します。ただしアクティブなサブスクリプションまたはProducerの接続がある場合には、トピックを削除できません。

###### CLI

```
pulsar-admin persistent delete persistent://test-property/cl1/ns1/my-topic
```

```
N/A
```

###### REST

```
DELETE /admin/persistent/{property}/{cluster}/{namespace}/{destination}
```

###### Java

```java
admin.persistentTopics().delete(persistentTopic)
```


#### トピックリストの取得

指定されたネームスペース下に存在するパーシステントトピックのリストを提供します。

###### CLI

```
$ pulsar-admin persistent list test-property/cl1/ns1
```

```
my-topic
```

###### REST

```
GET /admin/persistent/{property}/{cluster}/{namespace}
```

###### Java

```java
admin.persistentTopics().getList(namespace)
```

#### パーミッションの付与

指定されたトピックに対して特定のアクションを実行するためのパーミッションをクライアントロールに付与します。

###### CLI

```
$ pulsar-admin persistent grant-permission --actions produce,consume --role application1 persistent://test-property/cl1/ns1/tp1
```

```
N/A
```

###### REST

```
POST /admin/persistent/{property}/{cluster}/{namespace}/{destination}/permissions/{role}
```

###### Java

```java
admin.persistentTopics().grantPermission(destination, role, getAuthActions(actions))
```

#### パーミッションの取得

指定されたトピックに対してのクライアントロールのパーミッションのリストを表示します。

###### CLI

```
$ pulsar-admin permissions persistent://test-property/cl1/ns1/tp1
```

```json
{
    "application1": [
        "consume",
        "produce"
    ]
}
```

###### REST

```
GET /admin/persistent/{property}/{cluster}/{namespace}/{destination}/permissions
```

###### Java

```java
admin.persistentTopics().getPermissions(destination)
```

#### パーミッションの剥奪

クライアントロールに対して付与されたパーミッションを剥奪します。

###### CLI

```
$ pulsar-admin persistent revoke-permission --role application1 persistent://test-property/cl1/ns1/tp1
```

```
N/A
```

###### REST

```
DELETE /admin/persistent/{property}/{cluster}/{namespace}/{destination}/permissions/{role}
```

###### Java

```java
admin.persistentTopics().revokePermissions(destination, role)
```

#### パーティションドトピックの統計情報の取得

パーティションドトピックの現在の統計情報を表示します。

###### CLI

```
$ pulsar-admin persistent partitioned-stats --per-partition persistent://test-property/cl1/ns1/tp1
```

```json
{
    "msgRateIn": 4641.528542257553,
    "msgThroughputIn": 44663039.74947473,
    "msgRateOut": 0,
    "msgThroughputOut": 0,
    "averageMsgSize": 1232439.816728665,
    "storageSize": 135532389160,
    "publishers": [
        {
            "msgRateIn": 57.855383881403576,
            "msgThroughputIn": 558994.7078932219,
            "averageMsgSize": 613135,
            "producerId": 0,
            "producerName": null,
            "address": null,
            "connectedSince": null
        }
    ],
    "subscriptions": {
        "my-topic_subscription": {
            "msgRateOut": 0,
            "msgThroughputOut": 0,
            "msgBacklog": 116632,
            "type": null,
            "msgRateExpired": 36.98245516804671,
            "consumers": []
        }
    },
    "replication": {}
}          
```

###### REST

```
GET /admin/persistent/{property}/{cluster}/{namespace}/{destination}/partitioned-stats
```

###### Java

```java
admin.persistentTopics().getPartitionedStats(persistentTopic, perPartition)
```


#### 統計情報の取得

パーティションドトピックではないトピックの現在の統計情報を表示します。

  -   **msgRateIn**: 全てのローカルとレプリケーション用のPublisherの発行レートの合計で、1秒あたりのメッセージ数です。

  -   **msgThroughputIn**: 上記と同様ですが、1秒あたりのバイト数です。

  -   **msgRateOut**: 全てのローカルとレプリケーション用のConsumerへの配送レートの合計で、1秒あたりのメッセージ数です。

  -   **msgThroughputOut**: 上記と同様ですが、1秒あたりのバイト数です。

  -   **averageMsgSize**: 直近のインターバル内で発行されたメッセージの平均バイトサイズです。

  -   **storageSize**: このトピックのLedgerのストレージサイズの合計です。

  -   **publishers**: トピック内の全てのローカルPublisherの一覧です。0または何千もの可能性があります。

  -   **averageMsgSize**: 直近のインターバル内でこのPublisherからのメッセージの平均バイトサイズです。

  -   **producerId**: このトピック上での、このProducerの内部的な識別子です。

  -   **producerName**: クライアントライブラリによって生成されたこのProducerの内部的な識別子です。

  -   **address**: このProducerの接続用のIPアドレスと送信元ポートです。

  -   **connectedSince**: このProducerが作成または最後に再接続したタイムスタンプです。

  -   **subscriptions**: トピックに対してのローカルの全サブスクリプションリストです。

  -   **my-subscription**: このサブスクリプションの名前です (クライアントが定義します) 。

  -   **msgBacklog**: このサブスクリプションのバックログ内のメッセージ数です。

  -   **type**: このサブスクリプションのタイプです。

  -   **msgRateExpired**: TTLのためにこのサブスクリプションから配送されずに破棄されたメッセージのレートです。

  -   **consumers**: このサブスクリプションに接続しているConsumerリストです。

  -   **consumerName**: クライアントライブラリによって生成されたこのConsumerの内部的な識別子です。

  -   **availablePermits**: このConsumerがクライアントライブラリのlistenキューに格納できるメッセージ数です。0はクライアントライブラリのキューがいっぱいであり、receive()はコールされないことを意味します。0でない場合には、このConsumerはメッセージを配送される準備ができています。

  -   **replication**: このセクションは、トピックのクラスタ間でのレプリケーションの統計情報を示します。

  -   **replicationBacklog**: レプリケーション先のバックログに送信されるメッセージです。

  -   **connected**: 送信レプリケータが接続されているかどうかです。

  -   **replicationDelayInSeconds**: connectedがtrueの場合で、最も古いメッセージが送信されるのを待っている時間です。

  -   **inboundConnection**: このBrokerに対しての、リモートクラスタのPublisher接続におけるそのBrokerのIPとポートです。

  -   **inboundConnectedSince**: リモートクラスタにメッセージを発行するためにTCP接続が使われます。もし接続しているローカルのPublisherがいない場合には、この接続は数分後に自動的に閉じられます。
###### CLI

```
$ pulsar-admin persistent stats persistent://test-property/cl1/ns1/tp1
```

```json
{
    "msgRateIn": 0,
    "msgThroughputIn": 0,
    "msgRateOut": 0,
    "msgThroughputOut": 0,
    "averageMsgSize": 0,
    "storageSize": 11814,
    "publishers": [
        {
            "msgRateIn": 0,
            "msgThroughputIn": 0,
            "averageMsgSize": 0,
            "producerId": 0,
            "producerName": "gq1-54-4001",
            "address": "/10.215.138.238:44458",
            "connectedSince": "2016-06-16 22:56:56.509"
        }
    ],
    "subscriptions": {
        "my-subscription": {
            "msgRateOut": 0,
            "msgThroughputOut": 0,
            "msgBacklog": 17,
            "type": "Shared",
            "msgRateExpired": 2.1771406267194497,
            "consumers": [
                {
                    "msgRateOut": 0,
                    "msgThroughputOut": 0,
                    "consumerName": "a67f7",
                    "availablePermits": 1186,
                    "address": "/10.215.166.121:35095",
                    "connectedSince": "2016-06-25 00:05:58.312"
                }
            ]
        }
    },
    "replication": {}
}
```

###### REST
```
GET /admin/persistent/{property}/{cluster}/{namespace}/{destination}/stats
```

###### Java

```java
admin.persistentTopics().getStats(persistentTopic)
```

#### 詳細な統計情報の取得

トピックの詳細な統計情報を表示します。

  -   **entriesAddedCounter**: このBrokerがこのトピックを読み込んでから発行されたメッセージ数です。

  -   **numberOfEntries**: 書き込まれたメッセージの総数です。

  -   **totalSize**: 全メッセージのバイト単位での合計ストレージサイズです。

  -   **currentLedgerEntries**: 現在openしているLedgerに書き込まれたメッセージ数です。

  -   **currentLedgerSize**: 現在openしているLedgerに書き込まれたメッセージのバイトサイズです。

  -   **lastLedgerCreatedTimestamp**: 最後のLedgerが作成された時刻です。

  -   **lastLedgerCreationFailureTimestamp:** 最後のLedgerに障害が発生した時刻です。

  -   **waitingCursorsCount**:"キャッチアップ状態"で新しいメッセージが発行されるのを待っているカーソル数です。

  -   **pendingAddEntriesCount**:  完了を待っている (非同期) 書き込みリクエストのメッセージ数です。

  -   **lastConfirmedEntry**: 書き込みに成功した最後のメッセージのledgerid:entryid。entryidが−1の場合、Ledgerがすでにオープンされているか現在オープンされていますが、まだ書き込まれたエントリがないことを意味します。

  -   **state**: このLedgerの書き込みのための状態です。LedgerOpenedは、発行されたメッセージを保存するためのLedgerをオープンしていることを意味します。

  -   **ledgers**:　このトピックのメッセージを保持している全てのLedgerの順序付きリストです。

  -   **cursors**: このトピック上の全てのカーソルのリストです。トピックの統計情報上に表示されたサブスクリプションごとに1つ表示されます。

  -   **markDeletePosition**: Ackのポジション:SubscriberからAckが返された最後のメッセージです。

  -   **readPosition**: メッセージを読むためのSubscriberの最新のポジションです。

  -   **waitingReadOp**: サブスクリプションが最新のメッセージを読み込み、新しいメッセージが発行されるのを待っている時に、これはtrueになります。

  -   **pendingReadOps**: 進行中のBookKeeperへの未解決の読み取りリクエスト数です。

  -   **messagesConsumedCounter**: このBrokerがこのトピックを読み込んでからこのカーソルがAckしたメッセージ数です。

  -   **cursorLedger**: 永続的に現在のmarkDeletePositionを保存するために利用されているLedgerです。

  -   **cursorLedgerLastEntry**: 永続的に現在のmarkDeletePositionを保存するために使われた最後のentryidです。

  -   **individuallyDeletedMessages**: もしAckが順不同で行われている場合に、markDeletePositionと読み込みポジションの間でAckされたメッセージの範囲を表示します。

  -   **lastLedgerSwitchTimestamp**: カーソルLedgerがロールオーバされた最後の時刻です。

  -   **state**: カーソルLedgerの状態: Openは、markDeletePositionのアップデートを保存するためのカーソルLedgerが存在することを意味します。

###### CLI

```
$ pulsar-admin persistent stats-internal persistent://test-property/cl1/ns1/tp1
```

```json
{
    "entriesAddedCounter": 20449518,
    "numberOfEntries": 3233,
    "totalSize": 331482,
    "currentLedgerEntries": 3233,
    "currentLedgerSize": 331482,
    "lastLedgerCreatedTimestamp": "2016-06-29 03:00:23.825",
    "lastLedgerCreationFailureTimestamp": null,
    "waitingCursorsCount": 1,
    "pendingAddEntriesCount": 0,
    "lastConfirmedEntry": "324711539:3232",
    "state": "LedgerOpened",
    "ledgers": [
        {
            "ledgerId": 324711539,
            "entries": 0,
            "size": 0
        }
    ],
    "cursors": {
        "my-subscription": {
            "markDeletePosition": "324711539:3133",
            "readPosition": "324711539:3233",
            "waitingReadOp": true,
            "pendingReadOps": 0,
            "messagesConsumedCounter": 20449501,
            "cursorLedger": 324702104,
            "cursorLedgerLastEntry": 21,
            "individuallyDeletedMessages": "[(324711539:3134‥324711539:3136], (324711539:3137‥324711539:3140], ]",
            "lastLedgerSwitchTimestamp": "2016-06-29 01:30:19.313",
            "state": "Open"
        }
    }
}
```


###### REST

```
GET /admin/persistent/{property}/{cluster}/{namespace}/{destination}/internalStats
```

###### Java

```java
admin.persistentTopics().getInternalStats(persistentTopic)
```


#### メッセージを見る

指定されたトピックの特定のサブスクリプションのNつのメッセージを覗き見ます。

###### CLI

```
$ pulsar-admin persistent peek-messages --count 10 --subscription my-subscription persistent://test-property/cl1/ns1/my-topic
```

```          
Message ID: 315674752:0  
Properties:  {  "X-Pulsar-publish-time" : "2015-07-13 17:40:28.451"  }
msg-payload
```

###### REST

```
GET /admin/persistent/{property}/{cluster}/{namespace}/{destination}/subscription/{subName}/position/{messagePosition}
```


###### Java

```java
admin.persistentTopics().peekMessages(persistentTopic, subName, numMessages)
```


#### メッセージのスキップ

指定されたトピックの指定されたサブスクリプションのNつのメッセージをスキップします。

###### CLI

```
$ pulsar-admin persistent skip --count 10 --subscription my-subscription persistent://test-property/cl1/ns1/my-topic
```

```
N/A
```

###### REST

```
POST /admin/persistent/{property}/{cluster}/{namespace}/{destination}/subscription/{subName}/skip/{numMessages}
```

###### Java

```java
admin.persistentTopics().skipMessages(persistentTopic, subName, numMessages)
```

#### 全メッセージのスキップ

指定されたトピックの特定のサブスクリプションの全ての古いメッセージをスキップします。

###### CLI

```
$ pulsar-admin persistent skip-all --subscription my-subscription persistent://test-property/cl1/ns1/my-topic
```

```
N/A
```

###### REST

```
POST /admin/persistent/{property}/{cluster}/{namespace}/{destination}/subscription/{subName}/skip_all
```

###### Java

```java
admin.persistentTopics().skipAllMessages(persistentTopic, subName)
```

#### メッセージを有効期限切れにする

指定された有効期限 (秒単位) よりも古い、指定されたトピック上の特定のサブスクリプションのメッセージを有効期限切れにします。

###### CLI

```
$ pulsar-admin persistent expire-messages --subscription my-subscription --expireTime 120 persistent://test-property/cl1/ns1/my-topic
```

```
N/A
```

###### REST

```
POST /admin/persistent/{property}/{cluster}/{namespace}/{destination}/subscription/{subName}/expireMessages/{expireTimeInSeconds}
```

###### Java

```java
admin.persistentTopics().expireMessages(persistentTopic, subName, expireTimeInSeconds)
```

#### 全メッセージを有効期限切れにする

指定された有効期限 (秒単位) よりも古い、トピック上の全てのサブスクリプションのメッセージを有効期限切れにします。

###### CLI

```
$ pulsar-admin persistent expire-messages-all-subscriptions --expireTime 120 persistent://test-property/cl1/ns1/my-topic
```

```
N/A
```

###### REST

```
POST /admin/persistent/{property}/{cluster}/{namespace}/{destination}/all_subscription/expireMessages/{expireTimeInSeconds}
```

###### Java

```java
admin.persistentTopics().expireMessagesForAllSubscriptions(persistentTopic, expireTimeInSeconds)
```



#### カーソルのリセット

サブスクリプションのカーソル位置をX分前に記録された位置まで戻します。
これは、X分前の時間とカーソル位置を計算し、その位置にリセットします。

###### CLI

```
$ pulsar-admin persistent reset-cursor --subscription my-subscription --time 10 persistent://test-property/pstg-gq1/ns1/my-topic
```

```
N/A
```

###### REST

```
POST /admin/persistent/{property}/{cluster}/{namespace}/{destination}/subscription/{subName}/resetcursor/{timestamp}
```

###### Java

```java
admin.persistentTopics().resetCursor(persistentTopic, subName, timestamp)
```


#### トピックのルックアップ

指定されたトピックに対応しているBrokerのurlを探します。

###### CLI

```
$ pulsar-admin persistent lookup persistent://test-property/pstg-gq1/ns1/my-topic
```

```
"pulsar://broker1.org.com:4480"
```

###### REST
```
GET http://<broker-url>:<port>/lookup/v2/destination/persistent/{property}/{cluster}/{namespace}/{dest}
(\* this api serves by “lookup” resource and not “persistent”)
```

###### Java

```java
admin.lookups().lookupDestination(destination)
```

#### サブスクリプションリストの取得

指定されたトピックの全てのサブスクリプション名を表示します。

###### CLI

```
$ pulsar-admin persistent subscriptions persistent://test-property/pstg-gq1/ns1/my-topic
```

```
my-subscription
```

###### REST

```
GET /admin/persistent/{property}/{cluster}/{namespace}/{destination}/subscriptions
```

###### Java

```java
admin.persistentTopics().getSubscriptions(persistentTopic)
```

#### 購読解除

これ以上メッセージを処理しないサブスクリプションを購読解除する際にも役立ちます。

###### CLI

```
$pulsar-admin persistent unsubscribe --subscription my-subscription persistent://test-property/pstg-gq1/ns1/my-topic
```

```
N/A
```

###### REST

```
DELETE /admin/persistent/{property}/{cluster}/{namespace}/{destination}/subscription/{subName}
```

###### Java

```java
admin.persistentTopics().deleteSubscription(persistentTopic, subName)
```

### ネームスペースの隔離ポリシー

#### ネームスペースの隔離ポリシーの作成/更新

ネームスペースの隔離ポリシーを作成します。

  -   auto-failover-policy-params: 自動フェイルオーバーポリシーのパラメータで、カンマ区切りでname=value形式で指定します。

  -   auto-failover-policy-type: 自動フェイルオーバーポリシーのタイプ名です。

  -   namespaces: カンマ区切りのネームスペースの正規表現リストです。

  -   primary: カンマ区切りのプライマリBrokerの正規表現リストです。

  -   secondary: カンマ区切りのセカンダリBrokerの正規表現リストです。

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


#### ネームスペースの隔離ポリシーの取得

ネームスペースの隔離ポリシーを表示します。

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


#### ネームスペースの隔離ポリシーの削除

ネームスペースの隔離ポリシーを削除します。

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


#### ネームスペースの隔離ポリシー全リストの取得

指定されたクラスタによって提供されているネームスペースの隔離ポリシーの全リストを表示します。

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


### リソース割り当て

#### ネームスペースへのリソース割り当ての設定

指定されたネームスペースBundleに対して独自の割り当て情報をセットします。

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

#### ネームスペースへのリソース割り当ての取得

リソース割り当ての情報を表示します。

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

#### ネームスペースへのリソース割り当てのリセット

独自のリソース割り当てをデフォルトの設定に戻します。

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


Pulsarの付加的なツール
---

### Pulsarクライアントツール

Pulsarは任意のトピック上でメッセージのproduceとconsumeを行うためのJavaのAPIを提供しています。
しかし、Pulsarではトピック上でのメッセージのproduceとconsumeに役立つCLIユーティリティも提供しています。

ターミナル上で次のディレクトリに移動して、クライアントツールを試してみてください。

```$ $PULSAR_HOME/bin```

```$ ./pulsar-client --help```

#### メッセージをproduceするコマンド
<table>
  <tbody>
  <tr>
      <td colspan="2">```pulsar-client produce```</td>
    </tr>
    <tr>
      <th>オプション</th>
      <th>説明</th>
    </tr>
    <tr>
      <td>```-f, --files```</td>
      <td>```送信するファイルのパスをコンマ区切りで指定。-mと一緒に使用する事はできません。-fか-mのどちらかは指定しなければなりません```</td>
    </tr>
    <tr>
      <td>```-m, --messages```</td>
      <td>```送信するメッセージの文字列をコンマ区切りで指定。-fと一緒に使用する事はできません。-mか-fのどちらかは指定しなければなりません```</td>
    </tr>
    <tr>
      <td>```-n, --num-produce```</td>
      <td>```メッセージを送信する回数 (デフォルト：1) ```</td>
    </tr>
    <tr>
      <td>```-r, --rate```</td>
      <td>```メッセージをproduceするレート (メッセージ/秒) 。0の場合、メッセージは可能な限り速くproduceされます (デフォルト：0.0) ```</td>
    </tr>
<table>

#### メッセージをconsumeするコマンド
<table>
  <tbody>
  <tr>
      <td colspan="2">```pulsar-client consume```</td>
    </tr>
    <tr>
      <th>オプション</th>
      <th>説明</th>
    </tr>
    <tr>
      <td>```--hex```</td>
      <td>```バイナリメッセージを16進数で表示 (デフォルト：false) ```</td>
    </tr>
    <tr>
      <td>```-n, --num-messages```</td>
      <td>```consumeするメッセージの数 (デフォルト：1) ```</td>
    </tr>
    <tr>
      <td>```-r, --rate```</td>
      <td>```メッセージをconsumeするレート (メッセージ/秒) 。0の場合、メッセージは可能な限り速くconsumeされます (デフォルト：0.0) ```</td>
    </tr>
    <tr>
      <td>```-s, --subscription-name```</td>
      <td>```サブスクリプション名```</td>
    </tr>
    <tr>
      <td>```-t, --subscription-type```</td>
      <td>```Exclusive, Shared, Failoverの内どれか1つのサブスクリプションタイプ (デフォルト：Exclusive) ```</td>
    </tr>
<table>
