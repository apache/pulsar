クライアントライブラリを使ってPulsarに接続するには、[Pulsarプロトコル](../../project/BinaryProtocol)URLを指定する必要があります。

PulsarプロトコルURLは特定の{% popover_ja クラスタ %}に割り当てられており、`pulsar`スキームを使ってアクセスされます。ここでデフォルトのポート番号は6650になっています。以下は`localhost`の例です：

```
pulsar://localhost:6650
```

プロダクションのPulsarクラスタのURLは以下のようになります：

```
pulsar://pulsar.us-west.example.com:6650
```

[TLS](../../admin/Authz#tlsクライアント認証)認証を利用している場合、URLは以下のようになります：

```
pulsar+ssl://pulsar.us-west.example.com:6651
```

### グローバルトピックとクラスタ固有のトピック

Pulsarの{% popover_ja トピック %}にはクラスタ固有のものとグローバルなものがあります。クラスタ固有のトピックのURLは以下のような構造をしています：

{% include topic.html p="property" c="cluster" n="namespace" t="topic" %}

クラスタ固有のトピックに対してproduceまたはconsumeを行いたい場合、クライアントは[クラスタのメタデータを初期化](../../deployment/InstanceSetup#クラスタメタデータの初期化)した際に割り当てたBrokerサービスURLを使う必要があります。

一方グローバルトピックの場合、URLは以下のようになります：

{% include topic.html p="property" c="global" n="namespace" t="topic" %}

この場合、{% popover_ja インスタンス %}内の*任意の*クラスタに対して上記のBrokerサービスURLを利用することができ、Pulsarの内部サービスディスカバリシステムが残りの部分を処理します。
