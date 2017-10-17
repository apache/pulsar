---
title: Pulsarのバイナリプロトコルの仕様
mathjax: true
tags_ja:
- protobuf
- binary protocol
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

Pulsarは{% popover_ja Producer %}/{% popover_ja Consumer %}と{% popover_ja Broker %}の通信に独自バイナリプロトコルを利用しています。プロトコルは転送と実装の最大効率を保証しながら、{% popover_ja Ack %} (確認応答) やフロー制御のような要求される全ての機能をサポートするようにデザインされています。

クライアントとBrokerは互いに*コマンド*を交換します。コマンドは[Protocol Buffers](https://developers.google.com/protocol-buffers/) (*protobuf*) によるバイナリメッセージの形式です。protobufによるコマンドの仕様は[`PulsarApi.proto`](https://github.com/apache/incubator-pulsar/blob/master/pulsar-common/src/main/proto/PulsarApi.proto) に記述されています。また、本ドキュメントの[Protobufインターフェース](#protobufインターフェース)セクションにも記述されています。

{% include admonition.html type='success' title='接続の共有' content="異なるProducerとConsumerのコマンドは同じ接続を介して制限なく送信、インターリーブできます。
" %}

全てのコマンドは[enum](https://developers.google.com/protocol-buffers/docs/proto#enum)[型](#pulsar.proto.Type)と全ての可能なサブコマンドをオプショナルフィールドに含むprotobufのメッセージである[`BaseCommand`](#pulsar.proto.BaseCommand)に埋め込まれています。いかなる時も、1つの`BaseCommand`は1つのサブコマンドしか設定できません。

## フレーミング

protobufはいかなるメッセージのフレーミング機能も提供していないため、protobufのデータの前にフレームのサイズを指定する4バイトのフィールドを追加しています。1つのフレームの最大サイズは5 MBです。

Pulsarのプロトコルには以下の2つのタイプのコマンドがあります:
 1. ペイロードを持たない*シンプルなコマンド*。
 2. メッセージの配送や発行に使われる*ペイロードを持つコマンド*。このケースではprotobufのコマンドデータの後に、protobufの[メタデータ](#メッセージメタデータ)が追従します。ペイロードはprotobufメッセージの後にRaw形式で渡されます。全てのサイズは4バイト符号なしビッグエンディアン整数として渡されます。

{% include admonition.html type='info' content="効率面の理由からペイロードはprotobuf形式ではなくRaw形式で渡されます。" %}

#### シンプルなコマンド

シンプルな (ペイロードのない) コマンドは以下のような構造です:

$$
[totalSize][commandSize][message]
$$

| コンポーネント| 説明                                                                  | サイズ (バイト) |
|:------------|:---------------------------------------------------------------------|:--------------|
| totalSize   | フレームのサイズ (バイト), その後に続く全てのデータを数えます                 | 4             |
| commandSize | シリアライズされたprotobufのコマンドのサイズ                               | 4             |
| message     | Rawバイナリ形式 (protobuf形式ではなく) でシリアライズされたprotobufメッセージ |               |

#### ペイロードを持つコマンド

ペイロードコマンドは以下のような構造です:

$$
[totalSize][commandSize][message][magicNumber][checksum][metadataSize][metadata][payload]
$$

| コンポーネント | 説明                                                                           | サイズ (バイト) |
|:-------------|:------------------------------------------------------------------------------|:--------------|
| totalSize    | フレームのサイズ (バイト), その後に続く全てのデータを数えます                           | 4             |
| commandSize  | シリアライズされたprotobufのコマンドのサイズ                                         | 4            |
| message      | Rawバイナリ形式 (protobuf形式ではなく) でシリアライズされたprotobufメッセージ           |              |
| magicNumber  | (`0x0e01`) 現在のフォーマットを特定する2バイトのマジックナンバー                        | 2            |
| checksum     | この後に続くデータに対する[CRC32-Cチェックサム](http://www.evanjones.ca/crc32c.html) | 4             |
| metadataSize | メッセージ・[メタデータ](#メッセージメタデータ)のサイズ                                | 4             |
| metadata     | protobufによるバイナリメッセージの形式のメッセージ・[メタデータ](#メッセージメタデータ)    |               |
| payload      | フレームの残りの部分はペイロードとみなされます、任意のバイト列を含めることができます         |               |

## メッセージ・メタデータ

メッセージ・メタデータは、アプリケーションに指定されたペイロードとともに、シリアライズされたprotobufメッセージとして保存されます。メタデータは{% popover_ja Producer %}によって作成され、変更されずに{% popover_ja Consumer %}に渡されます。

| フィールド                                | 説明                                                                                                                                                                                                                                               |
|:-------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `producer_name`                      | メッセージを発行した{% popover_ja Producer %}の名前                                                                                                                                                                                         |
| `sequence_id`                        | {% popover_ja Producer %}から割り当てられたメッセージのシーケンスID                                                                                                                                                                                        |
| `publish_time`                       | 発行時のタイムスタンプ (UTCで1970年1月1日からのミリ秒単位の経過時間)                                                                                                                                                    |
| `properties`                         | Key-Valueペアのシーケンス ([`KeyValue`](https://github.com/apache/incubator-pulsar/blob/master/pulsar-common/src/main/proto/PulsarApi.proto#L32)メッセージを使用)。アプリケーションに定義されたKey-ValueでPulsarの動作には影響を与えません。 |
| `replicated_from` *(任意)*       | メッセージがレプリケートされたものかを表し、レプリケートされたものである場合レプリケート元の{% popover_ja クラスタ %}名を示します。                                                                                                             |
| `partition_key` *(任意)*         | パーティションドトピックに発行される場合、キーが存在すればそのハッシュをパーティションの選択に利用します。                                                                                                                          |
| `compression` *(任意)*           | ペイロードが圧縮されているか、どの圧縮ライブラリが使用されているか                                                                                                                                                                              |
| `uncompressed_size` *(任意)*     | 圧縮されている場合、Producerはこのフィールドに元のペイロードサイズを記述する必要があります。                                                                                                                                                 |
| `num_messages_in_batch` *(任意)* | このメッセージが複数のメッセージの[バッチ](#バッチメッセージ)である場合は、含まれているメッセージの数が記述されている必要があります。                                                                                                                   |

### バッチ・メッセージ

バッチ・メッセージを利用する時、ペイロードはエントリのリストを含んでいます。それらは`SingleMessageMetadata`により定義された固有のメタデータを持ちます。


1つのバッチのペイロードは以下の通りです:

$$
[metadataSize1][metadata1][payload1] [metadataSize2][metadata2][payload2] \dots
$$

| フィールド      | 説明                                                          |
|:--------------|:--------------------------------------------------------------|
| metadataSizeN | シリアライズされたprotobuf形式のシングル・メッセージ・メタデータのサイズ |
| metadataN     | シングル・メッセージ・メタデータ                                    |
| payloadN      | アプリケーションから渡されたメッセージペイロード                      |

各メタデータのフィールドは以下のとおりです:

| フィールド              | 説明                                          |
|:----------------------|:---------------------------------------------|
| properties            | アプリケーションが定義したプロパティ               |
| partition key *(任意)* | ハッシュによりパーティションを特定するためのKey     |
| payload_size          | バッチ内の1つのメッセージについてのペイロードのサイズ |

圧縮が有効な場合、バッチ全体が一度に圧縮されます。

## インタラクション

### 接続の確立

BrokerへのTCPコネクションの確立後、クライアントはセッションを開始しなければなりません。通常これには6650番のポートが利用されます。

![接続のインタラクション](/img/Binary%20Protocol%20-%20Connect.png)

Brokerから`Connected`という応答を受け取ると、クライアントは接続準備完了とみなします。もしBrokerがクライアントの認証を検証できなれければ、代わりに`Error`コマンドを返しTCPコネクションをクローズします。

例:

```protobuf
message CommandConnect {
  "client_version" : "Pulsar-Client-Java-v1.15.2",
  "auth_method_name" : "my-authentication-plugin",
  "auth_data" : "my-auth-data",
  "protocol_version" : 6
}
```

フィールド:
 * `client_version` → フォーマットの強制されていないString形式の識別子。
 * `auth_method_name` → *(任意)* 認証が有効な場合は認証プラグインの名前。
 * `auth_data` → *(任意)* プラグイン固有の認証データ。
 * `protocol_version` → クライアントのサポートするプロトコルのバージョン。  
    Brokerは指定されたバージョンより新しいプロトコルのコマンドを送りません。  
    Brokerは最低限のバージョンを要求する可能性があります。

```protobuf
message CommandConnected {
  "server_version" : "Pulsar-Broker-v1.15.2",
  "protocol_version" : 6
}
```

フィールド:
* `server_version` → BrokerのバージョンのString形式の識別子。
* `protocol_version` → Brokerがサポートするプロトコルのバージョン。  
   クライアントは指定されたバージョンより新しいプロトコルのコマンドを送ることができません。

### Keep Alive

クライアントとBrokerの長期のネットワークのパーティションやリモートエンドでのTCPコネクションを終了しないままマシンがクラッシュした場合(例: 停電、カーネルパニック、ハードリブート) を識別するため、リモートピアのアベイラビリティステータスを調べる仕組みが備わっています。

クライアントとBrokerは`Ping`コマンド定期的に送信し、タイムアウト時間内 (Brokerが使用するデフォルトの値は60秒)に`Pong`レスポンスを受け取らなければソケットをクローズします。

Pulsarクライアントの実装において、`Ping`の送信は必須ではありません。しかし、Brokerから強制的にTCPコネクションを終了されないために、`Ping`を受け取った場合は迅速な返答が必要です。


### Producer

メッセージを送るために、クライアントはProducerを作成する必要があります。Producerを作成する時、Brokerは最初にそのクライアントがトピックへの発行を認可されているかを検証します。

クライアントがProducerの作成を完了すると、ネゴシエートされたProducer IDを参照してBrokerにメッセージを発行できます。

![Producerのインタラクション](/img/Binary%20Protocol%20-%20Producer.png)

##### Producerコマンド

```protobuf
message CommandProducer {
  "topic" : "persistent://my-property/my-cluster/my-namespace/my-topic",
  "producer_id" : 1,
  "request_id" : 1
}
```

パラメータ:
 * `topic` → Producerを作成したいトピックの完全な名前
 * `producer_id` → クライアントが生成した同一接続内で一意に定まるProducerの識別子。
 * `request_id` → レスポンスのマッチングに用いる同一接続内で一意に定まるリクエストの識別子。
 * `producer_name` → *(任意)* Producer名が指定されていればそれが利用され、そうでなければ、Brokerが一意に定まる名前を生成します。
    生成されたProducer名はグローバルで一意に定まることが保証されます。  
    Producerが最初に作成された時、Brokerに新しいProducer名を作成させ、再接続後にProducerを再作成する時は再利用するという実装が期待されます。

Brokerは`ProducerSuccess`コマンドか`Error`コマンドを返します。

##### ProducerSuccessコマンド

```protobuf
message CommandProducerSuccess {
  "request_id" :  1,
  "producer_name" : "generated-unique-producer-name"
}
```

パラメータ:
 * `request_id` → `CreateProducer`リクエストのID。
 * `producer_name` → 生成されたグローバルで一意に定まるProducer名、もしくはクライアントにより指定された名前。

##### Sendコマンド

`Send`コマンドは既に存在するProducerのコンテキスト内で新しいメッセージを発行する時に使用されます。このコマンドはコマンドだけでなくペイロードを含むフレーム内で使用されます。ペイロードは[ペイロードを持つコマンド](#ペイロードを持つコマンド)のセクションに記されている完全なフォーマットで記述されます。

```protobuf
message CommandSend {
  "producer_id" : 1,
  "sequence_id" : 0,
  "num_messages" : 1
}
```

パラメータ:
 * `producer_id` → ProducerのID。
 * `sequence_id` → 各メッセージは関連する0からカウントが始まるような実装が期待されるシーケンスのIDを持ちます。    
    メッセージの効果的な発行を承認する`SendReceipt`は、シーケンスIDによってメッセージを参照します。
 * `num_messages` → *(任意)* バッチ・メッセージが発行される時に使用されます。

##### SendReceiptコマンド

メッセージが設定されたレプリカの数に応じて永続化されたあと、BrokerはProducerにメッセージを受け取ったことを示すAckを返します。


```protobuf
message CommandSendReceipt {
  "producer_id" : 1,
  "sequence_id" : 0,
  "message_id" : {
    "ledgerId" : 123,
    "entryId" : 456
  }
}
```

パラメータ:
 * `producer_id` → ProducerのID。
 * `sequence_id` → 発行されたメッセージのシーケンスのID。
 * `message_id` → システムに割り当てられた1つのクラスタ内で一意に定まるメッセージのID。  
    メッセージIDは`ledgerId`と`entryId`の2つのlong値から構成されます。  
		これらの値はBookKeeperのLedgerに追加された時に割り振られたIDに基づいています。


##### CloseProducerコマンド

**注**: *このコマンドはProducerとBrokerの両方から送信される可能性があります*。

Brokerが`CloseProducer`コマンドを受け取った時、BrokerはProducerからそれ以上のメッセージの受信を停止し、ペンディング中の全てのメッセージが永続化されるまで待ち、クライアントに`Success`を返します。

Brokerは、正常なフェイルオーバー (例: Brokerの再起動中、またはトピックが別のBrokerに転送されるようにロードバランサによってアンロードされている場合など) を実行している時、クライアントに`CloseProducer`コマンドを送ることができます。

Producerが`CloseProducer`コマンドを受け取った時、クライアントはサービスディスカバリ・ルックアップを通じてProducerを再作成することが期待されます。この際TCPコネクションは影響を受けません。

### Consumer

Consumerはサブスクリプションへの接続とそこからのメッセージのconsumeに利用されます。接続後、クライアントはトピックを購読する必要があります。もしサブスクリプションがそのトピックになければ、新しく作成されます。

![Consumer](/img/Binary%20Protocol%20-%20Consumer.png)

#### フロー制御

Consumerの準備が整ったあと、クライアントはBrokerにメッセージをプッシュするための*パーミッションを与える*必要があります。これは`Flow`コマンドによって成されます。

`Flow`コマンドは追加の*パーミッション*を与えます。一般的なConsumerの実装では、アプリケーションがメッセージをconsumeする準備が整うまでのメッセージの蓄積にキューを利用します。

アプリケーションがメッセージをデキューしたあと、ConsumerはBrokerに対してさらなるメッセージをプッシュするパーミッションを送ります。

##### Subscribeコマンド

```protobuf
message CommandSubscribe {
  "topic" : "persistent://my-property/my-cluster/my-namespace/my-topic",
  "subscription" : "my-subscription-name",
  "subType" : "Exclusive",
  "consumer_id" : 1,
  "request_id" : 1
}
```

パラメータ:
 * `topic` → Consumerを作成したいトピックの完全な名前。
 * `subscription` → サブスクリプション名。
 * `subType` → サブスクリプションタイプ: Exclusive, Shared, Failover
 * `consumer_id` → クライアントが生成した同一接続内で一意に定まるConsumerの識別子。
 * `request_id` → レスポンスのマッチングに用いる同一接続内で一意に定まるリクエストの識別子。
 * `consumer_name` → *(任意)* クライアントはConsumer名を指定できます。  
    この名前は、統計情報で特定のConsumerを追跡するのに利用されます。  
    また、サブスクリプションタイプがFailoverの時、この名前はどのConsumerが *master* (メッセージを受け取るConsumer) となるかを決めるのに使用されます。  
    ConsumerはConsumer名によってソートされ、最初のものがmasterとして選ばれます。

##### Flowコマンド

```protobuf
message CommandFlow {
  "consumer_id" : 1,
  "messagePermits" : 1000
}
```

パラメータ:
 * `consumer_id` → ConsumerのID。
 * `messagePermits` → Brokerに対して追加でプッシュを許可するメッセージの数。

##### Messageコマンド

`Message`コマンドはBrokerが、与えられたパーミッションの制限内でConsumerにメッセージをプッシュする際に使用されます。

このコマンドはコマンドだけでなくペイロードを含むフレーム内で使用されます。ペイロードは[ペイロードを持つコマンド](#ペイロードを持つコマンド)のセクションに記されている完全なフォーマットで記述されます。

```protobuf
message CommandMessage {
  "consumer_id" : 1,
  "message_id" : {
    "ledgerId" : 123,
    "entryId" : 456
  }
}
```


#### Ackコマンド

`Ack`コマンドは与えられたメッセージがアプリケーションによって正しく処理され、Brokerによる破棄が可能であるというBrokerへの信号です。

加えて、Brokerは`Ack`の返されたメッセージに基いてConsumerの購読位置を管理します。

```protobuf
message CommandAck {
  "consumer_id" : 1,
  "ack_type" : "Individual",
  "message_id" : {
    "ledgerId" : 123,
    "entryId" : 456
  }
}
```

パラメータ:
 * `consumer_id` → ConsumerのID。
 * `ack_type` → Ackのタイプ: `Individual` もしくは `Cumulative`
 * `message_id` → メッセージのID。
 * `validation_error` → *(任意)* Consumerが次の理由のためメッセージを破棄したことを示します: `UncompressedSizeCorruption`,
   `DecompressionError`, `ChecksumMismatch`, `BatchDeSerializeError`

##### CloseConsumerコマンド

***注***: *このコマンドはConsumerとBrokerの両方から送信される可能性があります*。

このコマンドは[`CloseProducer`](#closeproducerコマンド)と同様の振る舞いをします。

##### RedeliverUnacknowledgedMessagesコマンド

ConsumerはBrokerに、特定のConsumerにプッシュしたがまだAckが返っていないメッセージの再配送を要求できます。

そのprotobufメッセージはConsumerが再配送してほしいメッセージのIDのリストから構成されます。リストが空の場合は、Brokerはペンディング中の全てのメッセージを再送します。

再配送において、メッセージは同一のConsumer、あるいはサブスクリプションタイプがSharedの場合は全ての利用可能なConsumerに送信されます。

##### ReachedEndOfTopicコマンド

トピックが"終了"しサブスクリプション上の全てのメッセージにAckが返された際に、そのConsumerに対してBrokerから送信されます。　

クライアントはアプリケーションにこれ以上Consumerからメッセージが来ないことを通知するためにこのコマンドを使用します。

## サービスディスカバリ

### トピックのルックアップ

トピックのルックアップはクライアントがProducer, Consumerを作成、再接続する度に必要となります。ルックアップはどのBrokerが、利用しようとしているトピックを提供しているかを見つけるのに使用されます。

ルックアップは[admin API](../../admin/AdminInterface#トピックのルックアップ)で
説明されているREST APIのコールで行うことが可能です。

Pulsar-1.16からは、バイナリプロトコルで行うことも可能です。

例として、サービスディスカバリコンポーネントを`pulsar://broker.example.com:6650`で起動していると仮定します。

個別のBrokerは`pulsar://broker-1.example.com:6650`,
`pulsar://broker-2.example.com:6650`, ...で起動しています。

クライアントはサービスディスカバリへの接続を`LookupTopic`コマンドを送るために利用できます。レスポンスは接続すべきBrokerのホスト名、あるいはルックアップをリトライするためのBrokerのホスト名のどちらかです。

`LookupTopic`コマンドは`Connect` / `Connected`の最初のハンドシェイクを終えた接続で使用されなければなりません。

![トピックのルックアップ](/img/Binary%20Protocol%20-%20Topic%20lookup.png)

```protobuf
message CommandLookupTopic {
  "topic" : "persistent://my-property/my-cluster/my-namespace/my-topic",
  "request_id" : 1,
  "authoritative" : false
}
```

フィールド:
 * `topic` → ルックアップするトピックの名前。
 * `request_id` → レスポンスとともに受け取るリクエストのID。
 * `authoritative` → 最初のルックアップリクエストではfalse、その後に続くリダイレクトのレスポンスではクライアントはレスポンスに含まれているものと同じ値を渡すべきです。

##### LookupTopicResponse

成功時のレスポンス例:

```protobuf
message CommandLookupTopicResponse {
  "request_id" : 1,
  "response" : "Connect",
  "brokerServiceUrl" : "pulsar://broker-1.example.com:6650",
  "brokerServiceUrlTls" : "pulsar+ssl://broker-1.example.com:6651",
  "authoritative" : true
}
```

リダイレクト時のレスポンス例:

```protobuf
message CommandLookupTopicResponse {
  "request_id" : 1,
  "response" : "Redirect",
  "brokerServiceUrl" : "pulsar://broker-2.example.com:6650",
  "brokerServiceUrlTls" : "pulsar+ssl://broker-2.example.com:6651",
  "authoritative" : true
}
```

後者の場合、`LookupTopic`コマンドリクエストを`broker-2.example.com` に対して再発行する必要があります。このBrokerはルックアップリクエストに対して決定的なレスポンスを返すことができます。

### パーティションドトピックのディスカバリ

パーティションドトピックのメタデータのディスカバリはトピックがパーティションドトピックかどうか、いくつのパーティションがセットアップされているかを調べるのに利用されます。

トピックがパーティションドである場合、クライアントは`partition-X`というサフィックスを用いて各パーティションにつき一つのProducerあるいはConsumerを作成することが期待されます。

この情報は最初にProducerあるいはConsumerが作成される時のみに利用されます。再接続後は必要ありません。

パーティションドトピック・メタデータのディスカバリはトピックのルックアップと非常によく似た働きをします。クライアントはサービスディスカバリに対してリクエストを送り、レスポンスには実際のメタデータが含まれます。

##### PartitionedTopicMetadataコマンド

```protobuf
message CommandPartitionedTopicMetadata {
  "topic" : "persistent://my-property/my-cluster/my-namespace/my-topic",
  "request_id" : 1
}
```

フィールド:
 * `topic` → パーティションのメタデータを確認するトピック。
 * `request_id` → レスポンスに渡されるリクエストのID。


##### PartitionedTopicMetadataResponseコマンド

メタデータ付きのレスポンスの例:

```protobuf
message CommandPartitionedTopicMetadataResponse {
  "request_id" : 1,
  "response" : "Success",
  "partitions" : 32
}
```

## Protobufインターフェース

{% include protobuf.html %}
