---
title: PulsarにおけるWebSocket API
tags_ja: [websocket, nodejs, python]
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

Pulsarにおける[WebSocket](https://developer.mozilla.org/ja/docs/Web/API/WebSockets_API) APIは公式の[クライアントライブラリ](../../getting-started/Clients)のない言語からもPulsarとメッセージをやり取りする簡単な方法を提供することを意味します。WebSocketを通してメッセージの送受信と[Java](../Java), [Python](../Python), [C++](../Cpp)クライアントライブラリから利用可能な全機能を利用できます。

{% include admonition.html type="success" content="PulsarのWebSocket APIはどのWebSocketクライアントライブラリからでも利用できます。PythonとNode.jsのサンプルは[こちら](#クライアントの実装例)にあります。" %}

## WebSocketサービスの起動

[ローカルでの開発](../../getting-started/LocalCluster)で推奨しているPulsarの{% popover_ja スタンドアローン %}モードではすでにWebSocketサービスは利用可能な状態です。

スタンドアローン以外のモードではWebSocketサービスをデプロイする方法が2つあります:
* Pulsar {% popover_ja Broker %}に[組み込む](#pulsar-brokerに組み込む)
* [独立したコンポーネント](#独立したコンポーネントとして起動)として起動する

### Pulsar Brokerに組み込む

このモードでは、WebSocketサービスはBrokerですでに起動されている同じHTTPサービス内に起動します。このモードを有効にするには、 [`conf/broker.conf`](../../reference/Configuration#broker)ファイルに[`webSocketServiceEnabled`](../../reference/Configuration#broker-webSocketServiceEnabled)パラメータをセットします。

```properties
webSocketServiceEnabled=true
```

### 独立したコンポーネントとして起動

このモードでは、Pulsar {% popover_ja Broker %}とは別サービスとして起動されます。このモードの設定は[`conf/websocket.conf`](../../reference/Configuration#websocket)ファイルでハンドリングされます。*少なくとも*以下のパラメータを設定する必要があります:

* [`configurationStoreServers`](../../reference/Configuration#websocket-configurationStoreServers)
* [`webServicePort`](../../reference/Configuration#websocket-webServicePort)
* [`clusterName`](../../reference/Configuration#websocket-clusterName)

こちらは設定例です:

```properties
configurationStoreServers=zk1:2181,zk2:2181,zk3:2181
webServicePort=8080
clusterName=my-cluster
```

### WebSocketサービスの起動

設定を行なったら、[`pulsar-daemon`](../../reference/CliTools#pulsar-daemon)ツールを利用してサービスを起動できます:

```shell
$ bin/pulsar-daemon start websocket
```

## APIリファレンス

PulsarのWebSocket APIは2つのエンドポイントを提供します。メッセージを[produce](#producerエンドポイント)するためのエンドポイントと[consume](#consumerエンドポイント)するためのエンドポイントです。

WebSocket APIを介した全てのやり取りはJSONで行われます。

### Producerエンドポイント

ProducerエンドポイントはURLで{% popover_ja プロパティ %}、{% popover_ja クラスタ %}、{% popover_ja ネームスペース %}、{% popover_ja トピック %}を指定します:

{% endpoint ws://broker-service-url:8080/ws/producer/persistent/:property/:cluster/:namespace/:topic %}

#### メッセージの送信
```json
{
  "payload": "SGVsbG8gV29ybGQ=",
  "properties": {"key1": "value1", "key2": "value2"},
  "context": "1"
}
```

キー | 型 | 必須? | 説明
:---|:-----|:----------|:-----------
`payload` | string | 必須 | Base-64エンコードされたペイロード
`properties` | key-value pairs | 任意 | アプリケーションによって定義されたプロパティ
`context` | string | 任意 | アプリケーションによって定義されたリクエスト識別子
`key` | string | 任意 | パーティションドトピックの場合、使用するパーティション
`replicationClusters` | array | 任意 | レプリケーションを行う{% popover_ja クラスタ %}をここで指定したものだけに制限


##### 成功時のレスポンス例

```json
{
   "result": "ok",
   "messageId": "CAAQAw==",
   "context": "1"
 }
```
##### 失敗時のレスポンス例

```json
 {
   "result": "send-error:3",
   "errorMsg": "Failed to de-serialize from JSON",
   "context": "1"
 }
```

キー | 型 | 必須? | 説明
:---|:-----|:----------|:-----------
`result` | string | 必須 | 成功時は`ok`、失敗時はエラーメッセージ
`messageId` | string | 必須 | produceされたメッセージに割り当てられたメッセージID
`context` | string | 任意 | アプリケーションによって定義されたリクエスト識別子


### Consumerエンドポイント

Consumerエンドポイントは{% popover_ja プロパティ %}、{% popover_ja クラスタ %}、{% popover_ja ネームスペース %}、{% popover_ja トピック %}と同様に{% popover_ja サブスクリプション %}もURLで指定します:

{% endpoint ws://broker-service-url:8080/ws/consumer/persistent/:property/:cluster/:namespace/:topic/:subscription %}

##### メッセージの受信

WebSocketセッション上でサーバからメッセージがプッシュされます:

```json
{
  "messageId": "CAAQAw==",
  "payload": "SGVsbG8gV29ybGQ=",
  "properties": {"key1": "value1", "key2": "value2"},
  "publishTime": "2016-08-30 16:45:57.785"
}
```

キー | 型 | 必須? | 説明
:---|:-----|:----------|:-----------
`messageId` | string | 必須 | メッセージID
`payload` | string | 必須 | Base-64エンコードされたペイロード
`publishTime` | string | 必須 | メッセージがproduceされた時間のタイムスタンプ
`properties` | key-value pairs | 任意 | アプリケーションによって定義されたプロパティ
`key` | string | 任意 |  Producerによってセットされたオリジナルのルーティングキー

#### Ack (確認応答)

Pulsar Brokerがメッセージを削除できるように、Consumerはメッセージが正常に処理できたというAckを返す必要があります。

```json
{
  "messageId": "CAAQAw=="
}
```

キー | 型 | 必須? | 説明
:---|:-----|:----------|:-----------
`messageId`| string | 必須 | 処理したメッセージのメッセージID


### エラーコード
エラーの場合、サーバは次のエラーコードを利用してWebSocketセッションをクローズします:

エラーコード | エラーメッセージ
:----------|:-------------
1 | Producerの作成に失敗
2 | 購読に失敗
3 | JSONからのデシリアライズに失敗
4 | JSONへのシリアライズに失敗
5 | クライアントの認証に失敗
6 | クライアントが認可されていない
7 | ペイロードの誤ったエンコード
8 | 未知のエラー

{% include admonition.html type='warning' content='アプリケーションは、バックオフ期間後にWebSocketセッションを再確立する責任があります。' %}

## クライアントの実装例

以下は[Python](#python)と[Node.js](#nodejs)のサンプルコードです。

### Python

この例では[`websocket-client`](https://pypi.python.org/pypi/websocket-client)パッケージを利用します。[pip](https://pypi.python.org/pypi/pip)を利用してインストールできます:

```shell
$ pip install websocket-client
```

[PyPI](https://pypi.python.org/pypi/websocket-client)から直接ダウンロードすることもできます。

#### Python Producer

Pulsarの{% popover_ja トピック %}に簡単なメッセージを送信するPython {% popover_ja Producer %}の実装例です。

```python
import websocket, base64, json

TOPIC = 'ws://localhost:8080/ws/producer/persistent/sample/standalone/ns1/my-topic'

ws = websocket.create_connection(TOPIC)

# Send one message as JSON
ws.send(json.dumps({
    'payload' : base64.b64encode('Hello World'),
    'properties': {
        'key1' : 'value1',
        'key2' : 'value2'
    },
    'context' : 5
}))

response =  json.loads(ws.recv())
if response['result'] == 'ok':
    print 'Message published successfully'
else:
    print 'Failed to publish message:', response
ws.close()
```

#### Python Consumer

Pulsar {% popover_ja トピック %}をリッスンしてメッセージを受信するたびにそのメッセージIDを表示するPython {% popover_ja Consumer %}の実装例です:

```python
import websocket, base64, json

TOPIC = 'ws://localhost:8080/ws/consumer/persistent/sample/standalone/ns1/my-topic/my-sub'

ws = websocket.create_connection(TOPIC)

while True:
    msg = json.loads(ws.recv())
    if not msg: break

    print "Received: {} - payload: {}".format(msg, base64.b64decode(msg['payload']))

    # 正常に処理できた事を応答します
    ws.send(json.dumps({'messageId' : msg['messageId']}))

ws.close()
```

### Node.js

この例では [`ws`](https://websockets.github.io/ws/)パッケージを利用します。[npm](https://www.npmjs.com/)を利用してインストールできます:

```shell
$ npm install ws
```

#### Node.js Producer

Pulsarの{% popover_ja トピック %}に簡単なメッセージを送信するNode.jsの実装例です。

```javascript
var WebSocket = require('ws'),
    topic = "ws://localhost:8080/ws/v2/producer/persistent/public/default/my-topic",
    ws = new WebSocket(topic);

var message = {
  "payload" : new Buffer("Hello World").toString('base64'),
  "properties": {
    "key1" : "value1",
    "key2" : "value2"
  },
  "context" : "1"
};

ws.on('open', function() {
  // 1つのメッセージを送信します
  ws.send(JSON.stringify(message));
});

ws.on('message', function(message) {
  console.log('received ack: %s', message);
});
```

#### NodeJS Consumer
```javascript
var WebSocket = require('ws'),
    topic = "ws://localhost:8080/ws/v2/reader/persistent/public/default/my-topic/my-sub",
    ws = new WebSocket(topic);

socket.onmessage = function(packet) {
	var receiveMsg = JSON.parse(packet.data);
	var ackMsg = {"messageId" : receiveMsg.messageId};
	socket.send(JSON.stringify(ackMsg));      
};
```

#### NodeJS reader
```javascript
var WebSocket = require('ws'),
    topic = "ws://localhost:8080/ws/v2/reader/persistent/public/default/my-topic",
    ws = new WebSocket(topic);

ws.on('message', function(message) {
    var receiveMsg = JSON.parse(message);
    console.log('Received: %s - payload: %s', message, new Buffer(receiveMsg.payload, 'base64').toString());
    var ackMsg = {"messageId" : receiveMsg.messageId};
    ws.send(JSON.stringify(ackMsg));
});
```