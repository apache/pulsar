# Pulsar WebSocket API

<!-- TOC depthFrom:2 depthTo:3 withLinks:1 updateOnSave:1 orderedList:0 -->

- [WebSocketサービスの実行](#websocketサービスの実行)
- [APIリファレンス](#apiリファレンス)
	- [Producer](#producer)
	- [Consumer](#consumer)
	- [エラーコード](#エラーコード)
- [実装例](#実装例)
	- [Python](#python)
	- [NodeJS](#nodejs)

<!-- /TOC -->

***このAPIは実験的なものであり、仕様は最終的に確定されるまでに変更が加えられる可能性があります。
私たちはこのWebSocket APIに対するフィードバックを求めています。***

PulsarのWebSocket APIは、PulsarとJVMの外の言語とのやりとりを簡単に行う方法を提供するためのものです。

WebSocketを通してメッセージのproduceとconsumeを行う事ができ、
Javaのクライアントライブラリから利用可能な全ての機能を使用できます。

## WebSocketサービスの実行

既にWebSocketサービスが有効になっているスタンドアローンなPulsarを使用する他に、
WebSocketサービスをデプロイする2つの方法があります。

#### Pulsar Brokerへの組み込み

`conf/broker.conf` でWebSocketを有効化します:

```shell
webSocketServiceEnabled=true
```

これによって、既にBrokerで実行されているHTTPサーバの中でWebSocketサービスも実行されます。

#### 独立したコンポーネントとしてのWebSocket

WebSocketサービスは別のコンポーネントとして単独で動作する事ができます。

設定は `conf/websocket.conf` にあり、変更の必要がある最小限のパラメータは次の通りです:

```shell
globalZookeeperServers=...

# Port to use to server HTTP request
webServicePort=8080

# Name of the pulsar cluster to connect to
clusterName=...
```

そして、WebSocketサービスを実行するには次のようにします:

```shell
$ bin/pulsar-daemon start websocket
```

## APIリファレンス

WebSocketのエンドポイントは、メッセージのproduce用とconsume用の2つが存在しています。
また、全てのやりとりはJSON形式のメッセージを通して行われます。

### Producer

特定のトピックに対するProducerを作成するために、WebSocketのセッションをオープンします。

```perl
http://{serviceUrl}:8080/ws/producer/persistent/{property}/{cluster}/{namespace}/{topic}
```

##### メッセージの送信

```json
{
  "payload": "SGVsbG8gV29ybGQ=",
  "properties": {"key1": "value1", "key2": "value2"},
  "context": "1"
}
```

| キー                  |       型        |    要求     |                             説明                             |
|:----------------------|:---------------:|:-----------:|:------------------------------------------------------------:|
| `payload`             |     String      |    必須     |              Base-64エンコードされたペイロード               |
| `properties`          | Key-Value pairs |    任意     |         アプリケーションによって定義されたプロパティ         |
| `context`             |     String      |    任意     |      アプリケーションによって定義されたリクエスト識別子      |
| `key`                 |     String      |    任意     |    パーティションドトピックの場合、使用するパーティション    |
| `replicationClusters` |      List       |    任意     | レプリケーションを行うクラスタをここで指定したものだけに制限 |


##### サーバからの応答

###### 成功時のレスポンス
```json
{
   "result": "ok",
   "messageId": "CAAQAw==",
   "context": "1"
 }
```
###### 失敗時のレスポンス
```json
 {
   "result": "send-error:3",
   "errorMsg": "Failed to de-serialize from JSON",
   "context": "1"
 }
```

| キー        |   型   |    要求     |                        説明                         |
|:------------|:------:|:-----------:|:---------------------------------------------------:|
| `result`    | String |    必須     |      成功時は `ok` 、失敗時はエラーメッセージ       |
| `messageId` | String |    必須     | produceされたメッセージに割り当てられたメッセージID |
| `context`   | String |    任意     | アプリケーションによって定義されたリクエスト識別子  |


### Consumer

特定のトピックに対するConsumerを作成するために、WebSocketのセッションをオープンします。

```perl
http://{serviceUrl}:8080/ws/consumer/persistent/{property}/{cluster}/{namespace}/{topic}/{subscription}
```

##### メッセージの受信

WebSocketセッション上でサーバからメッセージがプッシュされます:

```json
{
  "messageId": "CAAQAw==",
  "payload": "SGVsbG8gV29ybGQ=",
  "properties": {"key1": "value1", "key2": "value2"},
  "publishTime": "2016-08-30 16:45:57.785",
  "context": "1"
}
```

| キー          |       型        |    要求     |                           説明                           |
|:--------------|:---------------:|:-----------:|:--------------------------------------------------------:|
| `messageId`   |     String      |    必須     |                       メッセージID                       |
| `payload`     |     String      |    必須     |            Base-64エンコードされたペイロード             |
| `properties`  | Key-Value pairs |    任意     |       アプリケーションによって定義されたプロパティ       |
| `publishTime` |     String      |    必須     |            produceされた時間のタイムスタンプ             |
| `context`     |     String      |    任意     |    アプリケーションによって定義されたリクエスト識別子    |
| `key`         |     String      |    任意     | Producerによってセットされたオリジナルのルーティングキー |

##### メッセージへの応答

Pulsar Brokerがメッセージを削除できるように、Consumerはメッセージが正常に処理できたという応答を返す必要があります。

```json
{
  "messageId": "CAAQAw=="
}
```

| キー        |   型   |    要求     |                説明                 |
|:------------|:------:|:-----------:|:-----------------------------------:|
| `messageId` | String |    必須     |  処理したメッセージのメッセージID   |


### エラーコード

エラーの場合、サーバは次のエラーコードを使用してWebSocketセッションをクローズします。

#### 可能性のあるさまざまなエラータイプ

| エラーコード | エラーメッセージ                 |
|:------------:|:---------------------------------|
|      1       | Producerの作成に失敗             |
|      2       | 購読に失敗                       |
|      3       | JSONからのデシリアライズに失敗   |
|      4       | JSONへのシリアライズに失敗       |
|      5       | クライアントの認証に失敗         |
|      6       | クライアントが認可されていない   |
|      7       | ペイロードの誤ったエンコード     |
|      8       | 未知のエラー                     |

アプリケーションは、バックオフ期間後にWebSocketセッションを再確立する責任があります。

## 実装例

### Python
この例では `websocket-client` パッケージをインストールする必要があります。
それは `pip install websocket-client` を使用するか、[Pypi page](https://pypi.python.org/pypi/websocket-client) からダウンロードしてインストールする事ができます。

#### Python Producer

```python
import websocket, base64, json

ws = websocket.create_connection(
   'ws://localhost:8080/ws/producer/persistent/sample/standalone/ns1/my-topic')

# 1つのメッセージを送信します
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

```python
import websocket, base64, json

ws = websocket.create_connection(
   'ws://localhost:8080/ws/consumer/persistent/sample/standalone/ns1/my-topic/my-sub')

while True:
    msg = json.loads(ws.recv())
    if not msg: break

    print 'Received: ', msg, ' - payload:', base64.b64decode(msg['payload'])

    # 正常に処理できた事を応答します
    ws.send(json.dumps({'messageId' : msg['messageId']}))

ws.close()
```

### NodeJS

#### NodeJS Producer

```javascript
var WebSocket = require('ws');
var ws = new WebSocket(
	  "ws://localhost:8080/ws/producer/persistent/my-property/us-west/my-ns/my-topic1");

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
var WebSocket = require('ws');
var socket = new WebSocket(
	"ws://localhost:8080/ws/consumer/persistent/my-property/us-west/my-ns/my-topic1/my-sub1")

socket.onmessage = function(pckt){
	var receiveMsg = JSON.parse(pckt.data);
	var ackMsg = {"messageId" : receiveMsg.messageId}
	socket.send(JSON.stringify(ackMsg));      
};
```
