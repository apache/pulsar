# Pulsar WebSocket API

<!-- TOC depthFrom:2 depthTo:3 withLinks:1 updateOnSave:1 orderedList:0 -->

- [Running WebSocket service](#running-websocket-service)
- [API Reference](#api-reference)
	- [Producer](#producer)
	- [Consumer](#consumer)
	- [Error codes](#error-codes)
- [Examples](#examples)
	- [Python](#python)
	- [NodeJS](#nodejs)

<!-- /TOC -->

***This API is considered experimental and might change before it's finalized.
We encourage to submit feedback on this WebSocket API.***

Pulsar WebSocket API is meant to provide a simple way to interact with Pulsar
from languages outside the JVM.

Through WebSocket you can publish and consume messages and use all the features
available from the Java client library.

## Running WebSocket service

Apart from the Pulsar standalone that already has the WebSocket service enabled,
there are 2 possible ways to deploy the WebSocket service:

#### Embedded with Pulsar broker

Enable WebSocket in `conf/broker.conf`:

```shell
webSocketServiceEnabled=true
```

This will run the service within the same HTTP server that is already
running in the broker.

#### WebSocket as a separate component

WebSocket service can be run as a separate component on its own.

Configuration is located at `conf/websocket.conf` and the minimal parameters to
change are:

```shell
globalZookeeperServers=...

# Port to use to server HTTP request
webServicePort=8080

# Name of the pulsar cluster to connect to
clusterName=...
```

Then, to start the service:

```shell
$ bin/pulsar-daemon start websocket
```

## API Reference

There are 2 WebSocket endpoint, for publishing and consuming messages and all
exchanges are done through JSON messages.

### Producer

Open a WebSocket session to create a producer for a specific topic:

```perl
http://{serviceUrl}:8080/ws/producer/persistent/{property}/{cluster}/{namespace}/{topic}
```

##### Publishing a message

```json
{
  "payload": "SGVsbG8gV29ybGQ=",
  "properties": {"key1": "value1", "key2": "value2"},
  "context": "1"
}
```

| Key                   |      Type       | Requirement |                         Explanation                         |
|:----------------------|:---------------:|:-----------:|:-----------------------------------------------------------:|
| `payload`             |     String      |  Required   |                   Base-64 encoded payload                   |
| `properties`          | Key-Value pairs |  Optional   |               Application defined properties                |
| `context`             |     String      |  Optional   |           Application defined request identifier            |
| `key`                 |     String      |  Optional   | For partitioned topics, decides the <br /> partition to use |
| `replicationClusters` |      List       |  Optional   |           Restrict replication to these clusters            |


##### Acknowledgement from server

###### Success response
```json
{
   "result": "ok",
   "messageId": "CAAQAw==",
   "context": "1"
 }
```
###### Failure response
```json
 {
   "result": "send-error:3",
   "errorMsg": "Failed to de-serialize from JSON",
   "context": "1"
 }
```

| Key         |  Type  | Requirement |                 Explanation                  |
|:------------|:------:|:-----------:|:--------------------------------------------:|
| `result`    | String |  Required   |     `ok` if successful or error message      |
| `messageId` | String |  Required   | Message Id assigned to the published message |
| `context`   | String |  Optional   |    Application defined request identifier    |


### Consumer

Open a WebSocket session to create a consumer for a specific topic:

```perl
http://{serviceUrl}:8080/ws/consumer/persistent/{property}/{cluster}/{namespace}/{topic}/{subscription}
```

##### Receiving messages

Server will push messages on the WebSocket session:

```json
{
  "messageId": "CAAQAw==",
  "payload": "SGVsbG8gV29ybGQ=",
  "properties": {"key1": "value1", "key2": "value2"},
  "publishTime": "2016-08-30 16:45:57.785",
  "context": "1"
}
```

| Key           |      Type       | Requirement |              Explanation               |
|:--------------|:---------------:|:-----------:|:--------------------------------------:|
| `messageId`   |     String      |  Required   |               Message Id               |
| `payload`     |     String      |  Required   |        Base-64 encoded payload         |
| `properties`  | Key-Value pairs |  Optional   |     Application defined properties     |
| `publishTime` |     String      |  Required   |           Publish timestamp            |
| `context`     |     String      |  Optional   | Application defined request identifier |
| `key`         |     String      |  Optional   |  Original routing key set by producer  |

##### Acknowledging the message

Consumer needs to acknowledge the successful processing of the message to
have the Pulsar broker delete it.

```json
{
  "messageId": "CAAQAw=="
}
```

| Key         |  Type  | Requirement |             Explanation             |
|:------------|:------:|:-----------:|:-----------------------------------:|
| `messageId` | String |  Required   | Message Id of the processed message |


### Error codes

In case of error the server will close the WebSocket session using the
following error codes:

#### Different Possible Error Types

| Error Code | Error Message                    |
|:----------:|:---------------------------------|
|     1      | Failed to create producer        |
|     2      | Failed to subscribe              |
|     3      | Failed to de-serialize from JSON |
|     4      | Failed to serialize to JSON      |
|     5      | Failed to authenticate client    |
|     6      | Client is not authorized         |
|     7      | Invalid payload encoding         |
|     8      | Unknown error			        |

Application is responsible to re-establish a new WebSocket session after
a backoff period.

## Examples

### Python
in this example you need to install `websocket-client` package , you can install it using `pip install websocket-client` or download it from [Pypi page](https://pypi.python.org/pypi/websocket-client) .

#### Python producer

```python
import websocket, base64, json

ws = websocket.create_connection(
   'ws://localhost:8080/ws/producer/persistent/sample/standalone/ns1/my-topic')

# Send one message
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

#### Python consumer

```python
import websocket, base64, json

ws = websocket.create_connection(
   'ws://localhost:8080/ws/consumer/persistent/sample/standalone/ns1/my-topic/my-sub')

while True:
    msg = json.loads(ws.recv())
    if not msg: break

    print 'Received: ', msg, ' - payload:', base64.b64decode(msg['payload'])

    # Acknowledge successful processing
    ws.send(json.dumps({'messageId' : msg['messageId']}))

ws.close()
```

### NodeJS

#### NodeJS producer

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
  // Send one message
  ws.send(JSON.stringify(message));
});

ws.on('message', function(message) {
  console.log('received ack: %s', message);
});

```

#### NodeJS consumer
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
