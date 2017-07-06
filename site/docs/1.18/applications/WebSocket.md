---
title: Pulsar's WebSocket API
tags: [websocket, nodejs, python]
---

Pulsar's [WebSocket](https://developer.mozilla.org/en-US/docs/Web/API/WebSockets_API) API is meant to provide a simple way to interact with Pulsar using languages that do not have an official [client library](../../getting-started/Clients). Through WebSockets you can publish and consume messages and use all the features available in the [Java](../JavaClient), [Python](../PythonClient), and [C++](../CppClient) client libraries.

{% include admonition.html type="success" content="You can use Pulsar's WebSocket API with any WebSocket client library. See examples for Python and Node.js [below](#client-examples)." %}

## Running the WebSocket service

The {% popover standalone %} variant of Pulsar that we recommend using for [local development](../../getting-started/LocalCluster) already has the WebSocket service enabled.

In non-standalone mode, there are two ways to deploy the WebSocket service:

* [embedded](#embedded-with-a-pulsar-broker) with a Pulsar {% popover broker %}
* as a [separate component](#as-a-separate-component)

### Embedded with a Pulsar broker

In this mode, the WebSocket service will run within the same HTTP service that's already running in the broker. To enable this mode, set the [`webSocketServiceEnabled`](../../reference/Configuration#broker-webSocketServiceEnabled) parameter in the [`conf/broker.conf`](../../reference/Configuration#broker) configuration file in your installation.

```properties
webSocketServiceEnabled=true
```

### As a separate component

In this mode, the WebSocket service will be run from a Pulsar {% popover broker %} as a separate service. Configuration for this mode is handled in the [`conf/websocket.conf`](../../reference/Configuration#websocket) configuration file. You'll need to set *at least* the following parameters:

* [`globalZookeeperServers`](../../reference/Configuration#websocket-globalZookeeperServers)
* [`webServicePort`](../../reference/Configuration#websocket-webServicePort)
* [`clusterName`](../../reference/Configuration#websocket-clusterName)

Here's an example:

```properties
globalZookeeperServers=zk1:2181,zk2:2181,zk3:2181
webServicePort=8080
clusterName=my-cluster
```

### Starting the broker

When the configuration is set, you can start the service using the [`pulsar-daemon`](../../reference/CliTools#pulsar-daemon) tool:

```shell
$ bin/pulsar-daemon start websocket
```

## API Reference

Pulsar's WebSocket API offers two endpoints, one for [producing](#producer-endpoint) messages and one for [consuming](#consumer-endpoint) messages.

All exchanges via the WebSocket API use JSON.

### Producer endpoint

The producer endpoint requires you to specify a {% popover property %}, {% popover cluster %}, {% popover namespace %}, and {% popover topic %} in the URL:

{% endpoint ws://broker-service-url:8080/ws/producer/persistent/:property/:cluster/:namespace/:topic %}

#### Publishing a message

```json
{
  "payload": "SGVsbG8gV29ybGQ=",
  "properties": {"key1": "value1", "key2": "value2"},
  "context": "1"
}
```

Key | Type | Required? | Explanation
:---|:-----|:----------|:-----------
`payload` | string | yes | Base-64 encoded payload
`properties` | key-value pairs | no | Application-defined properties
`context` | string | no | Application-defined request identifier
`key` | string | no | For partitioned topics, decides which partition to use
`replicationClusters` | array | no | Restrict replication to this list of {% popover clusters %}, specified by name


##### Example success response

```json
{
   "result": "ok",
   "messageId": "CAAQAw==",
   "context": "1"
 }
```
##### Example failure response

```json
 {
   "result": "send-error:3",
   "errorMsg": "Failed to de-serialize from JSON",
   "context": "1"
 }
```

Key | Type | Required? | Explanation
:---|:-----|:----------|:-----------
`result` | string | yes | `ok` if successful or an error message if unsuccessful
`messageId` | string | yes | Message ID assigned to the published message
`context` | string | no | Application-defined request identifier


### Consumer endpoint

The producer endpoint requires you to specify a {% popover property %}, {% popover cluster %}, {% popover namespace %}, and {% popover topic %}, as well as a {% popover subscription %}, in the URL:

{% endpoint ws://broker-service-url:8080/ws/consumer/persistent/:property/:cluster/:namespace/:topic/:subscription %}

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

Key | Type | Required? | Explanation
:---|:-----|:----------|:-----------
`messageId` | string | yes | Message ID
`payload` | string | yes | Base-64 encoded payload
`publishTime` | string | yes | Publish timestamp
`properties` | key-value pairs | no | Application-defined properties
`context` | string | no | Application-defined request identifier
`key` | string | no |  Original routing key set by producer

#### Acknowledging the message

Consumer needs to acknowledge the successful processing of the message to
have the Pulsar broker delete it.

```json
{
  "messageId": "CAAQAw=="
}
```

Key | Type | Required? | Explanation
:---|:-----|:----------|:-----------
`messageId`| string | yes | Message ID of the processed message


### Error codes

In case of error the server will close the WebSocket session using the
following error codes:

Error Code | Error Message
:----------|:-------------
1 | Failed to create producer
2 | Failed to subscribe
3 | Failed to deserialize from JSON
4 | Failed to serialize to JSON
5 | Failed to authenticate client
6 | Client is not authorized
7 | Invalid payload encoding
8 | Unknown error

{% include admonition.html type='warning' content='The application is responsible for re-establishing a new WebSocket session after a backoff period.' %}

## Client examples

Below you'll find code examples for the Pulsar WebSocket API in [Python](#python) and [Node.js](#node.js).

### Python

This example uses the [`websocket-client`](https://pypi.python.org/pypi/websocket-client) package. You can install it using [pip](https://pypi.python.org/pypi/pip):

```shell
$ pip install websocket-client
```

You can also download it from [PyPI](https://pypi.python.org/pypi/websocket-client).

#### Python producer

Here's an example Python {% popover producer %} that sends a simple message to a Pulsar {% popover topic %}:

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

#### Python consumer

Here's an example Python {% popover consumer %} that listens on a Pulsar {% popover topic %} and prints the message ID whenever a message arrives:

```python
import websocket, base64, json

TOPIC = 'ws://localhost:8080/ws/producer/persistent/sample/standalone/ns1/my-topic'

ws = websocket.create_connection(TOPIC)

while True:
    msg = json.loads(ws.recv())
    if not msg: break

    print "Received: {} - payload: {}".format(msg, base64.b64decode(msg['payload'])

    # Acknowledge successful processing
    ws.send(json.dumps({'messageId' : msg['messageId']}))

ws.close()
```

### Node.js

This example uses the [`ws`](https://websockets.github.io/ws/) package. You can install it using [npm](https://www.npmjs.com/):

```shell
$ npm install ws
```

#### Node.js producer

Here's an example Node.js {% popover producer %} that sends a simple message to a Pulsar {% popover topic %}:

```javascript
var WebSocket = require('ws'),
    topic = "ws://localhost:8080/ws/producer/persistent/my-property/us-west/my-ns/my-topic1",
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
  // Send one message
  ws.send(JSON.stringify(message));
});

ws.on('message', function(message) {
  console.log('received ack: %s', message);
});

```

#### NodeJS consumer
```javascript
var WebSocket = require('ws'),
    topic = "ws://localhost:8080/ws/producer/persistent/my-property/us-west/my-ns/my-topic1",
    ws = new WebSocket(topic);

socket.onmessage = function(packet) {
	var receiveMsg = JSON.parse(packet.data);
	var ackMsg = {"messageId" : receiveMsg.messageId};
	socket.send(JSON.stringify(ackMsg));      
};
```