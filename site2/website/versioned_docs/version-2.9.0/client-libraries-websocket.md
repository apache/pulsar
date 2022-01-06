---
id: version-2.9.0-client-libraries-websocket
title: Pulsar WebSocket API
sidebar_label: WebSocket
original_id: client-libraries-websocket
---

Pulsar [WebSocket](https://developer.mozilla.org/en-US/docs/Web/API/WebSockets_API) API provides a simple way to interact with Pulsar using languages that do not have an official [client library](getting-started-clients.md). Through WebSocket, you can publish and consume messages and use features available on the [Client Features Matrix](https://github.com/apache/pulsar/wiki/Client-Features-Matrix) page.


> You can use Pulsar WebSocket API with any WebSocket client library. See examples for Python and Node.js [below](#client-examples).

## Running the WebSocket service

The standalone variant of Pulsar that we recommend using for [local development](getting-started-standalone.md) already has the WebSocket service enabled.

In non-standalone mode, there are two ways to deploy the WebSocket service:

* [embedded](#embedded-with-a-pulsar-broker) with a Pulsar broker
* as a [separate component](#as-a-separate-component)

### Embedded with a Pulsar broker

In this mode, the WebSocket service will run within the same HTTP service that's already running in the broker. To enable this mode, set the [`webSocketServiceEnabled`](reference-configuration.md#broker-webSocketServiceEnabled) parameter in the [`conf/broker.conf`](reference-configuration.md#broker) configuration file in your installation.

```properties
webSocketServiceEnabled=true
```

### As a separate component

In this mode, the WebSocket service will be run from a Pulsar [broker](reference-terminology.md#broker) as a separate service. Configuration for this mode is handled in the [`conf/websocket.conf`](reference-configuration.md#websocket) configuration file. You'll need to set *at least* the following parameters:

* [`configurationStoreServers`](reference-configuration.md#websocket-configurationStoreServers)
* [`webServicePort`](reference-configuration.md#websocket-webServicePort)
* [`clusterName`](reference-configuration.md#websocket-clusterName)

Here's an example:

```properties
configurationStoreServers=zk1:2181,zk2:2181,zk3:2181
webServicePort=8080
clusterName=my-cluster
```

### Security settings

To enable TLS encryption on WebSocket service:
```properties
tlsEnabled=true
tlsAllowInsecureConnection=false
tlsCertificateFilePath=/path/to/client-websocket.cert.pem
tlsKeyFilePath=/path/to/client-websocket.key-pk8.pem
tlsTrustCertsFilePath=/path/to/ca.cert.pem
```

### Starting the broker

When the configuration is set, you can start the service using the [`pulsar-daemon`](reference-cli-tools.md#pulsar-daemon) tool:

```shell
$ bin/pulsar-daemon start websocket
```

## API Reference

Pulsar's WebSocket API offers three endpoints for [producing](#producer-endpoint) messages, [consuming](#consumer-endpoint) messages and [reading](#reader-endpoint) messages.

All exchanges via the WebSocket API use JSON.

### Authentication

#### Browser javascript WebSocket client

Use the query param `token` transport the authentication token.

```http
ws://broker-service-url:8080/path?token=token
```

### Producer endpoint

The producer endpoint requires you to specify a tenant, namespace, and topic in the URL:

```http
ws://broker-service-url:8080/ws/v2/producer/persistent/:tenant/:namespace/:topic 
```

##### Query param

Key | Type | Required? | Explanation
:---|:-----|:----------|:-----------
`sendTimeoutMillis` | long | no | Send timeout (default: 30 secs)
`batchingEnabled` | boolean | no | Enable batching of messages (default: false)
`batchingMaxMessages` | int | no | Maximum number of messages permitted in a batch (default: 1000)
`maxPendingMessages` | int | no | Set the max size of the internal-queue holding the messages (default: 1000)
`batchingMaxPublishDelay` | long | no | Time period within which the messages will be batched (default: 10ms)
`messageRoutingMode` | string | no | Message [routing mode](https://pulsar.apache.org/api/client/index.html?org/apache/pulsar/client/api/ProducerConfiguration.MessageRoutingMode.html) for the partitioned producer: `SinglePartition`, `RoundRobinPartition`
`compressionType` | string | no | Compression [type](https://pulsar.apache.org/api/client/index.html?org/apache/pulsar/client/api/CompressionType.html): `LZ4`, `ZLIB`
`producerName` | string | no | Specify the name for the producer. Pulsar will enforce only one producer with same name can be publishing on a topic
`initialSequenceId` | long | no | Set the baseline for the sequence ids for messages published by the producer.
`hashingScheme` | string | no | [Hashing function](http://pulsar.apache.org/api/client/org/apache/pulsar/client/api/ProducerConfiguration.HashingScheme.html) to use when publishing on a partitioned topic: `JavaStringHash`, `Murmur3_32Hash`
`token` | string | no | Authentication token, this is used for the browser javascript client


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
`replicationClusters` | array | no | Restrict replication to this list of [clusters](reference-terminology.md#cluster), specified by name


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

The consumer endpoint requires you to specify a tenant, namespace, and topic, as well as a subscription, in the URL:

```http
ws://broker-service-url:8080/ws/v2/consumer/persistent/:tenant/:namespace/:topic/:subscription
```

##### Query param

Key | Type | Required? | Explanation
:---|:-----|:----------|:-----------
`ackTimeoutMillis` | long | no | Set the timeout for unacked messages (default: 0)
`subscriptionType` | string | no | [Subscription type](https://pulsar.apache.org/api/client/index.html?org/apache/pulsar/client/api/SubscriptionType.html): `Exclusive`, `Failover`, `Shared`, `Key_Shared`
`receiverQueueSize` | int | no | Size of the consumer receive queue (default: 1000)
`consumerName` | string | no | Consumer name
`priorityLevel` | int | no | Define a [priority](http://pulsar.apache.org/api/client/org/apache/pulsar/client/api/ConsumerConfiguration.html#setPriorityLevel-int-) for the consumer
`maxRedeliverCount` | int | no | Define a [maxRedeliverCount](http://pulsar.apache.org/api/client/org/apache/pulsar/client/api/ConsumerBuilder.html#deadLetterPolicy-org.apache.pulsar.client.api.DeadLetterPolicy-) for the consumer (default: 0). Activates [Dead Letter Topic](https://github.com/apache/pulsar/wiki/PIP-22%3A-Pulsar-Dead-Letter-Topic) feature.
`deadLetterTopic` | string | no | Define a [deadLetterTopic](http://pulsar.apache.org/api/client/org/apache/pulsar/client/api/ConsumerBuilder.html#deadLetterPolicy-org.apache.pulsar.client.api.DeadLetterPolicy-) for the consumer (default: {topic}-{subscription}-DLQ). Activates [Dead Letter Topic](https://github.com/apache/pulsar/wiki/PIP-22%3A-Pulsar-Dead-Letter-Topic) feature.
`pullMode` | boolean | no | Enable pull mode (default: false). See "Flow Control" below.
`negativeAckRedeliveryDelay` | int | no | When a message is negatively acknowledged, the delay time before the message is redelivered (in milliseconds). The default value is 60000.
`token` | string | no | Authentication token, this is used for the browser javascript client

NB: these parameter (except `pullMode`) apply to the internal consumer of the WebSocket service.
So messages will be subject to the redelivery settings as soon as the get into the receive queue,
even if the client doesn't consume on the WebSocket.

##### Receiving messages

Server will push messages on the WebSocket session:

```json
{
"messageId": "CAMQADAA",
  "payload": "hvXcJvHW7kOSrUn17P2q71RA5SdiXwZBqw==",
  "properties": {},
  "publishTime": "2021-10-29T16:01:38.967-07:00",
  "redeliveryCount": 0,
  "encryptionContext": {
    "keys": {
      "client-rsa.pem": {
        "keyValue": "jEuwS+PeUzmCo7IfLNxqoj4h7txbLjCQjkwpaw5AWJfZ2xoIdMkOuWDkOsqgFmWwxiecakS6GOZHs94x3sxzKHQx9Oe1jpwBg2e7L4fd26pp+WmAiLm/ArZJo6JotTeFSvKO3u/yQtGTZojDDQxiqFOQ1ZbMdtMZA8DpSMuq+Zx7PqLo43UdW1+krjQfE5WD+y+qE3LJQfwyVDnXxoRtqWLpVsAROlN2LxaMbaftv5HckoejJoB4xpf/dPOUqhnRstwQHf6klKT5iNhjsY4usACt78uILT0pEPd14h8wEBidBz/vAlC/zVMEqiDVzgNS7dqEYS4iHbf7cnWVCn3Hxw==",
        "metadata": {}
      }
    },
    "param": "Tfu1PxVm6S9D3+Hk",
    "compressionType": "NONE",
    "uncompressedMessageSize": 0,
    "batchSize": {
      "empty": false,
      "present": true
    }
  }
}
```

Below are the parameters in the WebSocket consumer response.

- General parameters

    Key | Type | Required? | Explanation
    :---|:-----|:----------|:-----------
    `messageId` | string | yes | Message ID
    `payload` | string | yes | Base-64 encoded payload
    `publishTime` | string | yes | Publish timestamp
    `redeliveryCount` | number | yes | Number of times this message was already delivered
    `properties` | key-value pairs | no | Application-defined properties
    `key` | string | no |  Original routing key set by producer
    `encryptionContext` | EncryptionContext | no | Encryption context that consumers can use to decrypt received messages
    `param` | string | no | Initialization vector for cipher (Base64 encoding)
    `batchSize` | string | no | Number of entries in a message (if it is a batch message)
    `uncompressedMessageSize` | string | no | Message size before compression
    `compressionType` | string | no | Algorithm used to compress the message payload

- `encryptionContext` related parameter

    Key | Type | Required? | Explanation
    :---|:-----|:----------|:-----------
    `keys` |key-EncryptionKey pairs | yes | Key in `key-EncryptionKey` pairs is an encryption key name. Value in `key-EncryptionKey` pairs is an encryption key object.

- `encryptionKey` related parameters

    Key | Type | Required? | Explanation
    :---|:-----|:----------|:-----------
    `keyValue` | string | yes | Encryption key (Base64 encoding)
    `metadata` | key-value pairs | no | Application-defined metadata

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

#### Negatively acknowledging messages
```json
{
  "type": "negativeAcknowledge",
  "messageId": "CAAQAw=="
}
```

Key | Type | Required? | Explanation
:---|:-----|:----------|:-----------
`messageId`| string | yes | Message ID of the processed message

#### Flow control

##### Push Mode

By default (`pullMode=false`), the consumer endpoint will use the `receiverQueueSize` parameter both to size its
internal receive queue and to limit the number of unacknowledged messages that are passed to the WebSocket client.
In this mode, if you don't send acknowledgements, the Pulsar WebSocket service will stop sending messages after reaching
`receiverQueueSize` unacked messages sent to the WebSocket client.

##### Pull Mode

If you set `pullMode` to `true`, the WebSocket client will need to send `permit` commands to permit the
Pulsar WebSocket service to send more messages.

```json
{
  "type": "permit",
  "permitMessages": 100
}
```

Key | Type | Required? | Explanation
:---|:-----|:----------|:-----------
`type`| string | yes | Type of command. Must be `permit`
`permitMessages`| int | yes | Number of messages to permit

NB: in this mode it's possible to acknowledge messages in a different connection.

#### Check if reach end of topic

Consumer can check if it has reached end of topic by sending `isEndOfTopic` request.

**Request**
```json
{
  "type": "isEndOfTopic"
}
```

Key | Type | Required? | Explanation
:---|:-----|:----------|:-----------
`type`| string | yes | Type of command. Must be `isEndOfTopic`

**Response**
```json
{
   "endOfTopic": "true/false"
 }
```

### Reader endpoint

The reader endpoint requires you to specify a tenant, namespace, and topic in the URL:

```http
ws://broker-service-url:8080/ws/v2/reader/persistent/:tenant/:namespace/:topic
```

##### Query param

Key | Type | Required? | Explanation
:---|:-----|:----------|:-----------
`readerName` | string | no | Reader name
`receiverQueueSize` | int | no | Size of the consumer receive queue (default: 1000)
`messageId` | int or enum | no | Message ID to start from, `earliest` or `latest` (default: `latest`)
`token` | string | no | Authentication token, this is used for the browser javascript client

##### Receiving messages

Server will push messages on the WebSocket session:

```json
{
  "messageId": "CAAQAw==",
  "payload": "SGVsbG8gV29ybGQ=",
  "properties": {"key1": "value1", "key2": "value2"},
  "publishTime": "2016-08-30 16:45:57.785",
  "redeliveryCount": 4
}
```

Key | Type | Required? | Explanation
:---|:-----|:----------|:-----------
`messageId` | string | yes | Message ID
`payload` | string | yes | Base-64 encoded payload
`publishTime` | string | yes | Publish timestamp
`redeliveryCount` | number | yes | Number of times this message was already delivered
`properties` | key-value pairs | no | Application-defined properties
`key` | string | no |  Original routing key set by producer

#### Acknowledging the message

**In WebSocket**, Reader needs to acknowledge the successful processing of the message to
have the Pulsar WebSocket service update the number of pending messages.
If you don't send acknowledgements, Pulsar WebSocket service will stop sending messages after reaching the pendingMessages limit.

```json
{
  "messageId": "CAAQAw=="
}
```

Key | Type | Required? | Explanation
:---|:-----|:----------|:-----------
`messageId`| string | yes | Message ID of the processed message

#### Check if reach end of topic

Consumer can check if it has reached end of topic by sending `isEndOfTopic` request.

**Request**
```json
{
  "type": "isEndOfTopic"
}
```

Key | Type | Required? | Explanation
:---|:-----|:----------|:-----------
`type`| string | yes | Type of command. Must be `isEndOfTopic`

**Response**
```json
{
   "endOfTopic": "true/false"
 }
```



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

> The application is responsible for re-establishing a new WebSocket session after a backoff period.

## Client examples

Below you'll find code examples for the Pulsar WebSocket API in [Python](#python) and [Node.js](#nodejs).

### Python

This example uses the [`websocket-client`](https://pypi.python.org/pypi/websocket-client) package. You can install it using [pip](https://pypi.python.org/pypi/pip):

```shell
$ pip install websocket-client
```

You can also download it from [PyPI](https://pypi.python.org/pypi/websocket-client).

#### Python producer

Here's an example Python producer that sends a simple message to a Pulsar [topic](reference-terminology.md#topic):

```python
import websocket, base64, json

# If set enableTLS to true, your have to set tlsEnabled to true in conf/websocket.conf.
enable_TLS = False
scheme = 'ws'
if enable_TLS:
    scheme = 'wss'

TOPIC = scheme + '://localhost:8080/ws/v2/producer/persistent/public/default/my-topic'

ws = websocket.create_connection(TOPIC)

# encode message
s = "Hello World"
firstEncoded = s.encode("UTF-8")
binaryEncoded = base64.b64encode(firstEncoded)
payloadString = binaryEncoded.decode('UTF-8')

# Send one message as JSON
ws.send(json.dumps({
    'payload' : payloadString,
    'properties': {
        'key1' : 'value1',
        'key2' : 'value2'
    },
    'context' : 5
}))

response =  json.loads(ws.recv())
if response['result'] == 'ok':
    print( 'Message published successfully')
else:
    print('Failed to publish message:', response)
ws.close()
```

#### Python consumer

Here's an example Python consumer that listens on a Pulsar topic and prints the message ID whenever a message arrives:

```python
import websocket, base64, json

# If set enableTLS to true, your have to set tlsEnabled to true in conf/websocket.conf.
enable_TLS = False
scheme = 'ws'
if enable_TLS:
    scheme = 'wss'

TOPIC = scheme + '://localhost:8080/ws/v2/consumer/persistent/public/default/my-topic/my-sub'

ws = websocket.create_connection(TOPIC)

while True:
    msg = json.loads(ws.recv())
    if not msg: break

    print( "Received: {} - payload: {}".format(msg, base64.b64decode(msg['payload'])))

    # Acknowledge successful processing
    ws.send(json.dumps({'messageId' : msg['messageId']}))

ws.close()
```

#### Python reader

Here's an example Python reader that listens on a Pulsar topic and prints the message ID whenever a message arrives:

```python
import websocket, base64, json

# If set enableTLS to true, your have to set tlsEnabled to true in conf/websocket.conf.
enable_TLS = False
scheme = 'ws'
if enable_TLS:
    scheme = 'wss'

TOPIC = scheme + '://localhost:8080/ws/v2/reader/persistent/public/default/my-topic'
ws = websocket.create_connection(TOPIC)

while True:
    msg = json.loads(ws.recv())
    if not msg: break

    print ( "Received: {} - payload: {}".format(msg, base64.b64decode(msg['payload'])))

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

Here's an example Node.js producer that sends a simple message to a Pulsar topic:

```javascript
const WebSocket = require('ws');

// If set enableTLS to true, your have to set tlsEnabled to true in conf/websocket.conf.
const enableTLS = false;
const topic = `${enableTLS ? 'wss' : 'ws'}://localhost:8080/ws/v2/producer/persistent/public/default/my-topic`;
const ws = new WebSocket(topic);

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

#### Node.js consumer

Here's an example Node.js consumer that listens on the same topic used by the producer above:

```javascript
const WebSocket = require('ws');

// If set enableTLS to true, your have to set tlsEnabled to true in conf/websocket.conf.
const enableTLS = false;
const topic = `${enableTLS ? 'wss' : 'ws'}://localhost:8080/ws/v2/consumer/persistent/public/default/my-topic/my-sub`;
const ws = new WebSocket(topic);

ws.on('message', function(message) {
    var receiveMsg = JSON.parse(message);
    console.log('Received: %s - payload: %s', message, new Buffer(receiveMsg.payload, 'base64').toString());
    var ackMsg = {"messageId" : receiveMsg.messageId};
    ws.send(JSON.stringify(ackMsg));
});
```

#### NodeJS reader
```javascript
const WebSocket = require('ws');

// If set enableTLS to true, your have to set tlsEnabled to true in conf/websocket.conf.
const enableTLS = false;
const topic = `${enableTLS ? 'wss' : 'ws'}://localhost:8080/ws/v2/reader/persistent/public/default/my-topic`;
const ws = new WebSocket(topic);

ws.on('message', function(message) {
    var receiveMsg = JSON.parse(message);
    console.log('Received: %s - payload: %s', message, new Buffer(receiveMsg.payload, 'base64').toString());
    var ackMsg = {"messageId" : receiveMsg.messageId};
    ws.send(JSON.stringify(ackMsg));
});
```
