---
id: client-libraries-node
title: The Pulsar Node.js client
sidebar_label: Node.js
---

The Pulsar Node.js client can be used to create Pulsar [producers](#producers), and and [consumers](#consumers) in Node.js.

## Installation

You can install the [`pusar-client`](https://www.npmjs.com/package/pulsar-client) library via [npm](https://www.npmjs.com/).

### Requirements
Pulsar Node.js client library is based on the C++ client library.
Follow [these instructions](client-libraries-cpp.md#compilation) and install the Pulsar C++ client library.

### Installation using npm

Install the `pulsar-client` library via [npm](https://www.npmjs.com/):

```shell
$ npm install pulsar-client@^{{pulsar:version_number}}
```

> #### Note
> 
> Also, this library works only in Node.js 10.x or later because it uses the [`node-addon-api`](https://github.com/nodejs/node-addon-api) module to wrap the C++ library.

## Connection URLs
To connect to Pulsar using client libraries, you need to specify a [Pulsar protocol](developing-binary-protocol.md) URL.

Pulsar protocol URLs are assigned to specific clusters, use the `pulsar` scheme and have a default port of 6650. Here's an example for `localhost`:

```http
pulsar://localhost:6650
```

A URL for a production Pulsar cluster may look something like this:

```http
pulsar://pulsar.us-west.example.com:6650
```

If you're using [TLS](security-tls-authentication.md) authentication, the URL will look like something like this:

```http
pulsar+ssl://pulsar.us-west.example.com:6651
```

## Creating a client

```JavaScript
const Pulsar = require('pulsar-client');

(async () => {
  const client = new Pulsar.Client({
    serviceUrl: 'pulsar://localhost:6650',
  });
  
  await client.close();
})();
```

The following configurable parameters are available for Pulsar clients:

| Parameter | Description | Default |
| :-------- | :---------- | :------ |
| `serviceUrl` | The connection URL for the Pulsar cluster. See [above](#connection-urls) for more info | |
| `authentication` | | |
| `binding` | | |
| `operationTimeoutSeconds` | | |
| `ioThreads` | | |
| `messageListenerThreads` | | |
| `concurrentLookupRequest` | | |
| `useTls` | | |
| `tlsTrustCertsFilePath` | | |
| `tlsValidateHostname` | | |
| `tlsAllowInsecureConnection` | | |
| `statsIntervalInSeconds` | | |

## Producers

### Producer operations

| Method | Description | Return type |
| :----- | :---------- | :---------- |
| `send(message)` | | |
| `flush()` | | |
| `close()` | | |

### Producer configuration

| Parameter | Description | Default |
| :-------- | :---------- | :------ |
| `topic` | | |
| `producerName` | | |
| `sendTimeoutMs` | | |
| `initialSequenceId` | | |
| `maxPendingMessages` | | |
| `maxPendingMessagesAcrossPartitions` | | |
| `blockIfQueueFull` | | |
| `messageRoutingMode` | | |
| `hashingScheme` | | |
| `compressionType` | | |
| `batchingEnabled` | | |
| `batchingMaxPublishDelayMs` | | |
| `batchingMaxMessages` | | |
| `properties` | | |

## Consumers

### Consumer operations

| Method | Description | Return type |
| :----- | :---------- | :---------- |
| `receive()` | | |
| `receive(timeout)` | | |
| `acknowledge(message)` | | |
| `acknowledgeId(messageId)` | | |
| `acknowledgeCumulative(message)` | | |
| `acknowledgeCumulativeId(messageId)` | | |
| `close()` | | |

### Consumer configuration

| Parameter | Description | Default |
| :-------- | :---------- | :------ |
| `topic` | | |
| `subscription` | | |
| `subscriptionType` | | |
| `ackTimeoutMs` | | |
| `receiverQueueSize` | | |
| `receiverQueueSizeAcrossPartitions` | | |
| `consumerName` | | |
| `properties` | | |

## Examples

Below you'll find Node.js code examples for the `pulsar-client` library.

### Producer example

This creates a Node.js producer for the `my-topic` topic and send 10 messages on that topic:

```JavaScript
const Pulsar = require('pulsar-client');

(async () => {
  // Create a client
  const client = new Pulsar.Client({
    serviceUrl: 'pulsar://localhost:6650',
  });

  // Create a producer
  const producer = await client.createProducer({
    topic: 'my-topic',
  });

  // Send messages
  for (let i = 0; i < 10; i += 1) {
    const msg = `my-message-${i}`;
    producer.send({
      data: Buffer.from(msg),
    });
    console.log(`Sent message: ${msg}`);
  }
  await producer.flush();

  await producer.close();
  await client.close();
})();
```

### Consumer example

This creates a Node.js consumer with the `my-subscription` subscription on the `my-topic` topic, listen for incoming messages, print the content that arrive, and acknowledge each message to the Pulsar broker:

```JavaScript
const Pulsar = require('pulsar-client');

(async () => {
  // Create a client
  const client = new Pulsar.Client({
    serviceUrl: 'pulsar://localhost:6650',
  });

  // Create a consumer
  const consumer = await client.subscribe({
    topic: 'my-topic',
    subscription: 'my-subscription',
  });

  // Receive messages
  for (let i = 0; i < 10; i += 1) {
    const msg = await consumer.receive();
    console.log(msg.getData().toString());
    consumer.acknowledge(msg);
  }

  await consumer.close();
  await client.close();
})();
```

