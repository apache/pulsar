---
id: client-libraries-node
title: The Pulsar Node.js client
sidebar_label: Node.js
---

The Pulsar Node.js client can be used to create Pulsar [producers](#producers), [consumers](#consumers), and [readers](#readers) in Node.js.

## Installation

You can install the [`pusar-client`](https://www.npmjs.com/package/pulsar-client) library via [npm](https://www.npmjs.com/).

### Requirements
Pulsar Node.js client library is based on the C++ client library.
Follow [these instructions](client-libraries-cpp.md#compilation) and install the Pulsar C++ client library.

### Compatibility

Compatibility between each version of the Node.js client and the C++ client is as follows:

| Node.js client | C++ client     |
| :------------- | :------------- |
| 1.0.0          | 2.3.0 or later |

If an incompatible version of the C++ client is installed, you may fail to build or run this library.

### Installation using npm

Install the `pulsar-client` library via [npm](https://www.npmjs.com/):

```shell
$ npm install pulsar-client
```

> #### Note
> 
> Also, this library works only in Node.js 10.x or later because it uses the [`node-addon-api`](https://github.com/nodejs/node-addon-api) module to wrap the C++ library.

## Connection URLs
To connect to Pulsar using client libraries, you need to specify a [Pulsar protocol](developing-binary-protocol.md) URL.

Pulsar protocol URLs are assigned to specific clusters, use the `pulsar` scheme and have a default port of 6650. Here is an example for `localhost`:

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

In order to interact with Pulsar, you'll first need a client object. You can create a client instance using a `new` operator and the `Client` method, passing in a client options object (more on configuration [below](#client-configuration)).

Here is an example:

```JavaScript
const Pulsar = require('pulsar-client');

(async () => {
  const client = new Pulsar.Client({
    serviceUrl: 'pulsar://localhost:6650',
  });
  
  await client.close();
})();
```

### Client configuration

The following configurable parameters are available for Pulsar clients:

| Parameter | Description | Default |
| :-------- | :---------- | :------ |
| `serviceUrl` | The connection URL for the Pulsar cluster. See [above](#connection-urls) for more info | |
| `authentication` | | |
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

Pulsar producers publish messages to Pulsar topics. You can [configure](#producer-configuration) Node.js producers using a producer configuration object.

Here is an example:

```JavaScript
const producer = await client.createProducer({
  topic: 'my-topic',
});

await producer.send({
  data: Buffer.from("Hello, Pulsar"),
});

await producer.close();
```

> #### Promise operation
> When you create a new Pulsar producer, the operation will return `Promise` object and get producer instance or an error through executor function.  
> In this example, we use await operator instead of executor function.

### Producer operations

Pulsar Node.js producers have the following methods available:

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

### Produce example

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

> #### Promise operation
> When you create a new Pulsar consumer, the operation will return `Promise` object and get consumer instance or an error through executor function.  
> In this example, we use await operator instead of executor function.

## Consumers

Pulsar consumers subscribe to one or more Pulsar topics and listen for incoming messages produced on that topic/those topics. You can [configure](#consumer-configuration) Node.js consumers using a consumer configuration object.

Here is an example:

```JavaScript
const consumer = await client.subscribe({
  topic: 'my-topic',
  subscription: 'my-subscription',
});

const msg = await consumer.receive();
console.log(msg.getData().toString());
consumer.acknowledge(msg);

await consumer.close();
```

### Consumer operations

Pulsar Node.js consumers have the following methods available:

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

### Consume example

This creates a Node.js consumer with the `my-subscription` subscription on the `my-topic` topic, receive messages, print the content that arrive, and acknowledge each message to the Pulsar broker for 10 times:

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

## Readers

Pulsar readers process messages from Pulsar topics. Readers are different from consumers because with readers you need to explicitly specify which message in the stream you want to begin with (consumers, on the other hand, automatically begin with the most recent unacked message). You can [configure](#reader-configuration) Node.js readers using a reader configuration object.

Here is an example:

```JavaScript
const reader = await client.createReader({
  topic: 'my-topic',
  startMessageId: Pulsar.MessageId.earliest(),
});

const msg = await reader.readNext();
console.log(msg.getData().toString());

await reader.close();
```

### Reader operations

Pulsar Node.js readers have the following methods available:

| Method | Description | Return type |
| :----- | :---------- | :---------- |
| `readNext()` | | |
| `readNext(timeout)` | | |
| `hasNext()` | | |
| `close()` | | |

### Reader configuration

| Parameter | Description | Default |
| :-------- | :---------- | :------ |
| `topic` | | |
| `startMessageId` | | |
| `receiverQueueSize` | | |
| `readerName` | | |
| `subscriptionRolePrefix` | | |

### Reader example

This create a Node.js reader with the `my-topic` topic, read messages, print the content that arrive for 10 times:

```JavaScript
const Pulsar = require('pulsar-client');

(async () => {
  // Create a client
  const client = new Pulsar.Client({
    serviceUrl: 'pulsar://localhost:6650',
    operationTimeoutSeconds: 30,
  });

  // Create a reader
  const reader = await client.createReader({
    topic: 'my-topic',
    startMessageId: Pulsar.MessageId.earliest(),
  });

  // read messages
  for (let i = 0; i < 10; i += 1) {
    const msg = await reader.readNext();
    console.log(msg.getData().toString());
  }

  await reader.close();
  await client.close();
})();
```

## Messages

In Pulsar Node.js client, you have to construct producer message object for producer.

Here is an example message:

```JavaScript
const msg = {
  data: Buffer.from("Hello, Pulsar"),
  partitionKey: foo,
  properties: foo,
  eventTimestamp: foo,
  replicationClusters: foo,
}

await producer.send(msg);
```

The following keys are available for producer message objects:

### Consumer configuration

| Parameter | Description | Default |
| :-------- | :---------- | :---------- |
| `data` | | |
| `properties` | | |
| `eventTimestamp` | | |
| `sequenceId` | | |
| `partitionKey` | | |
| `replicationClusters` | | |

