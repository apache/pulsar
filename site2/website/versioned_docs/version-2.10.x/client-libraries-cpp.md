---
id: client-libraries-cpp
title: Pulsar C++ client
sidebar_label: "C++"
original_id: client-libraries-cpp
---

````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````

You can use a Pulsar C++ client to create producers, consumers, and readers.

All the methods in producer, consumer, and reader of a C++ client are thread-safe. You can read the [API docs](@pulsar:apidoc:cpp@) for the C++ client.

## Installation

Use one of the following methods to install a Pulsar C++ client.

### Brew

Use [Homebrew](http://brew.sh/) to install the latest tagged version with the library and headers:

```bash
brew install libpulsar
```

### Deb

1. Download any one of the Deb packages:

<Tabs>
<TabItem value="client">

```bash
wget @pulsar:deb:client@
```

This package contains shared libraries `libpulsar.so` and `libpulsarnossl.so`.

</TabItem>
<TabItem value="client-devel">

```bash
wget @pulsar:deb:client-devel@
```

This package contains static libraries: `libpulsar.a`, `libpulsarwithdeps.a` and C/C++ headers.

</TabItem>
</Tabs>

2. Install the package using the following command:

```bash
apt install ./apache-pulsar-client*.deb
```

Now, you can see Pulsar C++ client libraries installed under the `/usr/lib` directory.

### RPM

1. Download any one of the RPM packages:

<Tabs>
<TabItem value="client">

```bash
wget @pulsar:dist_rpm:client@
```

This package contains shared libraries: `libpulsar.so` and `libpulsarnossl.so`.

</TabItem>
<TabItem value="client-debuginfo">

```bash
wget @pulsar:dist_rpm:client-debuginfo@
```

This package contains debug symbols for `libpulsar.so`.

</TabItem>
<TabItem value="client-devel">

```bash
wget @pulsar:dist_rpm:client-devel@
```

This package contains static libraries: `libpulsar.a`, `libpulsarwithdeps.a` and C/C++ headers.

</TabItem>
</Tabs>

2. Install the package using the following command:

```bash
rpm -ivh apache-pulsar-client*.rpm
```

Now, you can see Pulsar C++ client libraries installed under the `/usr/lib` directory.

:::note

If you get an error like "libpulsar.so: cannot open shared object file: No such file or directory" when starting a Pulsar client, you need to run `ldconfig` first.

:::

### Source

For how to build Pulsar C++ client on different platforms from source code, see [compliation](https://github.com/apache/pulsar-client-cpp#compilation).

## Connection URLs

To connect Pulsar using client libraries, you need to specify a Pulsar protocol URL.

Pulsar protocol URLs are assigned to specific clusters, you can use the Pulsar URI scheme. The default port is `6650`. The following is an example for localhost.

```http

pulsar://localhost:6650

```

In a Pulsar cluster in production, the URL looks as follows.

```http

pulsar://pulsar.us-west.example.com:6650

```

If you use TLS authentication, you need to add `ssl`, and the default port is `6651`. The following is an example.

```http

pulsar+ssl://pulsar.us-west.example.com:6651

```

## Create a producer

To use Pulsar as a producer, you need to create a producer on the C++ client. There are two main ways of using a producer:
- [Blocking style](#simple-blocking-example) : each call to `send` waits for an ack from the broker.
- [Non-blocking asynchronous style](#non-blocking-example) : `sendAsync` is called instead of `send` and a callback is supplied for when the ack is received from the broker.

### Simple blocking example

This example sends 100 messages using the blocking style. While simple, it does not produce high throughput as it waits for each ack to come back before sending the next message.

```cpp

#include <pulsar/Client.h>
#include <thread>

using namespace pulsar;

int main() {
    Client client("pulsar://localhost:6650");

    Result result = client.createProducer("persistent://public/default/my-topic", producer);
    if (result != ResultOk) {
        std::cout << "Error creating producer: " << result << std::endl;
        return -1;
    }

    // Send 100 messages synchronously
    int ctr = 0;
    while (ctr < 100) {
        std::string content = "msg" + std::to_string(ctr);
        Message msg = MessageBuilder().setContent(content).setProperty("x", "1").build();
        Result result = producer.send(msg);
        if (result != ResultOk) {
            std::cout << "The message " << content << " could not be sent, received code: " << result << std::endl;
        } else {
            std::cout << "The message " << content << " sent successfully" << std::endl;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        ctr++;
    }

    std::cout << "Finished producing synchronously!" << std::endl;

    client.close();
    return 0;
}

```

### Non-blocking example

This example sends 100 messages using the non-blocking style calling `sendAsync` instead of `send`. This allows the producer to have multiple messages inflight at a time which increases throughput.

The producer configuration `blockIfQueueFull` is useful here to avoid `ResultProducerQueueIsFull` errors when the internal queue for outgoing send requests becomes full. Once the internal queue is full, `sendAsync` becomes blocking which can make your code simpler.

Without this configuration, the result code `ResultProducerQueueIsFull` is passed to the callback. You must decide how to deal with that (retry, discard etc).

```cpp

#include <pulsar/Client.h>
#include <thread>

using namespace pulsar;

std::atomic<uint32_t> acksReceived;

void callback(Result code, const MessageId& msgId, std::string msgContent) {
    // message processing logic here
    std::cout << "Received ack for msg: " << msgContent << " with code: "
        << code << " -- MsgID: " << msgId << std::endl;
    acksReceived++;
}

int main() {
    Client client("pulsar://localhost:6650");

    ProducerConfiguration producerConf;
    producerConf.setBlockIfQueueFull(true);
    Producer producer;
    Result result = client.createProducer("persistent://public/default/my-topic",
                                          producerConf, producer);
    if (result != ResultOk) {
        std::cout << "Error creating producer: " << result << std::endl;
        return -1;
    }

    // Send 100 messages asynchronously
    int ctr = 0;
    while (ctr < 100) {
        std::string content = "msg" + std::to_string(ctr);
        Message msg = MessageBuilder().setContent(content).setProperty("x", "1").build();
        producer.sendAsync(msg, std::bind(callback,
                                          std::placeholders::_1, std::placeholders::_2, content));

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        ctr++;
    }

    // wait for 100 messages to be acked
    while (acksReceived < 100) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    std::cout << "Finished producing asynchronously!" << std::endl;

    client.close();
    return 0;
}

```

### Partitioned topics and lazy producers

When scaling out a Pulsar topic, you may configure a topic to have hundreds of partitions. Likewise, you may have also scaled out your producers so there are hundreds or even thousands of producers. This can put some strain on the Pulsar brokers as when you create a producer on a partitioned topic, internally it creates one internal producer per partition which involves communications to the brokers for each one. So for a topic with 1000 partitions and 1000 producers, it ends up creating 1,000,000 internal producers across the producer applications, each of which has to communicate with a broker to find out which broker it should connect to and then perform the connection handshake.

You can reduce the load caused by this combination of a large number of partitions and many producers by doing the following:
- use SinglePartition partition routing mode (this ensures that all messages are only sent to a single, randomly selected partition)
- use non-keyed messages (when messages are keyed, routing is based on the hash of the key and so messages will end up being sent to multiple partitions)
- use lazy producers (this ensures that an internal producer is only created on demand when a message needs to be routed to a partition)

With our example above, that reduces the number of internal producers spread out over the 1000 producer apps from 1,000,000 to just 1000.

Note that there can be extra latency for the first message sent. If you set a low send timeout, this timeout could be reached if the initial connection handshake is slow to complete.

```cpp

ProducerConfiguration producerConf;
producerConf.setPartitionsRoutingMode(ProducerConfiguration::UseSinglePartition);
producerConf.setLazyStartPartitionedProducers(true);

```

### Enable chunking

Message [chunking](concepts-messaging.md#chunking) enables Pulsar to process large payload messages by splitting the message into chunks at the producer side and aggregating chunked messages at the consumer side.

The message chunking feature is OFF by default. The following is an example about how to enable message chunking when creating a producer.

```cpp

ProducerConfiguration conf;
conf.setBatchingEnabled(false);
conf.setChunkingEnabled(true);
Producer producer;
client.createProducer("my-topic", conf, producer);

```

> **Note:** To enable chunking, you need to disable batching (`setBatchingEnabled`=`false`) concurrently.

## Create a consumer

To use Pulsar as a consumer, you need to create a consumer on the C++ client. There are two main ways of using the consumer:
- [Blocking style](#blocking-example): synchronously calling `receive(msg)`.
- [Non-blocking](#consumer-with-a-message-listener) (event based) style: using a message listener.

### Blocking example

The benefit of this approach is that it is the simplest code. Simply keeps calling `receive(msg)` which blocks until a message is received.

This example starts a subscription at the earliest offset and consumes 100 messages.

```cpp

#include <pulsar/Client.h>

using namespace pulsar;

int main() {
    Client client("pulsar://localhost:6650");

    Consumer consumer;
    ConsumerConfiguration config;
    config.setSubscriptionInitialPosition(InitialPositionEarliest);
    Result result = client.subscribe("persistent://public/default/my-topic", "consumer-1", config, consumer);
    if (result != ResultOk) {
        std::cout << "Failed to subscribe: " << result << std::endl;
        return -1;
    }

    Message msg;
    int ctr = 0;
    // consume 100 messages
    while (ctr < 100) {
        consumer.receive(msg);
        std::cout << "Received: " << msg
            << "  with payload '" << msg.getDataAsString() << "'" << std::endl;

        consumer.acknowledge(msg);
        ctr++;
    }

    std::cout << "Finished consuming synchronously!" << std::endl;

    client.close();
    return 0;
}

```

### Consumer with a message listener

You can avoid running a loop by blocking calls with an event-based style by using a message listener which is invoked for each message that is received.

This example starts a subscription at the earliest offset and consumes 100 messages.

```cpp

#include <pulsar/Client.h>
#include <atomic>
#include <thread>

using namespace pulsar;

std::atomic<uint32_t> messagesReceived;

void handleAckComplete(Result res) {
    std::cout << "Ack res: " << res << std::endl;
}

void listener(Consumer consumer, const Message& msg) {
    std::cout << "Got message " << msg << " with content '" << msg.getDataAsString() << "'" << std::endl;
    messagesReceived++;
    consumer.acknowledgeAsync(msg.getMessageId(), handleAckComplete);
}

int main() {
    Client client("pulsar://localhost:6650");

    Consumer consumer;
    ConsumerConfiguration config;
    config.setMessageListener(listener);
    config.setSubscriptionInitialPosition(InitialPositionEarliest);
    Result result = client.subscribe("persistent://public/default/my-topic", "consumer-1", config, consumer);
    if (result != ResultOk) {
        std::cout << "Failed to subscribe: " << result << std::endl;
        return -1;
    }

    // wait for 100 messages to be consumed
    while (messagesReceived < 100) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    std::cout << "Finished consuming asynchronously!" << std::endl;

    client.close();
    return 0;
}

```

### Configure chunking

You can limit the maximum number of chunked messages a consumer maintains concurrently by configuring the `setMaxPendingChunkedMessage` and `setAutoAckOldestChunkedMessageOnQueueFull` parameters. When the threshold is reached, the consumer drops pending messages by silently acknowledging them or asking the broker to redeliver them later.

The following is an example of how to configure message chunking.

```cpp

ConsumerConfiguration conf;
conf.setAutoAckOldestChunkedMessageOnQueueFull(true);
conf.setMaxPendingChunkedMessage(100);
Consumer consumer;
client.subscribe("my-topic", "my-sub", conf, consumer);

```

## Enable authentication in connection URLs
If you use TLS authentication when connecting to Pulsar, you need to add `ssl` in the connection URLs, and the default port is `6651`. The following is an example.

```cpp

ClientConfiguration config = ClientConfiguration();
config.setUseTls(true);
config.setTlsTrustCertsFilePath("/path/to/cacert.pem");
config.setTlsAllowInsecureConnection(false);
config.setAuth(pulsar::AuthTls::create(
            "/path/to/client-cert.pem", "/path/to/client-key.pem"););

Client client("pulsar+ssl://my-broker.com:6651", config);

```

For complete examples, refer to [C++ client examples](https://github.com/apache/pulsar-client-cpp/tree/main/examples).

## Schema

This section describes some examples about schema. For more information about
schema, see [Pulsar schema](schema-get-started.md).

### Avro schema

- The following example shows how to create a producer with an Avro schema.

  ```cpp

  static const std::string exampleSchema =
      "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"test\","
      "\"fields\":[{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"b\",\"type\":\"int\"}]}";
  Producer producer;
  ProducerConfiguration producerConf;
  producerConf.setSchema(SchemaInfo(AVRO, "Avro", exampleSchema));
  client.createProducer("topic-avro", producerConf, producer);

  ```

- The following example shows how to create a consumer with an Avro schema.

  ```cpp

  static const std::string exampleSchema =
      "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"test\","
      "\"fields\":[{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"b\",\"type\":\"int\"}]}";
  ConsumerConfiguration consumerConf;
  Consumer consumer;
  consumerConf.setSchema(SchemaInfo(AVRO, "Avro", exampleSchema));
  client.subscribe("topic-avro", "sub-2", consumerConf, consumer)

  ```

### ProtobufNative schema

The following example shows how to create a producer and a consumer with a ProtobufNative schema.
​
1. Generate the `User` class using Protobuf3.

   :::note

   You need to use Protobuf3 or later versions.

   :::

​

   ```protobuf

   syntax = "proto3";

   message User {
       string name = 1;
       int32 age = 2;
   }

   ```

​
2. Include the `ProtobufNativeSchema.h` in your source code. Ensure the Protobuf dependency has been added to your project.
​

   ```cpp

   #include <pulsar/ProtobufNativeSchema.h>

   ```

​
3. Create a producer to send a `User` instance.
​

   ```cpp

   ProducerConfiguration producerConf;
   producerConf.setSchema(createProtobufNativeSchema(User::GetDescriptor()));
   Producer producer;
   client.createProducer("topic-protobuf", producerConf, producer);
   User user;
   user.set_name("my-name");
   user.set_age(10);
   std::string content;
   user.SerializeToString(&content);
   producer.send(MessageBuilder().setContent(content).build());

   ```

​
4. Create a consumer to receive a `User` instance.
​

   ```cpp

   ConsumerConfiguration consumerConf;
   consumerConf.setSchema(createProtobufNativeSchema(User::GetDescriptor()));
   consumerConf.setSubscriptionInitialPosition(InitialPositionEarliest);
   Consumer consumer;
   client.subscribe("topic-protobuf", "my-sub", consumerConf, consumer);
   Message msg;
   consumer.receive(msg);
   User user2;
   user2.ParseFromArray(msg.getData(), msg.getLength());

   ```

