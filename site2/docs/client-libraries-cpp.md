---
id: client-libraries-cpp
title: Pulsar C++ client
sidebar_label: "C++"
---

````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````

You can use a Pulsar C++ client to create producers, consumers, and readers. For Pulsar features that C++ clients support, see [Client Feature Matrix](https://docs.google.com/spreadsheets/d/1YHYTkIXR8-Ql103u-IMI18TXLlGStK8uJjDsOOA0T20/edit#gid=1784579914). For complete examples, refer to [C++ client examples](https://github.com/apache/pulsar-client-cpp/tree/main/examples).

## Changes for version 3.0.0 or later

The new version of the Pulsar C++ client starts from 3.0.0 and has been no longer consistent with Pulsar since 2.10.x. For the latest releases, see the [Download](/download/) page.

Take the [3.0.0 release](https://archive.apache.org/dist/pulsar/pulsar-client-cpp-3.0.0/) for example, there are following subdirectories:
- apk-arm64: the Alpine Linux packages for ARM64 architectures
- apk-x86_64: the Alpine Linux packages for x64 architectures
- deb-arm64: the Debian-based Linux packages for ARM64 architectures
- deb-x86_64: the Debian-based Linux packages for x64 architectures
- rpm-arm64: the RedHat-based Linux packages for ARM64 architectures
- rpm-x86_64: the RedHat-based Linux packages for x64 architectures

These Linux packages above all contain the C++ headers installed under `/usr/include` and the following libraries installed under `/usr/lib`:
- libpulsar.so: the shared library that links 3rd party dependencies statically
- libpulsar.a: the static library
- libpulsarwithdeps.a: the fat static library that includes all 3rd party dependencies

Here is an example to link these libraries for a C++ source file named `main.cc`:

```bash
# Link to libpulsar.so
g++ -std=c++11 main.cc -lpulsar
# Link to libpulsarwithdeps.a
g++ -std=c++11 main.cc /usr/lib/libpulsarwithdeps.a -lpthread -ldl
# Link to libpulsar.a
g++ -std=c++11 main.cc /usr/lib/libpulsar.a \
  -lprotobuf -lcurl -lssl -lcrypto -lz -lzstd -lsnappy -lpthread -ldl
```

:::caution

Linking to `libpulsar.a` can be difficult for beginners because the 3rd party dependencies must be compatible. For example, the protobuf version must be 3.20.0 or higher for Pulsar C++ client 3.0.0. It's better to link to `libpulsarwithdeps.a` instead.

:::

:::danger

Before 3.0.0, there was a `libpulsarnossl.so`, which is removed now.

:::

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

   This package contains static libraries: `libpulsar.a`, `libpulsarwithdeps.a`, and C/C++ headers.

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

### APK

```bash
apk add --allow-untrusted ./apache-pulsar-client-*.apk
```

## Connection URLs

To connect to Pulsar using client libraries, you need to specify a [Pulsar protocol](developing-binary-protocol.md) URL.

You can assign Pulsar protocol URLs to specific clusters and use the `pulsar` scheme. The following is an example of `localhost` with the default port `6650`:

```http
pulsar://localhost:6650
```

If you have multiple brokers, separate `IP:port` by commas:

```http
pulsar://localhost:6550,localhost:6651,localhost:6652
```

If you use [TLS](security-tls-authentication.md) authentication, add `+ssl` in the scheme:

```http
pulsar+ssl://pulsar.us-west.example.com:6651
```

## API reference

All the methods in producer, consumer, and reader of Pulsar C++ clients are thread-safe. See the [API docs](@pulsar:apidoc:cpp@) for more details.

## Release notes

For the changelog of Pulsar C++ clients, see [release notes](/release-notes/#c).

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

    Producer producer;

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

This example sends 100 messages using the non-blocking style calling `sendAsync` instead of `send`. This allows the producer to have multiple messages in-flight at a time which increases throughput.

The producer configuration `blockIfQueueFull` is useful here to avoid `ResultProducerQueueIsFull` errors when the internal queue for outgoing send requests becomes full. Once the internal queue is full, `sendAsync` becomes blocking which can make your code simpler.

Without this configuration, the result code `ResultProducerQueueIsFull` is passed to the callback. You must decide how to deal with that (retry, discard etc).

```cpp
#include <pulsar/Client.h>
#include <thread>
#include <atomic>

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

:::note

To enable chunking, you need to disable batching (`setBatchingEnabled`=`false`) concurrently.

:::

## Create a consumer

To use Pulsar as a consumer, you need to create a consumer on the C++ client. There are two main ways of using the consumer:
- [Blocking style](#blocking-example): synchronously calling `receive(msg)`.
- [Non-blocking](#consumer-with-a-message-listener) (event-based) style: using a message listener.

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

## Schema

To work with [Pulsar schema](schema-overview.md) using C++ clients, see [Schema - Get started](schema-get-started.md). For specific schema types that C++ clients support, see [code](https://github.com/apache/pulsar-client-cpp/blob/main/include/pulsar/Schema.h).
