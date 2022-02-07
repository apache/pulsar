---
id: client-libraries-cpp
title: Pulsar C++ client
sidebar_label: "C++"
---

You can use Pulsar C++ client to create Pulsar producers and consumers in C++.

All the methods in producer, consumer, and reader of a C++ client are thread-safe.

## Supported platforms

Pulsar C++ client is supported on **Linux** ,**MacOS** and **Windows** platforms.

[Doxygen](http://www.doxygen.nl/)-generated API docs for the C++ client are available [here](/api/cpp).

## System requirements

You need to install the following components before using the C++ client:

* [CMake](https://cmake.org/)
* [Boost](http://www.boost.org/)
* [Protocol Buffers](https://developers.google.com/protocol-buffers/) >= 3
* [libcurl](https://curl.se/libcurl/)
* [Google Test](https://github.com/google/googletest)

## Linux

### Compilation 

1. Clone the Pulsar repository.

```shell

$ git clone https://github.com/apache/pulsar

```

2. Install all necessary dependencies.

```shell

$ apt-get install cmake libssl-dev libcurl4-openssl-dev liblog4cxx-dev \
  libprotobuf-dev protobuf-compiler libboost-all-dev google-mock libgtest-dev libjsoncpp-dev

```

3. Compile and install [Google Test](https://github.com/google/googletest).

```shell

# libgtest-dev version is 1.18.0 or above
$ cd /usr/src/googletest
$ sudo cmake .
$ sudo make
$ sudo cp ./googlemock/libgmock.a ./googlemock/gtest/libgtest.a /usr/lib/

# less than 1.18.0
$ cd /usr/src/gtest
$ sudo cmake .
$ sudo make
$ sudo cp libgtest.a /usr/lib

$ cd /usr/src/gmock
$ sudo cmake .
$ sudo make
$ sudo cp libgmock.a /usr/lib

```

4. Compile the Pulsar client library for C++ inside the Pulsar repository.

```shell

$ cd pulsar-client-cpp
$ cmake .
$ make

```

After you install the components successfully, the files `libpulsar.so` and `libpulsar.a` are in the `lib` folder of the repository. The tools `perfProducer` and `perfConsumer` are in the `perf` directory.

### Install Dependencies

> Since 2.1.0 release, Pulsar ships pre-built RPM and Debian packages. You can download and install those packages directly.

After you download and install RPM or DEB, the `libpulsar.so`, `libpulsarnossl.so`, `libpulsar.a`, and `libpulsarwithdeps.a` libraries are in your `/usr/lib` directory.

By default, they are built in code path `${PULSAR_HOME}/pulsar-client-cpp`. You can build with the command below.

 `cmake . -DBUILD_TESTS=OFF -DLINK_STATIC=ON && make pulsarShared pulsarSharedNossl pulsarStatic pulsarStaticWithDeps -j 3`.

These libraries rely on some other libraries. If you want to get detailed version of dependencies, see [RPM](https://github.com/apache/pulsar/blob/master/pulsar-client-cpp/pkg/rpm/Dockerfile) or [DEB](https://github.com/apache/pulsar/blob/master/pulsar-client-cpp/pkg/deb/Dockerfile) files.

1. `libpulsar.so` is a shared library, containing statically linked `boost` and `openssl`. It also dynamically links all other necessary libraries. You can use this Pulsar library with the command below.

```bash

 g++ --std=c++11  PulsarTest.cpp -o test /usr/lib/libpulsar.so -I/usr/local/ssl/include

```

2. `libpulsarnossl.so` is a shared library, similar to `libpulsar.so` except that the libraries `openssl` and `crypto` are dynamically linked. You can use this Pulsar library with the command below.

```bash

 g++ --std=c++11  PulsarTest.cpp -o test /usr/lib/libpulsarnossl.so -lssl -lcrypto -I/usr/local/ssl/include -L/usr/local/ssl/lib

```

3. `libpulsar.a` is a static library. You need to load dependencies before using this library. You can use this Pulsar library with the command below.

```bash

 g++ --std=c++11  PulsarTest.cpp -o test /usr/lib/libpulsar.a -lssl -lcrypto -ldl -lpthread  -I/usr/local/ssl/include -L/usr/local/ssl/lib -lboost_system -lboost_regex -lcurl -lprotobuf -lzstd -lz

```

4. `libpulsarwithdeps.a` is a static library, based on `libpulsar.a`. It is archived in the dependencies of `libboost_regex`, `libboost_system`, `libcurl`, `libprotobuf`, `libzstd` and `libz`. You can use this Pulsar library with the command below.

```bash

 g++ --std=c++11  PulsarTest.cpp -o test /usr/lib/libpulsarwithdeps.a -lssl -lcrypto -ldl -lpthread  -I/usr/local/ssl/include -L/usr/local/ssl/lib

```

The `libpulsarwithdeps.a` does not include library openssl related libraries `libssl` and `libcrypto`, because these two libraries are related to security. It is more reasonable and easier to use the versions provided by the local system to handle security issues and upgrade libraries.

### Install RPM

1. Download a RPM package from the links in the table. 

| Link | Crypto files |
|------|--------------|
| [client](@pulsar:dist_rpm:client@) | [asc](@pulsar:dist_rpm:client@.asc), [sha512](@pulsar:dist_rpm:client@.sha512) |
| [client-debuginfo](@pulsar:dist_rpm:client-debuginfo@) | [asc](@pulsar:dist_rpm:client-debuginfo@.asc),  [sha512](@pulsar:dist_rpm:client-debuginfo@.sha512) |
| [client-devel](@pulsar:dist_rpm:client-devel@) | [asc](@pulsar:dist_rpm:client-devel@.asc),  [sha512](@pulsar:dist_rpm:client-devel@.sha512) |

2. Install the package using the following command.

```bash

$ rpm -ivh apache-pulsar-client*.rpm

```

After you install RPM successfully, Pulsar libraries are in the `/usr/lib` directory.

:::note

If you get the error that `libpulsar.so: cannot open shared object file: No such file or directory` when starting Pulsar client, you may need to run `ldconfig` first.

:::

### Install Debian

1. Download a Debian package from the links in the table. 

| Link | Crypto files |
|------|--------------|
| [client](@pulsar:deb:client@) | [asc](@pulsar:dist_deb:client@.asc), [sha512](@pulsar:dist_deb:client@.sha512) |
| [client-devel](@pulsar:deb:client-devel@) | [asc](@pulsar:dist_deb:client-devel@.asc),  [sha512](@pulsar:dist_deb:client-devel@.sha512) |

2. Install the package using the following command.

```bash

$ apt install ./apache-pulsar-client*.deb

```

After you install DEB successfully, Pulsar libraries are in the `/usr/lib` directory.

### Build

> If you want to build RPM and Debian packages from the latest master, follow the instructions below. You should run all the instructions at the root directory of your cloned Pulsar repository.

There are recipes that build RPM and Debian packages containing a
statically linked `libpulsar.so` / `libpulsarnossl.so` / `libpulsar.a` / `libpulsarwithdeps.a` with all required dependencies.

To build the C++ library packages, you need to build the Java packages first.

```shell

mvn install -DskipTests

```

#### RPM

To build the RPM inside a Docker container, use the command below. The RPMs are in the `pulsar-client-cpp/pkg/rpm/RPMS/x86_64/` path.

```shell

pulsar-client-cpp/pkg/rpm/docker-build-rpm.sh

```

| Package name | Content |
|-----|-----|
| pulsar-client | Shared library `libpulsar.so` and `libpulsarnossl.so` |
| pulsar-client-devel | Static library `libpulsar.a`, `libpulsarwithdeps.a`and C++ and C headers |
| pulsar-client-debuginfo | Debug symbols for `libpulsar.so` |

#### Debian

To build Debian packages, enter the following command.

```shell

pulsar-client-cpp/pkg/deb/docker-build-deb.sh

```

Debian packages are created in the `pulsar-client-cpp/pkg/deb/BUILD/DEB/` path.

| Package name | Content |
|-----|-----|
| pulsar-client | Shared library `libpulsar.so` and `libpulsarnossl.so` |
| pulsar-client-dev | Static library `libpulsar.a`, `libpulsarwithdeps.a` and C++ and C headers |

## MacOS

### Compilation

1. Clone the Pulsar repository.

```shell

$ git clone https://github.com/apache/pulsar

```

2. Install all necessary dependencies.

```shell

# OpenSSL installation
$ brew install openssl
$ export OPENSSL_INCLUDE_DIR=/usr/local/opt/openssl/include/
$ export OPENSSL_ROOT_DIR=/usr/local/opt/openssl/

# Protocol Buffers installation
$ brew install protobuf boost boost-python log4cxx
# If you are using python3, you need to install boost-python3 

# Google Test installation
$ git clone https://github.com/google/googletest.git
$ cd googletest
$ cmake .
$ make install

```

3. Compile the Pulsar client library in the repository that you cloned.

```shell

$ cd pulsar-client-cpp
$ cmake .
$ make

```

### Install `libpulsar`

Pulsar releases are available in the [Homebrew](https://brew.sh/) core repository. You can install the C++ client library with the following command. The package is installed with the library and headers.

```shell

brew install libpulsar

```

## Windows (64-bit)

### Compilation

1. Clone the Pulsar repository.

```shell

$ git clone https://github.com/apache/pulsar

```

2. Install all necessary dependencies.

```shell

cd ${PULSAR_HOME}/pulsar-client-cpp
vcpkg install --feature-flags=manifests --triplet x64-windows

```

3. Build C++ libraries.

```shell

cmake -B ./build -A x64 -DBUILD_PYTHON_WRAPPER=OFF -DBUILD_TESTS=OFF -DVCPKG_TRIPLET=x64-windows -DCMAKE_BUILD_TYPE=Release -S .
cmake --build ./build --config Release

```

> **NOTE**
>
> 1. For Windows 32-bit, you need to use `-A Win32` and `-DVCPKG_TRIPLET=x86-windows`.
> 2. For MSVC Debug mode, you need to replace `Release` with `Debug` for both `CMAKE_BUILD_TYPE` variable and `--config` option.

4. Client libraries are available in the following places.

```

${PULSAR_HOME}/pulsar-client-cpp/build/lib/Release/pulsar.lib
${PULSAR_HOME}/pulsar-client-cpp/build/lib/Release/pulsar.dll

```

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

## Create a consumer

To use Pulsar as a consumer, you need to create a consumer on the C++ client. There are two main ways of using the consumer:
- [Blocking style](#blocking-example): synchronously calling `receive(msg)`.
- [Non-blocking](#consumer-with-a-message-listener) (event based) style: using a message listener.

### Blocking example

The benefit of this approach is that it is the simplest code. Simply keeps calling `receive(msg)` which blocks until a message is received.

This example starts a subscription at the earliest offset and consumes 100 messages.

```c++

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

You can avoid  running a loop with blocking calls with an event based style by using a message listener which is invoked for each message that is received.

This example starts a subscription at the earliest offset and consumes 100 messages.

```c++

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

## Create a producer

To use Pulsar as a producer, you need to create a producer on the C++ client. There are two main ways of using a producer:
- [Blocking style](#simple-blocking-example) : each call to `send` waits for an ack from the broker.
- [Non-blocking asynchronous style](#non-blocking-example) : `sendAsync` is called instead of `send` and a callback is supplied for when the ack is received from the broker.

### Simple blocking example

This example sends 100 messages using the blocking style. While simple, it does not produce high throughput as it waits for each ack to come back before sending the next message.

```c++

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

```c++

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

```c++

ProducerConfiguration producerConf;
producerConf.setPartitionsRoutingMode(ProducerConfiguration::UseSinglePartition);
producerConf.setLazyStartPartitionedProducers(true);

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

For complete examples, refer to [C++ client examples](https://github.com/apache/pulsar/tree/master/pulsar-client-cpp/examples).

## Schema

This section describes some examples about schema. For more information about
schema, see [Pulsar schema](schema-get-started).

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

   ```c++
   
   #include <pulsar/ProtobufNativeSchema.h>
   
   ```

​
3. Create a producer to send a `User` instance.
​

   ```c++
   
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

   ```c++
   
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

