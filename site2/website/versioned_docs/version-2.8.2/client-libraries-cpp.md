---
id: client-libraries-cpp
title: Pulsar C++ client
sidebar_label: "C++"
original_id: client-libraries-cpp
---

You can use Pulsar C++ client to create Pulsar producers and consumers in C++.

All the methods in producer, consumer, and reader of a C++ client are thread-safe.

## Supported platforms

Pulsar C++ client is supported on **Linux** and **MacOS** platforms.

[Doxygen](http://www.doxygen.nl/)-generated API docs for the C++ client are available [here](/api/cpp).

## System requirements

You need to install the following components before using the C++ client:

* [CMake](https://cmake.org/)
* [Boost](http://www.boost.org/)
* [Protocol Buffers](https://developers.google.com/protocol-buffers/) 2.6
* [libcurl](https://curl.haxx.se/libcurl/)
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
$ brew tap homebrew/versions
$ brew install protobuf260
$ brew install boost
$ brew install log4cxx

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

To use Pulsar as a consumer, you need to create a consumer on the C++ client. The following is an example. 

```c++

Client client("pulsar://localhost:6650");

Consumer consumer;
Result result = client.subscribe("my-topic", "my-subscription-name", consumer);
if (result != ResultOk) {
    LOG_ERROR("Failed to subscribe: " << result);
    return -1;
}

Message msg;

while (true) {
    consumer.receive(msg);
    LOG_INFO("Received: " << msg
            << "  with payload '" << msg.getDataAsString() << "'");

    consumer.acknowledge(msg);
}

client.close();

```

## Create a producer

To use Pulsar as a producer, you need to create a producer on the C++ client. The following is an example. 

```c++

Client client("pulsar://localhost:6650");

Producer producer;
Result result = client.createProducer("my-topic", producer);
if (result != ResultOk) {
    LOG_ERROR("Error creating producer: " << result);
    return -1;
}

// Publish 10 messages to the topic
for (int i = 0; i < 10; i++){
    Message msg = MessageBuilder().setContent("my-message").build();
    Result res = producer.send(msg);
    LOG_INFO("Message sent: " << res);
}
client.close();

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

This section describes some examples about schema. For more information about schema, see [Pulsar schema](schema-get-started).

### Create producer with Avro schema

The following example shows how to create a producer with an Avro schema.

```cpp

static const std::string exampleSchema =
    "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"test\","
    "\"fields\":[{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"b\",\"type\":\"int\"}]}";
Producer producer;
ProducerConfiguration producerConf;
producerConf.setSchema(SchemaInfo(AVRO, "Avro", exampleSchema));
client.createProducer("topic-avro", producerConf, producer);

```

### Create consumer with Avro schema

The following example shows how to create a consumer with an Avro schema.

```cpp

static const std::string exampleSchema =
    "{\"type\":\"record\",\"name\":\"Example\",\"namespace\":\"test\","
    "\"fields\":[{\"name\":\"a\",\"type\":\"int\"},{\"name\":\"b\",\"type\":\"int\"}]}";
ConsumerConfiguration consumerConf;
Consumer consumer;
consumerConf.setSchema(SchemaInfo(AVRO, "Avro", exampleSchema));
client.subscribe("topic-avro", "sub-2", consumerConf, consumer)

```

