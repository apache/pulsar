---
id: version-2.6.2-client-libraries-cpp
title: Pulsar C++ client
sidebar_label: C++
original_id: client-libraries-cpp
---

You can use Pulsar C++ client to create Pulsar producers and consumers in C++.

All the methods in producer, consumer, and reader of a C++ client are thread-safe.

## Supported platforms

Pulsar C++ client is supported on **Linux** and **MacOS** platforms.

[Doxygen](http://www.doxygen.nl/)-generated API docs for the C++ client are available [here](/api/cpp).

## Linux

> Since 2.1.0 release, Pulsar ships pre-built RPM and Debian packages. You can download and install those packages directly.

Four kind of libraries `libpulsar.so` / `libpulsarnossl.so` / `libpulsar.a` / `libpulsarwithdeps.a` are included in your `/usr/lib` after rpm/deb download and install.
By default, they are build under code path `${PULSAR_HOME}/pulsar-client-cpp`, using command
 `cmake . -DBUILD_TESTS=OFF -DLINK_STATIC=ON && make pulsarShared pulsarSharedNossl pulsarStatic pulsarStaticWithDeps -j 3`
These libraries rely on some other libraries, if you want to get detailed version of dependencies libraries, please reference [these](https://github.com/apache/pulsar/blob/master/pulsar-client-cpp/pkg/rpm/Dockerfile) [files](https://github.com/apache/pulsar/blob/master/pulsar-client-cpp/pkg/deb/Dockerfile).

1. `libpulsar.so` is the Shared library, it contains statically linked `boost` and `openssl`, and will also dynamically link all other needed libraries.
The command the when use this pulsar library is like this:
```bash
 g++ --std=c++11  PulsarTest.cpp -o test /usr/lib/libpulsar.so -I/usr/local/ssl/include
```

2. `libpulsarnossl.so` is the Shared library that similar to `libpulsar.so` except that the library `openssl` and `crypto` are dynamically linked.
The command the when use this pulsar library is like this:
```bash
 g++ --std=c++11  PulsarTest.cpp -o test /usr/lib/libpulsarnossl.so -lssl -lcrypto -I/usr/local/ssl/include -L/usr/local/ssl/lib
```

3. `libpulsar.a` is the Static library, it need to load some dependencies library when using it. 
The command the when use this pulsar library is like this:
```bash
 g++ --std=c++11  PulsarTest.cpp -o test /usr/lib/libpulsar.a -lssl -lcrypto -ldl -lpthread  -I/usr/local/ssl/include -L/usr/local/ssl/lib -lboost_system -lboost_regex -lcurl -lprotobuf -lzstd -lz
```

4. `libpulsarwithdeps.a` is the Static library, base on `libpulsar.a`, and archived in the dependencies libraries of `libboost_regex`,  `libboost_system`, `libcurl`, `libprotobuf`, `libzstd` and `libz`, 
The command the when use this pulsar library is like this:
```bash
 g++ --std=c++11  PulsarTest.cpp -o test /usr/lib/libpulsarwithdeps.a -lssl -lcrypto -ldl -lpthread  -I/usr/local/ssl/include -L/usr/local/ssl/lib
```
`libpulsarwithdeps.a` does not include library openssl related libraries: `libssl` and `libcrypto`, because these 2 library is related to security, 
by using user local system provided version is more reasonable, and more easy for user to handling security issue and library upgrade.

### Install RPM

1. Download a RPM package from the links in the table. 

| Link | Crypto files |
|------|--------------|
| [client]({{pulsar:dist_rpm:client}}) | [asc]({{pulsar:dist_rpm:client}}.asc), [sha512]({{pulsar:dist_rpm:client}}.sha512) |
| [client-debuginfo]({{pulsar:dist_rpm:client-debuginfo}}) | [asc]({{pulsar:dist_rpm:client-debuginfo}}.asc),  [sha512]({{pulsar:dist_rpm:client-debuginfo}}.sha512) |
| [client-devel]({{pulsar:dist_rpm:client-devel}}) | [asc]({{pulsar:dist_rpm:client-devel}}.asc),  [sha512]({{pulsar:dist_rpm:client-devel}}.sha512) |

2. Install the package using the following command.

```bash
$ rpm -ivh apache-pulsar-client*.rpm
```

After install, Pulsar libraries will be placed under `/usr/lib`.

### Install Debian

1. Download a Debian package from the links in the table. 

| Link | Crypto files |
|------|--------------|
| [client]({{pulsar:deb:client}}) | [asc]({{pulsar:dist_deb:client}}.asc), [sha512]({{pulsar:dist_deb:client}}.sha512) |
| [client-devel]({{pulsar:deb:client-devel}}) | [asc]({{pulsar:dist_deb:client-devel}}.asc),  [sha512]({{pulsar:dist_deb:client-devel}}.sha512) |

2. Install the package using the following command:

```bash
$ apt install ./apache-pulsar-client*.deb
```

After install, Pulsar libraries will be placed under `/usr/lib`.

### Build

> If you want to build RPM and Debian packages from the latest master, follow the instructions below. All the instructions are run at the root directory of your cloned Pulsar repository.

There are recipes that build RPM and Debian packages containing a
statically linked `libpulsar.so` / `libpulsarnossl.so` / `libpulsar.a` / `libpulsarwithdeps.a` with all the required
dependencies.

To build the C++ library packages, build the Java packages first.

```shell
mvn install -DskipTests
```

#### RPM

```shell
pulsar-client-cpp/pkg/rpm/docker-build-rpm.sh
```

This builds the RPM inside a Docker container and it leaves the RPMs in `pulsar-client-cpp/pkg/rpm/RPMS/x86_64/`.

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

Debian packages are created at `pulsar-client-cpp/pkg/deb/BUILD/DEB/`.

| Package name | Content |
|-----|-----|
| pulsar-client | Shared library `libpulsar.so` and `libpulsarnossl.so` |
| pulsar-client-dev | Static library `libpulsar.a`, `libpulsarwithdeps.a` and C++ and C headers |

## MacOS

Pulsar releases are available in the [Homebrew](https://brew.sh/) core repository. You can install the C++ client library with the following command. The package is installed with the library and headers.

```shell
brew install libpulsar
```

## Connection URLs

To connect to Pulsar using client libraries, you need to specify a Pulsar protocol URL.

Pulsar protocol URLs are assigned to specific clusters, you can use the Pulsar URI scheme. The default port is `6650`. The following is an example for localhost.

```http
pulsar://localhost:6650
```

In a Pulsar cluster in production, the URL looks as follows: 
```http
pulsar://pulsar.us-west.example.com:6650
```

If you use TLS authentication, you need to add `ssl`, and the default port is `6651`. The following is an example.
```http
pulsar+ssl://pulsar.us-west.example.com:6651
```

## Create a consumer
To connect to Pulsar as a consumer, you need to create a consumer on the C++ client. The following is an example. 

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
To connect to Pulsar as a producer, you need to create a producer on the C++ client. The following is an example. 

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

This section describes some examples about schema. For more information about schema, see [Pulsar schema](schema-get-started.md).

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