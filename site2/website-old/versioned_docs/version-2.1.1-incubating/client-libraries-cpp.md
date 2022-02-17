---
id: version-2.1.1-incubating-client-libraries-cpp
title: The Pulsar C++ client
sidebar_label: C++
original_id: client-libraries-cpp
---

## Supported platforms

The Pulsar C++ client has been successfully tested on **MacOS** and **Linux**.

## Linux

### Install

> Since the 2.1.0 release, Pulsar ships pre-built RPM and Debian packages. You can choose to download
> and install those packages instead of building them yourself.

#### RPM

| Link | Crypto files |
|------|--------------|
| [client]({{pulsar:rpm:client}}) | [asc]({{pulsar:rpm:client}}.asc), [sha512]({{pulsar:rpm:client}}.sha512) |
| [client-debuginfo]({{pulsar:rpm:client-debuginfo}}) | [asc]({{pulsar:rpm:client-debuginfo}}.asc),  [sha512]({{pulsar:rpm:client-debuginfo}}.sha512) |
| [client-devel]({{pulsar:rpm:client-devel}}) | [asc]({{pulsar:rpm:client-devel}}.asc),  [sha512]({{pulsar:rpm:client-devel}}.sha512) |

To install a RPM package, download the RPM packages and install them using the following command:

```bash
$ rpm -ivh apache-pulsar-client*.rpm
```

#### DEB

| Link | Crypto files |
|------|--------------|
| [client]({{pulsar:deb:client}}) | [asc]({{pulsar:deb:client}}.asc), [sha512]({{pulsar:deb:client}}.sha512) |
| [client-devel]({{pulsar:deb:client-devel}}) | [asc]({{pulsar:deb:client-devel}}.asc),  [sha512]({{pulsar:deb:client-devel}}.sha512) |

To install a DEB package, download the DEB packages and install them using the following command:

```bash
$ apt-install apache-pulsar-client*.deb
```

### Build

> If you want to build RPM and Debian packages off latest master, you can follow the instructions
> below to do so. All the instructions are run at the root directory of your cloned Pulsar
> repo.

There are recipes that build RPM and Debian packages containing a
statically linked `libpulsar.so` / `libpulsar.a` with all the required
dependencies.

To build the C++ library packages, first build the Java packages:

```shell
mvn install -DskipTests
```

#### RPM

```shell
pulsar-client-cpp/pkg/rpm/docker-build-rpm.sh
```

This will build the RPM inside a Docker container and it will leave the RPMs
in `pulsar-client-cpp/pkg/rpm/RPMS/x86_64/`.

| Package name | Content |
|-----|-----|
| pulsar-client | Shared library `libpulsar.so` |
| pulsar-client-devel | Static library `libpulsar.a` and C++ and C headers |
| pulsar-client-debuginfo | Debug symbols for `libpulsar.so` |

#### Deb

To build Debian packages:

```shell
pulsar-client-cpp/pkg/deb/docker-build-deb.sh
```

Debian packages will be created at `pulsar-client-cpp/pkg/deb/BUILD/DEB/`

| Package name | Content |
|-----|-----|
| pulsar-client | Shared library `libpulsar.so` |
| pulsar-client-dev | Static library `libpulsar.a` and C++ and C headers |

## MacOS

Use the [Homebrew](https://brew.sh/) supplied recipe to build the Pulsar
client lib on MacOS.

```shell
brew install https://raw.githubusercontent.com/apache/incubator-pulsar/master/pulsar-client-cpp/homebrew/libpulsar.rb
```

If using Python 3 on MacOS, add the flag `--with-python3` to the above command.

This will install the package with the library and headers.

## Connection URLs


To connect to Pulsar using client libraries, you need to specify a Pulsar protocol URL.

Pulsar protocol URLs are assigned to specific clusters, use the pulsar URI scheme and have a default port of 6650. Here’s an example for localhost:

```http
pulsar://localhost:6650
```

A URL for a production Pulsar cluster may look something like this:
```http
pulsar://pulsar.us-west.example.com:6650
```

If you’re using TLS authentication, the URL will look like something like this:
```http
pulsar+ssl://pulsar.us-west.example.com:6651
```

## Consumer

```c++
Client client("pulsar://localhost:6650");

Consumer consumer;
Result result = client.subscribe("my-topic", "my-subscribtion-name", consumer);
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


## Producer

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

## Authentication

```cpp
ClientConfiguration config = ClientConfiguration();
config.setUseTls(true);
config.setTlsTrustCertsFilePath("/path/to/cacert.pem");
config.setTlsAllowInsecureConnection(false);
config.setAuth(pulsar::AuthTls::create(
            "/path/to/client-cert.pem", "/path/to/client-key.pem"););

Client client("pulsar+ssl://my-broker.com:6651", config);
```
