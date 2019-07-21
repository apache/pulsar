---
title: The Pulsar C++ client
tags: [client, cpp]
---

<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

## Supported platforms

The Pulsar C++ client has been successfully tested on **MacOS** and **Linux**.

## Linux

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

{% include explanations/client-url.md %}

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
config.setTlsTrustCertsFilePath("/path/to/cacert.pem");
config.setTlsAllowInsecureConnection(false);
config.setAuth(pulsar::AuthTls::create(
            "/path/to/client-cert.pem", "/path/to/client-key.pem"););

Client client("pulsar+ssl://my-broker.com:6651", config);
```
