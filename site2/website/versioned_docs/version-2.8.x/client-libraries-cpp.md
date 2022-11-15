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

Pulsar protocol URLs are assigned to specific clusters, you can use the Pulsar URI scheme. The default port is `6650`. The following is an example of localhost.

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

```cpp

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

```cpp

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

For complete examples, refer to [C++ client examples](https://github.com/apache/pulsar-client-cpp/tree/main/examples).

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

