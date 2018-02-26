---
title: The Pulsar Java client
tags: [client, java]
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

The Pulsar Java client can be used both to create Java {% popover producers %} and {% popover consumers %} of messages but also to perform [administrative tasks](../../admin-api/overview).

The current version of the Java client is **{{ site.current_version }}**.

Javadoc for the Pulsar client is divided up into two domains, by package:

Package | Description
:-------|:-----------
[`org.apache.pulsar.client.api`](/api/client) | The {% popover producer %} and {% popover consumer %} API
[`org.apache.pulsar.client.admin`](/api/admin) | The Java [admin API](../../admin-api/overview)

This document will focus only on the client API for producing and consuming messages on Pulsar {% popover topics %}. For a guide to using the Java admin client, see [The Pulsar admin interface](../../admin-api/overview).

## Installation

The latest version of the Pulsar Java client library is available via [Maven Central](http://search.maven.org/#artifactdetails%7Corg.apache.pulsar%7Cpulsar-client%7C{{ site.current_version }}%7Cjar). To use the latest version, add the `pulsar-client` library to your build configuration.

### Maven

If you're using Maven, add this to your `pom.xml`:

```xml
<!-- in your <properties> block -->
<pulsar.version>{{ site.current_version }}</pulsar.version>

<!-- in your <dependencies> block -->
<dependency>
  <groupId>org.apache.pulsar</groupId>
  <artifactId>pulsar-client</artifactId>
  <version>${pulsar.version}</version>
</dependency>
```

### Gradle

If you're using Gradle, add this to your `build.gradle` file:

```groovy
def pulsarVersion = '{{ site.current_version }}'

dependencies {
    compile group: 'org.apache.pulsar', name: 'pulsar-client', version: pulsarVersion
}
```

## Connection URLs

{% include explanations/client-url.md %}

## Client configuration

You can instantiate a {% javadoc PulsarClient client org.apache.pulsar.client.api.PulsarClient %} object using just a URL for the target Pulsar {% popover cluster %}, like this:

```java
String pulsarBrokerRootUrl = "pulsar://localhost:6650";
PulsarClient client = PulsarClient.create(pulsarBrokerRootUrl);
```

This `PulsarClient` object will use the default configuration. See the Javadoc for {% javadoc ClientConfiguration client org.apache.pulsar.client.api.ClientConfiguration %} to see how to provide a non-default configuration.

{% include admonition.html type="info" content="
In addition to client-level configuration, you can also apply [producer](#configuring-producers)- and [consumer](#configuring-consumers)-specific configuration, as you'll see in the sections below.
" %}

## Producers

In Pulsar, {% popover producers %} write {% popover messages %} to {% popover topics %}. You can instantiate a new {% popover producer %} by first instantiating a {% javadoc PulsarClient client org.apache.pulsar.client.api.PulsarClient %}, passing it a URL for a Pulsar {% popover broker %}.

```java
String pulsarBrokerRootUrl = "pulsar://localhost:6650";
PulsarClient client = PulsarClient.create(pulsarBrokerRootUrl);
```

{% include admonition.html type='info' title='Default broker URLs for standalone clusters' content="
If you're running a cluster in [standalone mode](../../getting-started/LocalCluster), the broker will be available at the `pulsar://localhost:6650` URL by default." %}

Once you've instantiated a {% javadoc PulsarClient client org.apache.pulsar.client.api.PulsarClient %} object, you can create a {% javadoc Producer client org.apache.pulsar.client.api.Producer %} for a {% popover topic %}.

```java
String topic = "persistent://sample/standalone/ns1/my-topic";
Producer producer = client.createProducer(topic);
```

You can then send messages to the broker and topic you specified:

```java
// Publish 10 messages to the topic
for (int i = 0; i < 10; i++) {
    producer.send("my-message".getBytes());
}
```

{% include admonition.html type='warning' content="
You should always make sure to close your producers, consumers, and clients when they are no longer needed:

```java
producer.close();
consumer.close();
client.close();
```

Closer operations can also be asynchronous:

```java
producer.asyncClose();
consumer.asyncClose();
clioent.asyncClose();
```
" %}


### Configuring producers

If you instantiate a `Producer` object specifying only a topic name, as in the example above, the producer will use the default configuration. To use a non-default configuration, you can instantiate the `Producer` with a {% javadoc ProducerConfiguration client org.apache.pulsar.client.api.ProducerConfiguration %} object as well. Here's an example configuration:

```java
PulsarClient client = PulsarClient.create(pulsarBrokerRootUrl);
ProducerConfiguration config = new ProducerConfiguration();
config.setBatchingEnabled(true);
config.setSendTimeout(10, TimeUnit.SECONDS);
Producer producer = client.createProducer(topic, config);
```

### Message routing

When using {% popover partitioned topics %}, you can specify the routing mode whenever you publish messages using a {% popover producer %}. For more on specifying a routing mode using the Java client, see the [Partitioned Topics](../../advanced/PartitionedTopics) guide.

### Async send

You can publish messages [asynchronously](../../getting-started/ConceptsAndArchitecture#send-modes) using the Java client. With async send, the producer will put the message in a blocking queue and return immediately. The client library will then send the message to the {% popover broker %} in the background. If the queue is full (max size configurable), the producer could be blocked or fail immediately when calling the API, depending on arguments passed to the producer.

Here's an example async send operation:

```java
CompletableFuture<MessageId> future = producer.sendAsync("my-async-message".getBytes());
```

Async send operations return a {% javadoc MessageId client org.apache.pulsar.client.api.MessageId %} wrapped in a [`CompletableFuture`](http://www.baeldung.com/java-completablefuture).

## Consumers

In Pulsar, {% popover consumers %} subscribe to {% popover topics %} and handle {% popover messages %} that {% popover producers %} publish to those topics. You can instantiate a new {% popover consumer %} by first instantiating a {% javadoc PulsarClient client org.apache.pulsar.client.api.PulsarClient %}, passing it a URL for a Pulsar {% popover broker %} (we'll use the `client` object from the producer example above).

Once you've instantiated a {% javadoc PulsarClient client org.apache.pulsar.client.api.PulsarClient %} object, you can create a {% javadoc Consumer client org.apache.pulsar.client.api.Consumer %} for a {% popover topic %}. You also need to supply a {% popover subscription %} name.

```java
String topic = "persistent://sample/standalone/ns1/my-topic"; // from above
String subscription = "my-subscription";
Consumer consumer = client.subscribe(topic, subscription);
```

You can then use the `receive` method to listen for messages on the topic. This `while` loop sets up a long-running listener for the `persistent://sample/standalone/ns1/my-topic` topic, prints the contents of any message that's received, and then {% popover acknowledges %} that the message has been processed:

```java
while (true) {
  // Wait for a message
  Message msg = consumer.receive();

  System.out.println("Received message: " + msg.getData());

  // Acknowledge the message so that it can be deleted by broker
  consumer.acknowledge(msg);
}
```

### Configuring consumers

If you instantiate a `Consumer` object specifying only a topic and subscription name, as in the example above, the consumer will use the default configuration. To use a non-default configuration, you can instantiate the `Consumer` with a {% javadoc ConsumerConfiguration client org.apache.pulsar.client.api.ConsumerConfiguration %} object as well.

Here's an example configuration:

```java
PulsarClient client = PulsarClient.create(pulsarBrokerRootUrl);
ConsumerConfiguration config = new ConsumerConfiguration();
config.setSubscriptionType(SubscriptionType.Shared);
config.setReceiverQueueSize(10);
Consumer consumer = client.createConsumer(topic, config);
```

### Async receive

The `receive` method will receive messages synchronously (the consumer process will be blocked until a message is available). You can also use [async receive](../../getting-started/ConceptsAndArchitecture#receive-modes), which will return immediately with a [`CompletableFuture`](http://www.baeldung.com/java-completablefuture) object that completes once a new message is available.

Here's an example:

```java
CompletableFuture<Message> asyncMessage = consumer.receiveAsync();
```

Async receive operations return a {% javadoc Message client org.apache.pulsar.client.api.Message %} wrapped in a [`CompletableFuture`](http://www.baeldung.com/java-completablefuture).

## Reader interface

With the [reader interface](../../getting-started/ConceptsAndArchitecture#reader-interface), Pulsar clients can "manually position" themselves within a topic, reading all messages from a specified message onward. The Pulsar API for Java enables you to create  {% javadoc Reader client org.apache.pulsar.client.api.Reader %} objects by specifying a {% popover topic %}, a {% javadoc MessageId client org.apache.pulsar.client.api.MessageId %}, and {% javadoc ReaderConfiguration client org.apache.pulsar.client.api.ReaderConfiguration %}.

Here's an example:

```java
ReaderConfiguration conf = new ReaderConfiguration();
byte[] msgIdBytes = // Some message ID byte array
MessageId id = MessageId.fromByteArray(msgIdBytes);
Reader reader = pulsarClient.createReader(topic, id, conf);

while (true) {
    Message message = reader.readNext();
    // Process message
}
```

In the example above, a `Reader` object is instantiated for a specific topic and message (by ID); the reader then iterates over each message in the topic after the message identified by `msgIdBytes` (how that value is obtained depends on the application).

The code sample above shows pointing the `Reader` object to a specific message (by ID), but you can also use `MessageId.earliest` to point to the earliest available message on the topic of `MessageId.latest` to point to the most recent available message.

## Authentication

Pulsar currently supports two authentication schemes: [TLS](../../admin/Authz#tls-client-auth) and [Athenz](../../admin/Authz#athenz). The Pulsar Java client can be used with both.

### TLS Authentication

To use [TLS](../../admin/Authz#tls-client-auth), you need to set TLS to `true` using the `setUseTls` method, point your Pulsar client to a TLS cert path, and provide paths to cert and key files.

Here's an example configuration:

```java
ClientConfiguration conf = new ClientConfiguration();
conf.setUseTls(true);
conf.setTlsTrustCertsFilePath("/path/to/cacert.pem");

Map<String, String> authParams = new HashMap<>();
authParams.put("tlsCertFile", "/path/to/client-cert.pem");
authParams.put("tlsKeyFile", "/path/to/client-key.pem");
conf.setAuthentication(AuthenticationTls.class.getName(), authParams);

PulsarClient client = PulsarClient.create("pulsar+ssl://my-broker.com:6651", conf);
```

### Athenz

To use [Athenz](../../admin/Authz#athenz) as an authentication provider, you need to [use TLS](#tls-authentication) and provide values for four parameters in a hash:

* `tenantDomain`
* `tenantService`
* `providerDomain`
* `privateKey`

You can also set an optional `keyId`. Here's an example configuration:

```java
ClientConfiguration conf = new ClientConfiguration();

// Enable TLS
conf.setUseTls(true);
conf.setTlsTrustCertsFilePath("/path/to/cacert.pem");

// Set Athenz auth plugin and its parameters
Map<String, String> authParams = new HashMap<>();
authParams.put("tenantDomain", "shopping"); // Tenant domain name
authParams.put("tenantService", "some_app"); // Tenant service name
authParams.put("providerDomain", "pulsar"); // Provider domain name
authParams.put("privateKey", "file:///path/to/private.pem"); // Tenant private key path
authParams.put("keyId", "v1"); // Key id for the tenant private key (optional, default: "0")
conf.setAuthentication(AuthenticationAthenz.class.getName(), authParams);

PulsarClient client = PulsarClient.create(
        "pulsar+ssl://my-broker.com:6651", conf);
```

{% include admonition.html type="info" title="Supported pattern formats"
content='
The `privateKey` parameter supports the following three pattern formats:

* `file:///path/to/file`
* `file:/path/to/file`
* `data:application/x-pem-file;base64,<base64-encoded value>`' %}

