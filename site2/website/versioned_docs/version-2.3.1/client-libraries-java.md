---
id: version-2.3.1-client-libraries-java
title: The Pulsar Java client
sidebar_label: Java
original_id: client-libraries-java
---

The Pulsar Java client can be used both to create Java producers, consumers, and [readers](#reader-interface) of messages and to perform [administrative tasks](admin-api-overview.md). The current version of the Java client is **{{pulsar:version}}**.

Javadoc for the Pulsar client is divided up into two domains, by package:

Package | Description | Maven Artifact
:-------|:------------|:--------------
[`org.apache.pulsar.client.api`](/api/client) | The producer and consumer API | [org.apache.pulsar:pulsar-client:{{pulsar:version}}](http://search.maven.org/#artifactdetails%7Corg.apache.pulsar%7Cpulsar-client%7C{{pulsar:version}}%7Cjar)
[`org.apache.pulsar.client.admin`](/api/admin) | The Java [admin API](admin-api-overview.md) | [org.apache.pulsar:pulsar-client-admin:{{pulsar:version}}](http://search.maven.org/#artifactdetails%7Corg.apache.pulsar%7Cpulsar-client-admin%7C{{pulsar:version}}%7Cjar)

This document will focus only on the client API for producing and consuming messages on Pulsar topics. For a guide to using the Java admin client, see [The Pulsar admin interface](admin-api-overview.md).

## Installation

The latest version of the Pulsar Java client library is available via [Maven Central](http://search.maven.org/#artifactdetails%7Corg.apache.pulsar%7Cpulsar-client%7C{{pulsar:version}}%7Cjar). To use the latest version, add the `pulsar-client` library to your build configuration.

### Maven

If you're using Maven, add this to your `pom.xml`:

```xml
<!-- in your <properties> block -->
<pulsar.version>{{pulsar:version}}</pulsar.version>

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
def pulsarVersion = '{{pulsar:version}}'

dependencies {
    compile group: 'org.apache.pulsar', name: 'pulsar-client', version: pulsarVersion
}
```

## Connection URLs

To connect to Pulsar using client libraries, you need to specify a [Pulsar protocol](developing-binary-protocol.md) URL.

Pulsar protocol URLs are assigned to specific clusters, use the `pulsar` scheme and have a default port of 6650. Here's an example for `localhost`:

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

## Client configuration

You can instantiate a {@inject: javadoc:PulsarClient:/client/org/apache/pulsar/client/api/PulsarClient} object using just a URL for the target Pulsar [cluster](reference-terminology.md#cluster), like this:

```java
PulsarClient client = PulsarClient.builder()
        .serviceUrl("pulsar://localhost:6650")
        .build();
```

> #### Default broker URLs for standalone clusters
> If you're running a cluster in [standalone mode](getting-started-standalone.md), the broker will be available at the `pulsar://localhost:6650` URL by default.

Check out the Javadoc for the {@inject: javadoc:PulsarClient:/client/org/apache/pulsar/client/api/PulsarClient} class for a full listing of configurable parameters.

> In addition to client-level configuration, you can also apply [producer](#configuring-producers) and [consumer](#configuring-consumers) specific configuration, as you'll see in the sections below.


## Producers

In Pulsar, producers write messages to topics. Once you've instantiated a {@inject: javadoc:PulsarClient:/client/org/apache/pulsar/client/api/PulsarClient} object (as in the section [above](#client-configuration)), you can create a {@inject: javadoc:Producer:/client/org/apache/pulsar/client/api/Producer} for a specific Pulsar [topic](reference-terminology.md#topic).

```java
Producer<byte[]> producer = client.newProducer()
        .topic("my-topic")
        .create();

// You can then send messages to the broker and topic you specified:
producer.send("My message".getBytes());
```

By default, producers produce messages that consist of byte arrays. You can produce different types, however, by specifying a message [schema](#schemas).

```java
Producer<String> stringProducer = client.newProducer(Schema.STRING)
        .topic("my-topic")
        .create();
stringProducer.send("My message");
```

> You should always make sure to close your producers, consumers, and clients when they are no longer needed:
> ```java
> producer.close();
> consumer.close();
> client.close();
> ```
>
> Close operations can also be asynchronous:
> ```java
> producer.closeAsync()
>    .thenRun(() -> System.out.println("Producer closed"));
>    .exceptionally((ex) -> {
>        System.err.println("Failed to close producer: " + ex);
>        return ex;
>    });
> ```

### Configuring producers

If you instantiate a `Producer` object specifying only a topic name, as in the example above, the producer will use the default configuration. To use a non-default configuration, there's a variety of configurable parameters that you can set. For a full listing, see the Javadoc for the {@inject: javadoc:ProducerBuilder:/client/org/apache/pulsar/client/api/ProducerBuilder} class. Here's an example:

```java
Producer<byte[]> producer = client.newProducer()
    .topic("my-topic")
    .batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS)
    .sendTimeout(10, TimeUnit.SECONDS)
    .blockIfQueueFull(true)
    .create();
```

### Message routing

When using partitioned topics, you can specify the routing mode whenever you publish messages using a producer. For more on specifying a routing mode using the Java client, see the [Partitioned Topics](cookbooks-partitioned.md) cookbook.

### Async send

You can also publish messages [asynchronously](concepts-messaging.md#send-modes) using the Java client. With async send, the producer will put the message in a blocking queue and return immediately. The client library will then send the message to the broker in the background. If the queue is full (max size configurable), the producer could be blocked or fail immediately when calling the API, depending on arguments passed to the producer.

Here's an example async send operation:

```java
producer.sendAsync("my-async-message".getBytes()).thenAccept(msgId -> {
    System.out.printf("Message with ID %s successfully sent", msgId);
});
```

As you can see from the example above, async send operations return a {@inject: javadoc:MessageId:/client/org/apache/pulsar/client/api/MessageId} wrapped in a [`CompletableFuture`](http://www.baeldung.com/java-completablefuture).

### Configuring messages

In addition to a value, it's possible to set additional items on a given message:

```java
producer.newMessage()
    .key("my-message-key")
    .value("my-async-message".getBytes())
    .property("my-key", "my-value")
    .property("my-other-key", "my-other-value")
    .send();
```

As for the previous case, it's also possible to terminate the builder chain with `sendAsync()` and
get a future returned.

## Consumers

In Pulsar, consumers subscribe to topics and handle messages that producers publish to those topics. You can instantiate a new [consumer](reference-terminology.md#consumer) by first instantiating a {@inject: javadoc:PulsarClient:/client/org/apache/pulsar/client/api/PulsarClient} object and passing it a URL for a Pulsar broker (as [above](#client-configuration)).

Once you've instantiated a {@inject: javadoc:PulsarClient:/client/org/apache/pulsar/client/api/PulsarClient} object, you can create a {@inject: javadoc:Consumer:/client/org/apache/pulsar/client/api/Consumer} by specifying a [topic](reference-terminology.md#topic) and a [subscription](concepts-messaging.md#subscription-modes).

```java
Consumer consumer = client.newConsumer()
        .topic("my-topic")
        .subscriptionName("my-subscription")
        .subscribe();
```

The `subscribe` method will automatically subscribe the consumer to the specified topic and subscription. One way to make the consumer listen on the topic is to set up a `while` loop. In this example loop, the consumer listens for messages, prints the contents of any message that's received, and then [acknowledges](reference-terminology.md#acknowledgment-ack) that the message has been processed. If the processing logic fails, we use [negative acknowledgement](reference-terminology.md#acknowledgment-ack)
to have the message redelivered at a later point in time.

```java
while (true) {
  // Wait for a message
  Message msg = consumer.receive();

  try {
      // Do something with the message
      System.out.printf("Message received: %s", new String(msg.getData()));

      // Acknowledge the message so that it can be deleted by the message broker
      consumer.acknowledge(msg);
  } catch (Exception e) {
      // Message failed to process, redeliver later
      consumer.negativeAcknowledge(msg);
  }
}
```

### Configuring consumers

If you instantiate a `Consumer` object specifying only a topic and subscription name, as in the example above, the consumer will use the default configuration. To use a non-default configuration, there's a variety of configurable parameters that you can set. For a full listing, see the Javadoc for the {@inject: javadoc:ConsumerBuilder:/client/org/apache/pulsar/client/api/ConsumerBuilder} class. Here's an example:

Here's an example configuration:

```java
Consumer consumer = client.newConsumer()
        .topic("my-topic")
        .subscriptionName("my-subscription")
        .ackTimeout(10, TimeUnit.SECONDS)
        .subscriptionType(SubscriptionType.Exclusive)
        .subscribe();
```

### Async receive

The `receive` method will receive messages synchronously (the consumer process will be blocked until a message is available). You can also use [async receive](concepts-messaging.md#receive-modes), which will return immediately with a [`CompletableFuture`](http://www.baeldung.com/java-completablefuture) object that completes once a new message is available.

Here's an example:

```java
CompletableFuture<Message> asyncMessage = consumer.receiveAsync();
```

Async receive operations return a {@inject: javadoc:Message:/client/org/apache/pulsar/client/api/Message} wrapped inside of a [`CompletableFuture`](http://www.baeldung.com/java-completablefuture).

### Multi-topic subscriptions

In addition to subscribing a consumer to a single Pulsar topic, you can also subscribe to multiple topics simultaneously using [multi-topic subscriptions](concepts-messaging.md#multi-topic-subscriptions). To use multi-topic subscriptions you can supply either a regular expression (regex) or a `List` of topics. If you select topics via regex, all topics must be within the same Pulsar namespace.

Here are some examples:

```java
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClient;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

ConsumerBuilder consumerBuilder = pulsarClient.newConsumer()
        .subscriptionName(subscription);

// Subscribe to all topics in a namespace
Pattern allTopicsInNamespace = Pattern.compile("persistent://public/default/.*");
Consumer allTopicsConsumer = consumerBuilder
        .topicsPattern(allTopicsInNamespace)
        .subscribe();

// Subscribe to a subsets of topics in a namespace, based on regex
Pattern someTopicsInNamespace = Pattern.compile("persistent://public/default/foo.*");
Consumer allTopicsConsumer = consumerBuilder
        .topicsPattern(someTopicsInNamespace)
        .subscribe();
```

You can also subscribe to an explicit list of topics (across namespaces if you wish):

```java
List<String> topics = Arrays.asList(
        "topic-1",
        "topic-2",
        "topic-3"
);

Consumer multiTopicConsumer = consumerBuilder
        .topics(topics)
        .subscribe();

// Alternatively:
Consumer multiTopicConsumer = consumerBuilder
        .topics(
            "topic-1",
            "topic-2",
            "topic-3"
        )
        .subscribe();
```

You can also subscribe to multiple topics asynchronously using the `subscribeAsync` method rather than the synchronous `subscribe` method. Here's an example:

```java
Pattern allTopicsInNamespace = Pattern.compile("persistent://public/default.*");
consumerBuilder
        .topics(topics)
        .subscribeAsync()
        .thenAccept(consumer -> {
            do {
                try {
                    Message msg = consumer.receive();
                    // Do something with the received message
                } catch (PulsarClientException e) {
                    e.printStackTrace();
                }
            } while (true);
        });
```

## Reader interface

With the [reader interface](concepts-clients.md#reader-interface), Pulsar clients can "manually position" themselves within a topic, reading all messages from a specified message onward. The Pulsar API for Java enables you to create  {@inject: javadoc:Reader:/client/org/apache/pulsar/client/api/Reader} objects by specifying a topic, a {@inject: javadoc:MessageId:/client/org/apache/pulsar/client/api/MessageId}, and {@inject: javadoc:ReaderConfiguration:/client/org/apache/pulsar/client/api/ReaderConfiguration}.

Here's an example:

```java
ReaderConfiguration conf = new ReaderConfiguration();
byte[] msgIdBytes = // Some message ID byte array
MessageId id = MessageId.fromByteArray(msgIdBytes);
Reader reader = pulsarClient.newReader()
        .topic(topic)
        .startMessageId(id)
        .create();

while (true) {
    Message message = reader.readNext();
    // Process message
}
```

In the example above, a `Reader` object is instantiated for a specific topic and message (by ID); the reader then iterates over each message in the topic after the message identified by `msgIdBytes` (how that value is obtained depends on the application).

The code sample above shows pointing the `Reader` object to a specific message (by ID), but you can also use `MessageId.earliest` to point to the earliest available message on the topic of `MessageId.latest` to point to the most recent available message.

## Schemas

In Pulsar, all message data consists of byte arrays "under the hood." [Message schemas](concepts-schema-registry.md) enable you to use other types of data when constructing and handling messages (from simple types like strings to more complex, application-specific types). If you construct, say, a [producer](#producers) without specifying a schema, then the producer can only produce messages of type `byte[]`. Here's an example:

```java
Producer<byte[]> producer = client.newProducer()
        .topic(topic)
        .create();
```

The producer above is equivalent to a `Producer<byte[]>` (in fact, you should *always* explicitly specify the type). If you'd like to use a producer for a different type of data, you'll need to specify a **schema** that informs Pulsar which data type will be transmitted over the [topic](reference-terminology.md#topic).

### Schema example

Let's say that you have a `SensorReading` class that you'd like to transmit over a Pulsar topic:

```java
public class SensorReading {
    public float temperature;

    public SensorReading(float temperature) {
        this.temperature = temperature;
    }

    // A no-arg constructor is required
    public SensorReading() {
    }

    public float getTemperature() {
        return temperature;
    }

    public void setTemperature(float temperature) {
        this.temperature = temperature;
    }
}
```

You could then create a `Producer<SensorReading>` (or `Consumer<SensorReading>`) like so:

```java
Producer<SensorReading> producer = client.newProducer(JSONSchema.of(SensorReading.class))
        .topic("sensor-readings")
        .create();
```

The following schema formats are currently available for Java:

* No schema or the byte array schema (which can be applied using `Schema.BYTES`):

  ```java
  Producer<byte[]> bytesProducer = client.newProducer(Schema.BYTES)
        .topic("some-raw-bytes-topic")
        .create();
  ```

  Or, equivalently:

  ```java
  Producer<byte[]> bytesProducer = client.newProducer()
        .topic("some-raw-bytes-topic")
        .create();
  ```

* `String` for normal UTF-8-encoded string data. This schema can be applied using `Schema.STRING`:

  ```java
  Producer<String> stringProducer = client.newProducer(Schema.STRING)
        .topic("some-string-topic")
        .create();
  ```
* JSON schemas can be created for POJOs using the `JSONSchema` class. Here's an example:

  ```java
  Schema<MyPojo> pojoSchema = JSONSchema.of(MyPojo.class);
  Producer<MyPojo> pojoProducer = client.newProducer(pojoSchema)
        .topic("some-pojo-topic")
        .create();
  ```

## Authentication

Pulsar currently supports two authentication schemes: [TLS](security-tls-authentication.md) and [Athenz](security-athenz.md). The Pulsar Java client can be used with both.

### TLS Authentication

To use [TLS](security-tls-authentication.md), you need to set TLS to `true` using the `setUseTls` method, point your Pulsar client to a TLS cert path, and provide paths to cert and key files.

Here's an example configuration:

```java
Map<String, String> authParams = new HashMap<>();
authParams.put("tlsCertFile", "/path/to/client-cert.pem");
authParams.put("tlsKeyFile", "/path/to/client-key.pem");

Authentication tlsAuth = AuthenticationFactory
        .create(AuthenticationTls.class.getName(), authParams);

PulsarClient client = PulsarClient.builder()
        .serviceUrl("pulsar+ssl://my-broker.com:6651")
        .enableTls(true)
        .tlsTrustCertsFilePath("/path/to/cacert.pem")
        .authentication(tlsAuth)
        .build();
```

### Athenz

To use [Athenz](security-athenz.md) as an authentication provider, you need to [use TLS](#tls-authentication) and provide values for four parameters in a hash:

* `tenantDomain`
* `tenantService`
* `providerDomain`
* `privateKey`

You can also set an optional `keyId`. Here's an example configuration:

```java
Map<String, String> authParams = new HashMap<>();
authParams.put("tenantDomain", "shopping"); // Tenant domain name
authParams.put("tenantService", "some_app"); // Tenant service name
authParams.put("providerDomain", "pulsar"); // Provider domain name
authParams.put("privateKey", "file:///path/to/private.pem"); // Tenant private key path
authParams.put("keyId", "v1"); // Key id for the tenant private key (optional, default: "0")

Authentication athenzAuth = AuthenticationFactory
        .create(AuthenticationAthenz.class.getName(), authParams);

PulsarClient client = PulsarClient.builder()
        .serviceUrl("pulsar+ssl://my-broker.com:6651")
        .enableTls(true)
        .tlsTrustCertsFilePath("/path/to/cacert.pem")
        .authentication(athenzAuth)
        .build();
```

> #### Supported pattern formats
> The `privateKey` parameter supports the following three pattern formats:
> * `file:///path/to/file`
> * `file:/path/to/file`
> * `data:application/x-pem-file;base64,<base64-encoded value>`
