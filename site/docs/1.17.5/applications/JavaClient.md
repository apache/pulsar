---
title: The Pulsar Java client
tags: [client, java]
---

The Pulsar Java client can be used both to create Java {% popover producers %} and {% popover consumers %} of messages but also to perform [administrative tasks](../../admin/AdminInterface).

The current version of the Java client is **{{ site.current_version }}**.

Javadoc for the Pulsar client is divided up into three domains, by package:

Package | Description
:-------|:-----------
[`com.yahoo.pulsar.broker`]({{ site.baseurl }}/api/broker) | {% popover Broker %}-specific tasks
[`com.yahoo.pulsar.client.api`]({{ site.baseurl }}/api/client) | The {% popover producer %} and {% popover consumer %} API
[`com.yahoo.pulsar.client.admin`]({{ site.baseurl }}/api/admin) | The Java [admin API](../../admin/AdminInterface)

This document will focus only on the client API for producing and consuming messages on Pulsar {% popover topics %}.

## Installation

The latest version of the Pulsar Java client library is available via [Maven Central](http://search.maven.org/#artifactdetails%7Ccom.yahoo.pulsar%7Cpulsar-client%7C{{ site.current_version }}%7Cjar). To use the latest version, add the `pulsar-client` library to your build configuration.

### Maven

If you're using Maven, add this to your `pom.xml`:

```xml
<!-- in your <properties> block -->
<pulsar.version>{{ site.current_version }}</pulsar.version>

<!-- in your <dependencies> block -->
<dependency>
  <groupId>com.yahoo.pulsar</groupId>
  <artifactId>pulsar-client</artifactId>
  <version>${pulsar.version}</version>
</dependency>
```

### Gradle

If you're using Gradle, add this to your `build.gradle` file:

```groovy
dependencies {
    compile group: 'com.yahoo.pulsar', name: 'pulsar-client', version: '{{ site.current_version }}'
}
```

## Client configuration

You can instantiate a {% javadoc PulsarClient client com.yahoo.pulsar.client.api.PulsarClient %} object using just a URL for the target Pulsar {% popover broker %}, like this:

```java
String pulsarBrokerRootUrl = "pulsar://localhost:6650";
PulsarClient client = PulsarClient.create(pulsarBrokerRootUrl);
```

This object will use the default configuration.

```java
// Set the authentication provider to use in the Pulsar client instance
public void setAuthentication(Authentication authentication);
public void setAuthentication(String authPluginClassName, String authParamsString);
public void setAuthentication(String authPluginClassName, Map<String, String> authParams);

// Set the operation timeout(default: 30 seconds)
public void setOperationTimeout(int operationTimeout, TimeUnit unit);

// Set the number of threads to be used for handling connections to brokers (default: 1 thread)
public void setIoThreads(int numIoThreads);

// Set the number of threads to be used for message listeners(default: 1 thread)
public void setListenerThreads(int numListenerThreads);

// Sets the max number of connection that the client library will open to a single broker
public void setConnectionsPerBroker(int connectionsPerBroker);

// Configure whether to use TCP no-delay flag on the connection, to disable the Nagle algorithm
public void setUseTcpNoDelay(boolean useTcpNoDelay);
```

{% include admonition.html type="info" content="
In addition to client-level configuration, you can also apply [producer](#configuring-producers)- and [consumer](#configuring-consumers)-specific configuration.
" %}

## Producers

In Pulsar, {% popover producers %} point to a Pulsar {% popover broker %} and write {% popover messages %} to {% popover topics %}. You can instantiate a new popover producer  by first instantiating a {% javadoc PulsarClient client com.yahoo.pulsar.client.api.PulsarClient %}, passing it a URL for a Pulsar {% popover broker %}.

```java
String pulsarBrokerRootUrl = "pulsar://localhost:6650";
PulsarClient client = PulsarClient.create(pulsarBrokerRootUrl);
```

{% include admonition.html type='info' title='Default broker URLs for standalone clusters' content="
If you're running a cluster in [standalone mode](../getting-started/LocalCluster), the broker will be available at these two URLs by default:

* `pulsar://localhost:6650`
* `http://localhost:8080`" %}

Once you've instantiated a {% javadoc PulsarClient client com.yahoo.pulsar.client.api.PulsarClient %} object, you can create a {% javadoc Producer client com.yahoo.pulsar.client.api.Producer %} for a {% popover topic %}.

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
You should always make sure to close your producer, consumer, and client when no longer needed:

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

If you instantiate a `Producer` object specifying only a topic name, as in the example above, the producer will use the default configuration. To use a non-default configuration, you can instantiate the `Producer` with a {% javadoc ProducerConfiguration client com.yahoo.pulsar.client.api.ProducerConfiguration %} object as well. The following setters are available:

```java
// Set the send timeout (default: 30 seconds)
public void setSendTimeout(int sendTimeout, TimeUnit unit);

// Set the max size of the queue holding the messages pending to receive an acknowledgment from the broker.

public void setMaxPendingMessages(int maxPendingMessages);

// Set whether the Producer#send and Producer#sendAsync operations should block when the outgoing message queue is full.
public void setBlockIfQueueFull(boolean blockIfQueueFull);

// Set the message routing mode for the partitioned producer
public void setMessageRoutingMode(MessageRoutingMode messageRouteMode);

// Set the compression type for the producer.
public void setCompressionType(CompressionType compressionType);

// Set a custom message routing policy by passing an implementation of MessageRouter
public void setMessageRouter(MessageRouter messageRouter);

// Control whether automatic batching of messages is enabled for the producer, default: false.
public void setBatchingEnabled(boolean batchMessagesEnabled);

// Set the time period within which the messages sent will be batched, default: 10ms.
public void setBatchingMaxPublishDelay(long batchDelay, TimeUnit timeUnit);

// Set the maximum number of messages permitted in a batch, default: 1000.
public void setBatchingMaxMessages(int batchMessagesMaxMessagesPerBatch);
```

Here's a configuration example:

```java
PulsarClient client = PulsarClient.create(pulsarBrokerRootUrl);
ProducerConfiguration config = new ProducerConfiguration();
config.setIoThreads(10);
config.setUseTcpNoDelay(true);
Producer producer = client.createProducer(topic, config);
```

### Message routing

When using {% popover partitioned topics %}, you can specify the routing mode whenever you publish messages using a {% popover producer %}. For more on specifying a routing mode using the Java client, see the [Partitioned Topics](../../concerns/PartitionedTopics) guide.

### Async send

You can publish messages [asynchronously](../../getting-started/ConceptsAndArchitecture#send-modes) using the Java client. With async send, the producer will put the message in a blocking queue and return immediately. The client library will then send the message to the {% popover broker %} in the background. If the queue is full (max size configurable), the producer could be blocked or fail immediately when calling the API, depending on arguments passed to the producer.

Here's an example async send operation:

```java
producer.sendAsync("my-async-message".getBytes());
```

## Consumers

In Pulsar, {% popover consumers %} subscribe to {% popover topics %} and handle {% popover messages %} that {% popover producers %} publish to those topics. You can instantiate a new {% popover consumer %} by first instantiating a {% javadoc PulsarClient client com.yahoo.pulsar.client.api.PulsarClient %}, passing it a URL for a Pulsar {% popover broker %} (we'll use the `client` object from the producer example above).

Once you've instantiated a {% javadoc PulsarClient client com.yahoo.pulsar.client.api.PulsarClient %} object, you can create a {% javadoc Consumer client com.yahoo.pulsar.client.api.Consumer %} for a {% popover topic %}. You also need to supply a {% popover subscription %} name.

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

If you instantiate a `Consumer` object specifying only a topic and subscription name, as in the example above, the consumer will use the default configuration. To use a non-default configuration, you can instantiate the `Consumer` with a {% javadoc ConsumerConfiguration client com.yahoo.pulsar.client.api.ConsumerConfiguration %} object as well. The following setters are available:


```java
// Set the timeout for unacked messages, truncated to the nearest millisecond
public ConsumerConfiguration setAckTimeout(long ackTimeout, TimeUnit timeUnit);

// Select the subscription type to be used when subscribing to the topic
public ConsumerConfiguration setSubscriptionType(SubscriptionType subscriptionType);

// Sets a MessageListener for the consumer
public ConsumerConfiguration setMessageListener(MessageListener messageListener);

// Sets the size of the consumer receive queue
public ConsumerConfiguration setReceiverQueueSize(int receiverQueueSize);
```

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

## Authentication

Pulsar currently supports two authentication schemes: [TLS](../../admin/Authz#tls-authentication) and [Athenz](../../admin/Authz#athenz). The Pulsar Java client can be used with both.

### TLS Authentication

To use [TLS](../../admin/Authz#tls-authentication), you need to set TLS to `true` using the `setUseTls` method, point your Pulsar client to a TLS cert path, and provide paths to cert and key files. Here's an example configuration:

```java
ClientConfiguration conf = new ClientConfiguration();
conf.setUseTls(true);
conf.setTlsTrustCertsFilePath("/path/to/cacert.pem");

Map<String, String> authParams = new HashMap<>();
authParams.put("tlsCertFile", "/path/to/client-cert.pem");
authParams.put("tlsKeyFile", "/path/to/client-key.pem");
conf.setAuthentication(AuthenticationTls.class.getName(), authParams);

PulsarClient client = PulsarClient.create(
                        "pulsar+ssl://my-broker.com:6651", conf);
```

### Athenz

To use [Athenz](../../admin/Authz#athenz) as an authentication provider, you need to [use TLS](#tls-authentication) and provide values for four parameters in a hash:

* `tenantDomain`
* `tenantService`
* `providerDomain`
* `privateKeyPath`

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
authParams.put("privateKeyPath", "/path/to/private.pem"); // Tenant private key path
authParams.put("keyId", "v1"); // Key id for the tenant private key (optional, default: "0")
conf.setAuthentication(AuthenticationAthenz.class.getName(), authParams);

PulsarClient client = PulsarClient.create(
        "pulsar+ssl://my-broker.com:6651", conf);
```
