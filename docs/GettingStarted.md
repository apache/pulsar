
# Getting started with Pulsar

<!-- TOC depthFrom:2 depthTo:4 withLinks:1 updateOnSave:1 orderedList:0 -->

- [Basic concepts](#basic-concepts)
	- [Topic name](#topic-name)
	- [Subscription modes](#subscription-modes)
- [Starting a standalone Pulsar server](#starting-a-standalone-pulsar-server)
- [Using the Pulsar Java client API](#using-the-pulsar-java-client-api)
	- [Consumer](#consumer)
	- [Producer](#producer)
- [Using the Pulsar C++ client API](#using-the-pulsar-c-client-api)
	- [Consumer](#consumer)
	- [Producer](#producer)

<!-- /TOC -->

## Basic concepts

Pulsar is a messaging system built on the pub-sub paradigm. The **topic**
is the key resource to connect **producers** and **consumers**.

A producer can connect to a topic and publish messages. A consumer can
**subscribe** to a topic and receive messages.

Once a subscription has been created, all messages will be *retained* by
the system, even if the consumer gets disconnected, until a consumer will
**acknowledge** their successful processing.

### Topic name

A topic name will look like:
```
persistent://my-property/us-west/my-namespace/my-topic
```

The topic name structure is linked to the multi-tenant nature of Pulsar.
In this example:
 * `persistent` → Identifies a topic where all messages are durably persisted
    on multiple disks. This is the only supported type of topic at this point
 * `my-property` → **Property** identifies a *tenant* in the Pulsar
    instance
 * `us-west` → **Cluster** where the topic is located. Typically there
    will be a cluster for each geographical region or data-center
 * `my-namespace` → **Namespace** is the administrative unit and it
    represents a group of related topics. Most of the configuration
    is done at the namespace level. Each property can have multiple
    namespaces
 * `my-topic` → Final part of topic name. It's free form and has no
    special meaning to the system

### Subscription modes

Each topic can have multiple **subscriptions**, each with a different
subscription name and subscriptions can be of different types:

 * **Exclusive** → Only one consumer is allowed to attach to the
  subscription. Ordering is guaranteed.
 * **Shared** → Multiple consumers can connect to the same subscription
   and messages are delivered in round-robin across available consumers.
	 Messages ordering can be rearranged.
 * **Failover** → Only one consumer will be actively receive messages,
   while other consumer will be on standby. Ordering is guaranteed.

For a more detailed explanation, refer to [Architecture](Architecture.md)
page.

## Getting the software

Download latest binary release from

```
https://github.com/apache/incubator-pulsar/releases/latest
```

```shell
$ tar xvfz pulsar-X.Y-bin.tar.gz
$ cd pulsar-X.Y
```

## Starting a standalone Pulsar server

For application development or to quickly setup a working service,
we can use the Pulsar standalone mode. In this mode, we'll start
a broker, ZooKeeper and BookKeeper components inside a single JVM
process.

```shell
$ bin/pulsar standalone
```

The Pulsar service is now ready to use and we can point
clients to use service URL as either `http://localhost:8080/` or `pulsar://localhost:6650`

A sample namespace, `sample/standalone/ns1`, is already available.

## Using the Pulsar Java client API

Include dependency for Pulsar client library.

Latest version is [![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.apache.pulsar/pulsar-client/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.apache.pulsar/pulsar-client)

```xml
<dependency>
  <groupId>org.apache.pulsar</groupId>
  <artifactId>pulsar-client</artifactId>
  <version>${pulsar.version}</version>
</dependency>
```

### Consumer

```java
PulsarClient client = PulsarClient.create("pulsar://localhost:6650");

Consumer consumer = client.subscribe(
            "persistent://sample/standalone/ns1/my-topic",
            "my-subscribtion-name");

while (true) {
  // Wait for a message
  Message msg = consumer.receive();

  System.out.println("Received message: " + msg.getData());

  // Acknowledge the message so that it can be deleted by broker
  consumer.acknowledge(msg);
}

client.close();
```


### Producer

```java
PulsarClient client = PulsarClient.create("pulsar://localhost:6650");

Producer producer = client.createProducer(
            "persistent://sample/standalone/ns1/my-topic");

// Publish 10 messages to the topic
for (int i = 0; i < 10; i++) {
    producer.send("my-message".getBytes());
}

client.close();
```

## Using the Pulsar C++ client API

Build instructions are in [pulsar-client-cpp/README.md](../pulsar-client-cpp/README.md).

### Consumer

```cpp
Client client("pulsar://localhost:6650");

Consumer consumer;
Result result = client.subscribe("persistent://sample/standalone/ns1/my-topic", "my-subscribtion-name", consumer);
if (result != ResultOk) {
    LOG_ERROR("Failed to subscribe: " << result);
    return -1;
}

Message msg;

while (true) {
    consumer.receive(msg);
    LOG_INFO("Received: " << msg << "  with payload '" << msg.getDataAsString() << "'");

    consumer.acknowledge(msg);
}

client.close();
```


### Producer

```cpp
Client client("pulsar://localhost:6650");

Producer producer;
Result result = client.createProducer("persistent://sample/standalone/ns1/my-topic", producer);
if (result != ResultOk) {
    LOG_ERROR("Error creating producer: " << result);
    return -1;
}

// Publish 10 messages to the topic
for(int i=0;i<10;i++){
    Message msg = MessageBuilder().setContent("my-message").build();
    Result res = producer.send(msg);
    LOG_INFO("Message sent: " << res);
}
client.close();
```
