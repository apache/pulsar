---
id: schema-overview
title: Overview
sidebar_label: "Overview"
---

This section introduces the following content:
* [What is Pulsar Schema](#what-is-pulsar-schema)
* [Why use it](#why-use-it)
* [How it works](#how-it-works)
* [Use case](#use-case)
* [What's next?](#whats-next)

## What is Pulsar Schema

Pulsar messages are stored as unstructured byte arrays and the data structure (as known as schema) is applied to this data only when it's read. So both the producer and consumer need to agree upon the data structure of the messages, including the fields and their associated types.

Pulsar schema is the metadata that defines how to translate the raw message bytes into a more formal structure type, serving as a protocol between the applications that generate messages and the applications that consume them. It serializes data into raw bytes before they are published to a topic and deserializes the raw bytes before they are delivered to consumers.

Pulsar uses a schema registry as a central repository to store the registered schema information, which enables producers/consumers to coordinate the schema of a topic’s messages through brokers.

![Pulsar schema](/assets/schema.svg)

:::note

Currently, Pulsar schema is available for [Java clients](client-libraries-java.md), [Go clients](client-libraries-go.md), [Python clients](client-libraries-python.md), [C++ clients](client-libraries-cpp.md), and [C# clients](client-libraries-dotnet.md).

:::

## Why use it

Type safety is extremely important in any application built around a messaging and streaming system. Raw bytes are flexible for data transfer, but the flexibility and neutrality come with a cost: you have to overlay data type checking and serialization/deserialization to ensure that the bytes fed into the system can be read and successfully consumed. In other words, you need to make sure the data is intelligible and usable to applications.

Pulsar schema resolves the pain points with the following capabilities:
* enforces the data type safety when a topic has a schema defined. As a result, producers/consumers are only allowed to connect if they are using a "compatible" schema.
* provides a central location for storing information about the schemas used within your organization, in turn greatly simplifies the sharing of this information across application teams.
* serves as a single source of truth for all the message schemas used across all your services and development teams, which makes it easier for them to collaborate.
* keeps data compatibility on-track between schema versions. When new schemas are uploaded, the new versions can be read by old consumers. 
* stored in the existing storage layer BookKeeper, without additional system required.

## How it works

Pulsar schemas are applied and enforced at the **topic** level. Producers and consumers can upload schemas to brokers, so Pulsar schemas work on both sides.

### Producer side

This diagram illustrates how Pulsar schema works on the Producer side.

![How Pulsar schema works on the producer side](/assets/schema-producer.svg)

1. The application uses a schema instance to construct a producer instance. 
   The schema instance defines the schema for the data being produced using the producer instance. Take Avro as an example, Pulsar extracts the schema definition from the POJO class and constructs the `SchemaInfo`.

2. The producer requests to connect to the broker with the `SchemaInfo` extracted from the passed-in schema instance.
   
3. The broker looks up the schema registry to check if it is a registered schema. 
   * If the schema is registered, the broker returns the schema version to the producer.
   * Otherwise, go to step 4.

4. The broker checks whether the schema can be auto-updated. 
   * If it’s not allowed to be auto-updated, then the schema cannot be registered, and the broker rejects the producer.
   * Otherwise, go to step 5.

5. The broker performs the [schema compatibility check](schema-understand.md#schema-compatibility-check) defined for the topic.
   * If the schema passes the compatibility check, the broker stores it in the schema registry and returns the schema version to the producer. All the messages produced by this producer are tagged with the schema version. 
   * Otherwise, the broker rejects the producer.

### Consumer side

This diagram illustrates how schema works on the consumer side. 

![How Pulsar schema works on the consumer side](/assets/schema-consumer.svg)

1. The application uses a schema instance to construct a consumer instance.

2. The consumer connects to the broker with the `SchemaInfo` extracted from the passed-in schema instance.

3. The broker checks if the topic is in use (has at least one of the objects: schema, data, active producer or consumer).
   * If a topic has at least one of the above objects, go to step 5.
   * Otherwise, go to step 4.

4. The broker checks whether the schema can be auto-updated.
     * If the schema can be auto-updated, the broker registers the schema and connects the consumer.
     * Otherwise, the broker rejects the consumer.
       
5. The broker performs the [schema compatibility check](schema-understand.md#schema-compatibility-check).
     * If the schema passes the compatibility check, the broker connects the consumer.
     * Otherwise, the broker rejects the consumer. 

## Use case

You can use language-specific types of data when constructing and handling messages from simple data types like `string` to more complex application-specific types.

For example, you are using the _User_ class to define the messages sent to Pulsar topics.

```java
public class User {
   public String name;
   public int age;
   
   User() {}
   
   User(String name, int age) {
      this.name = name;
      this.age = age;
   }
}
```

**Without a schema**

If you construct a producer without specifying a schema, then the producer can only produce messages of type `byte[]`. If you have a POJO class, you need to serialize the POJO into bytes before sending messages.

```java
Producer<byte[]> producer = client.newProducer()
        .topic(topic)
        .create();
User user = new User("Tom", 28);
byte[] message = … // serialize the `user` by yourself;
producer.send(message);
```

**With a schema**

This example constructs a producer with the _JSONSchema_, and you can send the _User_ class to topics directly without worrying about how to serialize POJOs into bytes.

```java
// send with json schema
Producer<User> producer = client.newProducer(JSONSchema.of(User.class))
        .topic(topic)
        .create();
User user = new User("Tom", 28);
producer.send(user);

// receive with json schema
Consumer<User> consumer = client.newConsumer(JSONSchema.of(User.class))
   .topic(schemaTopic)
   .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
   .subscriptionName("schema-sub")
   .subscribe();
Message<User> message = consumer.receive();
User user = message.getValue();
assert user.age == 28 && user.name.equals("Tom");
```

## What's next?

* [Understand schema concepts](schema-understand.md)
* [Get started with schema](schema-get-started.md)
* [Manage schema](admin-api-schemas.md)
