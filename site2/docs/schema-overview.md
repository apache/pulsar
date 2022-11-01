---
id: schema-overview
title: Pulsar Schema overview
sidebar_label: "Overview"
---

This section introduces the following content:
* [What is Pulsar Schema](#what-is-pulsar-schema)
* [Why use it](#why-use-it)
* [How it works](#how-it-works)
* [What's next?](#whats-next)

## What is Pulsar schema

Pulsar messages are stored as unstructured byte arrays and the structure is applied to this data only when it's read.

Within Pulsar, each message consists of two distinct parts:
* message payload is stored as raw bytes to provide maximum flexibility;
* a collection of user-defined properties are stored as key/value pairs. 

Pulsar has a built-in **schema registry** that enables producers/consumers to coordinate on the structure of a topic’s data through brokers and enables clients to upload schemas on a per-topic basis, without needing an additional serving layer for your metadata. 

Each schema is a data structure used to serialize the bytes before they are published to a topic, and to deserialize them before they are delivered to the consumers. These schemas dictate which data types are recognized as valid for that topic.

:::note

Currently, Pulsar schema is only available for the [Java client](client-libraries-java.md), [Go client](client-libraries-go.md), [Python client](client-libraries-python.md), and [C++ client](client-libraries-cpp.md).

:::

## Why use it

Type safety is extremely important in any application built around a message bus like Pulsar. Producers and consumers need some kind of mechanism for coordinating types at the topic level to avoid various potential problems arising. For example, serialization and deserialization issues.

Producers and consumers are responsible for not only serializing and deserializing messages (which consist of raw bytes) but also "knowing" which types are being transmitted via which topics. Producers and consumers can send and receive messages consisting of raw byte arrays and leave all types of safety enforcement to the application on an "out-of-band" basis. If a producer is sending temperature sensor data on a topic, consumers of that topic will run into trouble if they attempt to parse that data as moisture sensor readings.

With schema registry, producers and consumers inform the messaging system which data types can be transmitted via a topic. The messaging system enforces type safety and ensures that producers and consumers remain synced. The schema registry provides a central location for storing information about the schemas used within your organization, in turn greatly simplifies the sharing of this information across application teams. It serves as a single source of truth for all the message schemas used across all your services and development teams, which makes it easier for them to collaborate.

Having a central schema registry along with the consistent use of schemas across the organization also makes data consumption and discovery much easier. If you define a standard schema for a common business entity such as a customer, product, or order that almost all applications will use, then all message-producing applications will be able to generate messages in the latest format. Similarly, consuming applications won’t need to perform any transformations on the data in order to make it conform to a different format.

## Use case

When a schema is enabled, Pulsar does parse data. It takes bytes as inputs and sends bytes as outputs. While data has meaning beyond bytes, you need to parse data and might encounter parse exceptions which mainly occur in the following situations:
* The field does not exist.
* The field type has changed (for example, `string` is changed to `int`).

You can adopt the Pulsar schema registry to perform schema evolution, enforcing data type safety in the language you are using and not breaking downstream applications.

Pulsar schema enables you to use language-specific types of data when constructing and handling messages from simple types like `string` to more complex application-specific types. 

**Example** 

You can use the _User_ class to define the messages sent to Pulsar topics.

```java
public class User {
    String name;
    int age;
}
```

When constructing a producer with the _User_ class, you can specify a schema or not as below.

**Without schema**

If you construct a producer without specifying a schema, then the producer can only produce messages of type `byte[]`. If you have a POJO class, you need to serialize the POJO into bytes before sending messages.

```java
Producer<byte[]> producer = client.newProducer()
        .topic(topic)
        .create();
User user = new User("Tom", 28);
byte[] message = … // serialize the `user` by yourself;
producer.send(message);
```

**With schema**

If you construct a producer by specifying a schema, then you can send a class to a topic directly without worrying about how to serialize POJOs into bytes. 

This example constructs a producer with the _JSONSchema_, and you can send the _User_ class to topics directly without worrying about how to serialize it into bytes. 

```java
Producer<User> producer = client.newProducer(JSONSchema.of(User.class))
        .topic(topic)
        .create();
User user = new User("Tom", 28);
producer.send(user);
```

## How it works

Pulsar schemas are applied and enforced at the **topic** level (schemas cannot be applied at the namespace or tenant level). 

Producers and consumers upload schemas to brokers, so Pulsar schemas work on the producer side and the consumer side.

### Producer side

This diagram illustrates how schema works on the Producer side.

![Schema works at the producer side](/assets/schema-producer.png)

1. The application uses a schema instance to construct a producer instance. 

   The schema instance defines the schema for the data being produced using the producer instance. 

   Take AVRO as an example, Pulsar extracts schema definition from the POJO class and constructs the `SchemaInfo` that the producer needs to pass to a broker when it connects.

2. The producer connects to the broker with the `SchemaInfo` extracted from the passed-in schema instance.
   
3. The broker looks up the schema in the schema storage to check if it is already a registered schema. 
   
4. If yes, the broker skips the schema validation since it is a known schema, and returns the schema version to the producer.

5. If no, the broker verifies whether a schema can be automatically created in this namespace:

   * If `isAllowAutoUpdateSchema` sets to **true**, then a schema can be created, and the broker validates the schema based on the schema compatibility check strategy defined for the topic.
  
   * If `isAllowAutoUpdateSchema` sets to **false**, then a schema can not be created, and the producer is rejected to connect to the broker.
  
   :::tip

   `isAllowAutoUpdateSchema` can be set via **Pulsar admin API** or **REST API.** 

   For how to set `isAllowAutoUpdateSchema` via Pulsar admin API, see [Manage AutoUpdate Strategy](admin-api-schemas.md#manage-autoupdate-strategy). 

    :::

6. If the schema is allowed to be updated, then the compatible strategy check is performed.
  
   * If the schema is compatible, the broker stores it and returns the schema version to the producer. All the messages produced by this producer are tagged with the schema version. 

   * If the schema is incompatible, the broker rejects it.

### Consumer side

This diagram illustrates how schema works on the consumer side. 

![Schema works at the consumer side](/assets/schema-consumer.png)

1. The application uses a schema instance to construct a consumer instance.
   
   The schema instance defines the schema that the consumer uses for decoding messages received from a broker.

2. The consumer connects to the broker with the `SchemaInfo` extracted from the passed-in schema instance.

3. The broker determines whether the topic has one of them (a schema/data/a local consumer and a local producer).

4. If a topic does not have all of them (a schema/data/a local consumer and a local producer):
   
     * If `isAllowAutoUpdateSchema` sets to **true**, then the consumer registers a schema and it is connected to a broker.
       
     * If `isAllowAutoUpdateSchema` sets to **false**, then the consumer is rejected to connect to a broker.
       
5. If a topic has one of them (a schema/data/a local consumer and a local producer), then the schema compatibility check is performed.
   
     * If the schema passes the compatibility check, then the consumer is connected to the broker.
       
     * If the schema does not pass the compatibility check, then the consumer is rejected to connect to the broker. 

6. The consumer receives messages from the broker. 

   If the schema used by the consumer supports schema versioning (for example, AVRO schema), the consumer fetches the `SchemaInfo` of the version tagged in messages and uses the passed-in schema and the schema tagged in messages to decode the messages.

## What's next?

* [Understand basic concepts](schema-understand.md)
* [Schema evolution and compatibility](schema-evolution-compatibility.md)
* [Get started](schema-get-started.md)
* [Manage schema](admin-api-schemas.md)
