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

Pulsar messages are stored as unstructured byte arrays and the data structure (as known as schema) is applied to this data only when it's read. The schema serializes the bytes before they are published to a topic and deserializes them before they are delivered to the consumers, dictating which data types are recognized as valid for a given topic.

Pulsar schema registry is a central repository to store the schema information, which enables producers/consumers to coordinate on the schema of a topic’s data through brokers.

:::note

Currently, Pulsar schema is only available for the [Java client](client-libraries-java.md), [Go client](client-libraries-go.md), [Python client](client-libraries-python.md), and [C++ client](client-libraries-cpp.md).

:::

## Why use it

Type safety is extremely important in any application built around a messaging and streaming system. Raw bytes are flexible for data transfer, but the flexibility and neutrality come with a cost: you have to overlay data type checking and serialization/deserialization to ensure that the bytes fed into the system can be read and successfully consumed. In other words, you need to make sure the data intelligible and usable to applications.

Pulsar schema resolves the pain points with the following capabilities:
* enforces the data type safety when a topic has a schema defined. As a result, producers/consumers are only allowed to connect if they are using a “compatible” schema.
* provides a central location for storing information about the schemas used within your organization, in turn greatly simplifies the sharing of this information across application teams.
* serves as a single source of truth for all the message schemas used across all your services and development teams, which makes it easier for them to collaborate.
* keeps data compatibility on-track between schema versions. When new schemas are uploaded, the new versions can be read by old consumers. 
* stored in the existing storage layer BookKeeper, no additional system required.

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

## Use case

You can use language-specific types of data when constructing and handling messages from simple data types like `string` to more complex application-specific types.

For example, you are using the _User_ class to define the messages sent to Pulsar topics.

```java
public class User {
    String name;
    int age;
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
Producer<User> producer = client.newProducer(JSONSchema.of(User.class))
        .topic(topic)
        .create();
User user = new User("Tom", 28);
producer.send(user);
```

## What's next?

* [Understand basic concepts](schema-understand.md)
* [Schema evolution and compatibility](schema-evolution-compatibility.md)
* [Get started](schema-get-started.md)
* [Manage schema](admin-api-schemas.md)
