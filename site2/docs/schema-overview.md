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


## What is Pulsar Schema

Pulsar schema is a data structure to define how to serialize/deserialize data and how to evolve your data format with backward compatibility. 


## Why use it

When a schema is enabled, Pulsar does parse data, it takes bytes as inputs and sends bytes as outputs. While data has meaning beyond bytes, you need to parse data and might encounter parse exceptions which mainly occur in the following situations:
* The field does not exist.
* The field type has changed (for example, `string` is changed to `int`).

There are a few methods to prevent and overcome these exceptions, for example, you can catch exceptions when parsing errors, which makes code hard to maintain; or you can adopt a schema management system to perform schema evolution, not to break downstream applications, and enforces type safety to max extend in the language you are using, the solution is Pulsar Schema.

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

### Without schema

If you construct a producer without specifying a schema, then the producer can only produce messages of type `byte[]`. If you have a POJO class, you need to serialize the POJO into bytes before sending messages.

**Example**

```java
Producer<byte[]> producer = client.newProducer()
        .topic(topic)
        .create();
User user = new User("Tom", 28);
byte[] message = … // serialize the `user` by yourself;
producer.send(message);
```

### With schema

If you construct a producer with specifying a schema, then you can send a class to a topic directly without worrying about how to serialize POJOs into bytes. 

**Example**

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

This diagram illustrates how does schema work on the Producer side.

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

For how to set `isAllowAutoUpdateSchema` via Pulsar admin API, see [Manage AutoUpdate Strategy](admin-api-schemas.md/#manage-autoupdate-strategy). 

:::

6. If the schema is allowed to be updated, then the compatible strategy check is performed.
  
  * If the schema is compatible, the broker stores it and returns the schema version to the producer. All the messages produced by this producer are tagged with the schema version. 

  * If the schema is incompatible, the broker rejects it.

### Consumer side

This diagram illustrates how does Schema work on the consumer side. 

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
