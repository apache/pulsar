---
id: schema-get-started
title: Get started
sidebar_label: "Get started"
original_id: schema-get-started
---

This chapter introduces Pulsar schemas and explains why they are important. 

## Schema Registry

Type safety is extremely important in any application built around a message bus like Pulsar. 

Producers and consumers need some kind of mechanism for coordinating types at the topic level to avoid various potential problems arise. For example, serialization and deserialization issues. 

Applications typically adopt one of the following approaches to guarantee type safety in messaging. Both approaches are available in Pulsar, and you're free to adopt one or the other or to mix and match on a per-topic basis.

#### Note
>
> Currently, the Pulsar schema registry is only available for the [Java client](client-libraries-java.md), [CGo client](client-libraries-cgo.md), [Python client](client-libraries-python.md), and [C++ client](client-libraries-cpp).

### Client-side approach

Producers and consumers are responsible for not only serializing and deserializing messages (which consist of raw bytes) but also "knowing" which types are being transmitted via which topics. 

If a producer is sending temperature sensor data on the topic `topic-1`, consumers of that topic will run into trouble if they attempt to parse that data as moisture sensor readings.

Producers and consumers can send and receive messages consisting of raw byte arrays and leave all type safety enforcement to the application on an "out-of-band" basis.

### Server-side approach 

Producers and consumers inform the system which data types can be transmitted via the topic. 

With this approach, the messaging system enforces type safety and ensures that producers and consumers remain synced.

Pulsar has a built-in **schema registry** that enables clients to upload data schemas on a per-topic basis. Those schemas dictate which data types are recognized as valid for that topic.

## Why use schema

When a schema is enabled, Pulsar does parse data, it takes bytes as inputs and sends bytes as outputs. While data has meaning beyond bytes, you need to parse data and might encounter parse exceptions which mainly occur in the following situations:

* The field does not exist

* The field type has changed (for example, `string` is changed to `int`)

There are a few methods to prevent and overcome these exceptions, for example, you can catch exceptions when parsing errors, which makes code hard to maintain; or you can adopt a schema management system to perform schema evolution, not to break downstream applications, and enforces type safety to max extend in the language you are using, the solution is Pulsar Schema.

Pulsar schema enables you to use language-specific types of data when constructing and handling messages from simple types like `string` to more complex application-specific types. 

**Example** 

You can use the _User_ class to define the messages sent to Pulsar topics.

```

public class User {
    String name;
    int age;
}

```

When constructing a producer with the _User_ class, you can specify a schema or not as below.

### Without schema

If you construct a producer without specifying a schema, then the producer can only produce messages of type `byte[]`. If you have a POJO class, you need to serialize the POJO into bytes before sending messages.

**Example**

```

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

```

Producer<User> producer = client.newProducer(JSONSchema.of(User.class))
        .topic(topic)
        .create();
User user = new User("Tom", 28);
producer.send(user);

```

### Summary

When constructing a producer with a schema, you do not need to serialize messages into bytes, instead Pulsar schema does this job in the background.
