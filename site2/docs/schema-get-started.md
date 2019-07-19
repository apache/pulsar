---
id: schema-get-started
title: Get started
sidebar_label: Get started
---

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

## Without schema

If you construct a producer without specifying a schema, then the producer can only produce messages of type `byte[]`. If you have a POJO class, you need to serialize the POJO into bytes before sending messages.

**Example**

```
Producer<byte[]> producer = client.newProducer()
        .topic(topic)
        .create();
User user = new User(“Tom”, 28);
byte[] message = … // serialize the `user` by yourself;
producer.send(message);
```
## With schema

If you construct a producer with specifying a schema, then you can send a class to a topic directly without worrying about how to serialize POJOs into bytes. 

**Example**

This example constructs a producer with the _JSONSchema_, and you can send the _User_ class to topics directly without worrying about how to serialize it into bytes. 

```
Producer<User> producer = client.newProducer(JSONSchema.of(User.class))
        .topic(topic)
        .create();
User user = new User(“Tom”, 28);
producer.send(User);
```

## Summary

When constructing a producer with a schema, you do not need to serialize messages into bytes, instead Pulsar schema does this job in the background.
