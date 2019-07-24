---
id: schema-understand
title: Understand schema
sidebar_label: Understand schema
---

## `SchemaInfo`

Pulsar schema is defined in a data structure called `SchemaInfo`. 

The `SchemaInfo` is stored and enforced on a per-topic basis and cannot be stored at the namespace or tenant level.

A `SchemaInfo` consists of the following fields:

| Field | Description |
|---|---|
| `name` | Schema name (a string). |
| `type` | Schema type, which determines how to interpret the schema data. |
| `schema` | Schema data, which is a sequence of 8-bit unsigned bytes and schema-type specific. |
| `properties` | A map of string key/value pairs, which is application-specific. |

**Example**

This is the `SchemaInfo` of a string.

```text
{
    “name”: “test-string-schema”,
    “type”: “STRING”,
    “schema”: “”,
    “properties”: {}
}
```

## Schema type

Pulsar supports various schema types, which are mainly divided into two categories: 

* Primitive type 

* Complex type

### Primitive type

Currently, Pulsar supports the following primitive types:

| Primitive Type | Description |
|---|---|
| `BOOLEAN` | A binary value |
| `INT8` | A 8-bit signed integer |
| `INT16` | A 16-bit signed integer |
| `INT32` | A 32-bit signed integer |
| `INT64` | A 64-bit signed integer |
| `FLOAT` | A single precision (32-bit) IEEE 754 floating-point number |
| `DOUBLE` | A double-precision (64-bit) IEEE 754 floating-point number |
| `BYTES` | A sequence of 8-bit unsigned bytes |
| `STRING` | A Unicode character sequence |
| `TIMESTAMP` (`DATE`, `TIME`) |  A logic type represents a specific instant in time with millisecond precision. It stores the number of milliseconds since `January 1, 1970, 00:00:00 GMT` as an `INT64` value | 

For primitive types, Pulsar does not store any schema data in `SchemaInfo`. The `type` in `SchemaInfo` is used to determine how to serialize and deserialize the data. 

Some of the primitive schema implementations can use `properties` to store implementation-specific tunable settings. For example, a `string` schema can use `properties` to store the encoding charset to serialize and deserialize strings.

The conversions between **Pulsar schema types** and **language-specific primitive types** are as below.

| Schema Type | Java Type| Python Type |
|---|---|---|
| BOOLEAN | boolean | bool |
| INT8 | byte | |
| INT16 | short | | 
| INT32 | int | |
| INT64 | long | |
| FLOAT | float | float |
| DOUBLE | double | float |
| BYTES | byte[], ByteBuffer, ByteBuf | bytes |
| STRING | string | str |
| TIMESTAMP | java.sql.Timestamp | |
| TIME | java.sql.Time | |
| DATE | java.util.Date | |

**Example**

This example demonstrates how to use a string schema.

1. Create a producer with a string schema and send messages.

    ```text
    Producer<String> producer = client.newProducer(Schema.STRING).create();
    producer.newMessage().value("Hello Pulsar!").send();
    ```

2. Create a consumer with a string schema and receive messages.  

    ```text
    Consumer<String> consumer = client.newConsumer(Schema.STRING).create();
    consumer.receive();
    ```

### Complex type

Currently, Pulsar supports the following complex types:

| Complex Type | Description |
|---|---|
| `keyvalue` | Represents a complex type of a key/value pair. |
| `struct` | Supports **AVRO**, **JSON**, and **Protobuf**. |

#### `keyvalue`

`Keyvalue` schema helps applications define schemas for both key and value. 

For `SchemaInfo` of `keyvalue` schema, Pulsar stores the `SchemaInfo` of key schema and the `SchemaInfo` of value schema together.

Pulsar provides two methods to encode a key/value pair in messages as below, users can choose the encoding type when constructing the key/value schema.

##### `INLINE`

Key/value pairs will be encoded together in the message payload.

##### `SEPARATED`

Key will be encoded in the message key and the value will be encoded in the message payload. 
  
**Example**
    
This example shows how to construct a key/value schema and then use it to produce and consume messages.

1. Construct a key/value schema with `INLINE` encoding type.

```text
Schema<KeyValue<Integer, String>> kvSchema = Schema.KeyValue(
Schema.INT32,
Schema.STRING,
KeyValueEncodingType.INLINE
);
```

2. Optionally, construct a key/value schema with `SEPARATED` encoding type.

```text
Schema<KeyValue<Integer, String>> kvSchema = Schema.KeyValue(
Schema.INT32,
Schema.STRING,
KeyValueEncodingType.SEPARATED
);
```

3. Produce messages using a key/value schema.

```text
Schema<KeyValue<Integer, String>> kvSchema = Schema.KeyValue(
Schema.INT32,
Schema.STRING,
KeyValueEncodingType.SEPARATED
);

Producer<KeyValue<Integer, String>> producer = client.newProducer(kvSchema)
    .topic(TOPIC)
    .create();

final int key = 100;
final String value = "value-100”;

// send the key/value message
producer.newMessage()
.value(new KeyValue<>(key, value))
.send();
```

4. Consume messages using a key/value schema.

```
Schema<KeyValue<Integer, String>> kvSchema = Schema.KeyValue(
Schema.INT32,
Schema.STRING,
KeyValueEncodingType.SEPARATED
);

Consumer<KeyValue<Integer, String>> consumer = client.newConsumer(kvSchema)
    .topic(TOPIC)
    ...
    .subscribe();

// receive key/value pair
Message<KeyValue<Integer, String>> msg = consumer.receive();
KeyValue<Integer, String> kv = msg.getValue();
```

#### `struct`

Pulsar uses [Avro Specification](http://avro.apache.org/docs/current/spec.html) to declare the schema definition for `struct` schema. This allows Pulsar:

* to use same tools to manage schema definitions

* to use different serialization/deserialization methods to handle data

There are two methods to use `struct` schema. 

##### `static`

You can predefine the `struct` schema, and it can be a POJO in Java, a `struct` in Go, or classes generated by Avro or Protobuf tools. 

**Example** 

Pulsar gets the schema definition from the predefined `struct` using an Avro library. The schema definition is the schema data stored as a part of the schema info.

1. Create the _User_ class to define the messages sent to Pulsar topics.

  ```text
  public class User {
      String name;
      int age;
  }
  ```

2. Create a producer with a `struct` schema and send messages.

  ```text
  Producer<User> producer = client.newProducer(Schema.AVRO(User.class)).create();
  producer.newMessage().value(User.builder().userName("pulsar-user").userId(1L).build()).send();
  ```

3. Create a consumer with a `struct` schema and receive messages

  ```text
  Consumer<User> consumer = client.newConsumer(Schema.AVRO(User.class)).create();
  User user = consumer.receive();
  ```

##### `generic`

Sometimes applications do not have pre-defined structs, and you can use this method to define schema and access data.

You can define the `struct` schema using the `GenericSchemaBuilder`, generate a generic struct using `GenericRecordBuilder` and consume messages into `GenericRecord`.

**Example** 

1. Use `RecordSchemaBuilder` to build a schema.

  ```text
  RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("schemaName");
  recordSchemaBuilder.field("intField").type(SchemaType.INT32);
  SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.AVRO);

  Producer<GenericRecord> producer = client.newProducer(Schema.generic(schemaInfo)).create();
  ```

2. Use `RecordBuilder` to build the struct records.

  ```text
  producer.newMessage().value(schema.newRecordBuilder()
              .set("intField", 32)
              .build()).send();
  ```

### Auto Schema

If you don't know the schema type of a Pulsar topic in advance, you can use AUTO schema to produce or consume generic records to or from brokers.

| Auto Schema Type | Description |
|---|---|
| `AUTO_PRODUCE` | This is useful for transferring data **from a producer to a Pulsar topic that has a schema**. |
| `AUTO_CONSUME` | This is useful for transferring data **from a Pulsar topic that has a schema to a consumer**. |

#### AUTO_PRODUCE

`AUTO_PRODUCE` schema helps a producer validate whether the bytes sent by the producer is compatible with the schema of a topic. 

**Example**

Suppose that:

* You have a producer processing messages from a Kafka topic _K_, 

* You have a Pulsar topic _P_, and you do not know its schema type.

* Your application reads the messages from _K_ and writes the messages to _P_.  
   
In this case, you can use `AUTO_PRODUCE` to verify whether the bytes produced by _K_ can be sent to _P_ or not.

```text
Produce<byte[]> pulsarProducer = client.newProducer(Schema.AUTO_PRODUCE())
    …
    .create();

byte[] kafkaMessageBytes = … ; 

pulsarProducer.produce(kafkaMessageBytes);
```

### AUTO_CONSUME

`AUTO_CONSUME` schema helps a Pulsar topic validate whether the bytes sent by a Pulsar topic is compatible with a consumer, that is, the Pulsar topic deserializes messages into language-specific objects using the `SchemaInfo` retrieved from broker-side. 

Currently, `AUTO_CONSUME` only supports **AVRO** and **JSON** schemas. It deserializes messages into `GenericRecord`.

**Example**

Suppose that:

* You have a Pulsar topic _P_.

* You have a consumer (for example, MySQL) receiving messages from the topic _P_.

* You application reads the messages from _P_ and writes the messages to MySQL.
   
In this case, you can use `AUTO_CONSUME` to verify whether the bytes produced by _P_ can be sent to MySQL or not.

```text
Consumer<GenericRecord> pulsarConsumer = client.newConsumer(Schema.AUTO_CONSUME())
    …
    .subscribe();

Message<GenericRecord> msg = consumer.receive() ; 
GenericRecord record = msg.getValue();
…
```
