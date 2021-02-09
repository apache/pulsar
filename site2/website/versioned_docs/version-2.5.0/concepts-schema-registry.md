---
id: version-2.5.0-concepts-schema-registry
title: Schema Registry
sidebar_label: Schema Registry
original_id: concepts-schema-registry
---

Type safety is extremely important in any application built around a message bus like Pulsar. Producers and consumers need some kind of mechanism for coordinating types at the topic level lest a wide variety of potential problems arise (for example serialization and deserialization issues). Applications typically adopt one of two basic approaches to type safety in messaging:

1. A "client-side" approach in which message producers and consumers are responsible for not only serializing and deserializing messages (which consist of raw bytes) but also "knowing" which types are being transmitted via which topics. If a producer is sending temperature sensor data on the topic `topic-1`, consumers of that topic will run into trouble if they attempt to parse that data as, say, moisture sensor readings.
2. A "server-side" approach in which producers and consumers inform the system which data types can be transmitted via the topic. With this approach, the messaging system enforces type safety and ensures that producers and consumers remain synced.

Both approaches are available in Pulsar, and you're free to adopt one or the other or to mix and match on a per-topic basis.

1. For the "client-side" approach, producers and consumers can send and receive messages consisting of raw byte arrays and leave all type safety enforcement to the application on an "out-of-band" basis.
1. For the "server-side" approach, Pulsar has a built-in **schema registry** that enables clients to upload data schemas on a per-topic basis. Those schemas dictate which data types are recognized as valid for that topic.

#### Note
>
> Currently, the Pulsar schema registry is only available for the [Java client](client-libraries-java.md), [CGo client](client-libraries-go.md), [Python client](client-libraries-python.md), and [C++ client](client-libraries-cpp.md).

## Basic architecture

Schemas are automatically uploaded when you create a typed Producer with a Schema. Additionally, Schemas can be manually uploaded to, fetched from, and updated via Pulsar's {@inject: rest:REST:tag/schemas} API.

> #### Other schema registry backends
> Out of the box, Pulsar uses the [Apache BookKeeper](concepts-architecture-overview#persistent-storage) log storage system for schema storage. You can, however, use different backends if you wish. Documentation for custom schema storage logic is coming soon.

## How schemas work

Pulsar schemas are applied and enforced *at the topic level* (schemas cannot be applied at the namespace or tenant level). Producers and consumers upload schemas to Pulsar brokers.

Pulsar schemas are fairly simple data structures that consist of:

* A **name**. In Pulsar, a schema's name is the topic to which the schema is applied.
* A **payload**, which is a binary representation of the schema
* A schema [**type**](#supported-schema-formats)
* User-defined **properties** as a string/string map. Usage of properties is wholly application specific. Possible properties might be the Git hash associated with a schema, an environment like `dev` or `prod`, etc.

## Schema versions

In order to illustrate how schema versioning works, let's walk through an example. Imagine that the Pulsar [Java client](client-libraries-java.md) created using the code below attempts to connect to Pulsar and begin sending messages:

```java
PulsarClient client = PulsarClient.builder()
        .serviceUrl("pulsar://localhost:6650")
        .build();

Producer<SensorReading> producer = client.newProducer(JSONSchema.of(SensorReading.class))
        .topic("sensor-data")
        .sendTimeout(3, TimeUnit.SECONDS)
        .create();
```

The table below lists the possible scenarios when this connection attempt occurs and what will happen in light of each scenario:

Scenario | What happens
:--------|:------------
No schema exists for the topic | The producer is created using the given schema. The schema is transmitted to the broker and stored (since no existing schema is "compatible" with the `SensorReading` schema). Any consumer created using the same schema/topic can consume messages from the `sensor-data` topic.
A schema already exists; the producer connects using the same schema that's already stored | The schema is transmitted to the Pulsar broker. The broker determines that the schema is compatible. The broker attempts to store the schema in [BookKeeper](concepts-architecture-overview.md#persistent-storage) but then determines that it's already stored, so it's then used to tag produced messages.
A schema already exists; the producer connects using a new schema that is compatible | The producer transmits the schema to the broker. The broker determines that the schema is compatible and stores the new schema as the current version (with a new version number).

> Schemas are versioned in succession. Schema storage happens in the broker that handles the associated topic so that version assignments can be made. Once a version is assigned/fetched to/for a schema, all subsequent messages produced by that producer are tagged with the appropriate version.


## Supported schema formats

The following formats are supported by the Pulsar schema registry:

* None. If no schema is specified for a topic, producers and consumers will handle raw bytes.
* `String` (used for UTF-8-encoded strings)
* [JSON](https://www.json.org/)
* [Protobuf](https://developers.google.com/protocol-buffers/)
* [Avro](https://avro.apache.org/)

For usage instructions, see the documentation for your preferred client library:

* [Java](client-libraries-java.md#schemas)

> Support for other schema formats will be added in future releases of Pulsar.

The following example shows how to define an Avro schema using the `GenericSchemaBuilder`, generate a generic Avro schema using `GenericRecordBuilder`, and consume messages into `GenericRecord`.

**Example** 

1. Use the `RecordSchemaBuilder` to build a schema.

    ```java
    RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("schemaName");
    recordSchemaBuilder.field("intField").type(SchemaType.INT32);
    SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.AVRO);

    Producer<GenericRecord> producer = client.newProducer(Schema.generic(schemaInfo)).create();
    ```

2. Use `RecordBuilder` to build the generic records.

    ```java
    producer.newMessage().value(schema.newRecordBuilder()
                .set("intField", 32)
                .build()).send();
    ```

## Managing Schemas

You can use Pulsar admin tools to manage schemas for topics.
