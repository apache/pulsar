---
id: schema-understand
title: Understand schema
sidebar_label: "Understand schema"
---

````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````


This section explains the basic concepts of Pulsar schema and provides additional reference.

## Schema definition

Pulsar schema is defined in a data structure called `SchemaInfo`. It is stored and enforced on a per-topic basis and cannot be stored at the namespace or tenant level.

A `SchemaInfo` consists of the following fields:

|  Field  |   Description  | 
| --- | --- |
|  `name`  |   Schema name (a string).  | 
|  `type`  |   Schema type, which determines how to interpret the schema data. <li>Predefined schema: see [here](#schema-type). </li><li>Customized schema: it is left as an empty string. </li> | 
|  `schema`（`payload`)  |   Schema data, which is a sequence of 8-bit unsigned bytes and schema-type specific.  | 
|  `properties`  |   It is a user-defined property as a string/string map. Applications can use this bag for carrying any application-specific logics. Possible properties might be the Git hash associated with the schema, and an environment string like `dev` or `prod`.  | 

**Example**

This is the `SchemaInfo` of a string.

```json
{
    "name": "test-string-schema",
    "type": "STRING",
    "schema": "",
    "properties": {}
}
```

## Schema type

Pulsar supports various schema types, which are mainly divided into two categories: 
* [Primitive type](#primitive-type) 
* [Complex type](#complex-type)

### Primitive type

The following table outlines the primitive types that Pulsar schema supports, and the conversions between **schema types** and **language-specific primitive types**.

| Primitive Type | Description | Java Type| Python Type | Go Type |
|---|---|---|---|---|
| `BOOLEAN` | A binary value | boolean | bool | bool |
| `INT8` | A 8-bit signed integer | int | | int8 |
| `INT16` | A 16-bit signed integer | int | | int16 |
| `INT32` | A 32-bit signed integer | int | | int32 |
| `INT64` | A 64-bit signed integer | int | | int64 |
| `FLOAT` | A single precision (32-bit) IEEE 754 floating-point number | float | float | float32 |
| `DOUBLE` | A double-precision (64-bit) IEEE 754 floating-point number | double | float | float64|
| `BYTES` | A sequence of 8-bit unsigned bytes | byte[], ByteBuffer, ByteBuf | bytes | []byte |
| `STRING` | A Unicode character sequence | string | str | string| 
| `TIMESTAMP` (`DATE`, `TIME`) |  A logic type represents a specific instant in time with millisecond precision. <br />It stores the number of milliseconds since `January 1, 1970, 00:00:00 GMT` as an `INT64` value |  java.sql.Timestamp (java.sql.Time, java.util.Date) | | |
| INSTANT | A single instantaneous point on the time-line with nanoseconds precision| java.time.Instant | | |
| LOCAL_DATE | An immutable date-time object that represents a date, often viewed as year-month-day| java.time.LocalDate | | |
| LOCAL_TIME | An immutable date-time object that represents a time, often viewed as hour-minute-second. Time is represented to nanosecond precision.| java.time.LocalDateTime | |
| LOCAL_DATE_TIME | An immutable date-time object that represents a date-time, often viewed as year-month-day-hour-minute-second | java.time.LocalTime | |

For primitive types, Pulsar does not store any schema data in `SchemaInfo`. The `type` in `SchemaInfo` determines how to serialize and deserialize the data. 

Some of the primitive schema implementations can use `properties` to store implementation-specific tunable settings. For example, a `string` schema can use `properties` to store the encoding charset to serialize and deserialize strings.

For more instructions, see [Construct a string schema](schema-get-started.md#construct-a-string-schema).


### Complex type

Currently, Pulsar supports the following complex types:

| Complex Type | Description |
|---|---|
| `KeyValue` | Represents a complex type of a key/value pair. |
| `Struct` | Handles structured data. It supports `AvroBaseStructSchema` and `ProtobufNativeSchema`. |

#### `KeyValue` schema

`KeyValue` schema helps applications define schemas for both key and value. Pulsar stores the `SchemaInfo` of key schema and the value schema together.

You can choose the encoding type when constructing the key/value schema.：
* `INLINE` - Key/value pairs are encoded together in the message payload.
* `SEPARATED` - see [Construct a key/value schema](schema-get-started.md#construct-a-keyvalue-schema).

#### `Struct` schema

`Struct` schema supports `AvroBaseStructSchema` and `ProtobufNativeSchema`.

|Type|Description|
---|---|
`AvroBaseStructSchema`|Pulsar uses [Avro Specification](http://avro.apache.org/docs/current/spec.html) to declare the schema definition for `AvroBaseStructSchema`, which supports  `AvroSchema`, `JsonSchema`, and `ProtobufSchema`. <br /><br />This allows Pulsar:<br />- to use the same tools to manage schema definitions<br />- to use different serialization or deserialization methods to handle data|
`ProtobufNativeSchema`|`ProtobufNativeSchema` is based on protobuf native Descriptor. <br /><br />This allows Pulsar:<br />- to use native protobuf-v3 to serialize or deserialize data<br />- to use `AutoConsume` to deserialize data.

Pulsar provides the following methods to use the `struct` schema. 
* `static`
* `generic`
* `SchemaDefinition`

For more examples, see [Construct a struct schema](schema-get-started.md#construct-a-struct-schema).

### Auto Schema

If you don't know the schema type of a Pulsar topic in advance, you can use AUTO schema to produce or consume generic records to or from brokers.

Auto schema contains two categories:
* `AUTO_PRODUCE` transfers data from a producer to a Pulsar topic that has a schema and helps the producer validate whether the out-bound bytes are compatible with the schema of the topic. For more instructions, see [Construct an AUTO_PRODUCE schema](schema-get-started.md#construct-an-auto_produce-schema).
* `AUTO_CONSUME` transfers data from a Pulsar topic that has a schema to a consumer and helps the topic validate whether the out-bound bytes are compatible with the consumer. In other words, the topic deserializes messages into language-specific objects `GenericRecord` using the `SchemaInfo` retrieved from brokers. Currently, `AUTO_CONSUME` supports AVRO, JSON and ProtobufNativeSchema schemas. For more instructions, see [Construct an AUTO_CONSUME schema](schema-get-started.md#construct-an-auto_consume-schema).

### Native Avro Schema

When migrating or ingesting event or message data from external systems (such as Kafka and Cassandra), the events are often already serialized in Avro format. The applications producing the data typically have validated the data against their schemas (including compatibility checks) and stored them in a database or a dedicated service (such as a schema registry). The schema of each serialized data record is usually retrievable by some metadata attached to that record. In such cases, a Pulsar producer doesn't need to repeat the schema validation step when sending the ingested events to a topic. All it needs to do is passing each message or event with its schema to Pulsar.

Hence, we provide `Schema.NATIVE_AVRO` to wrap a native Avro schema of type `org.apache.avro.Schema`. The result is a schema instance of Pulsar that accepts a serialized Avro payload without validating it against the wrapped Avro schema. See for more details.

## Schema versioning

Each `SchemaInfo` stored with a topic has a version. The schema version manages schema changes happening within a topic. 

Messages produced with a given `SchemaInfo` is tagged with a schema version, so when a message is consumed by a Pulsar client, the Pulsar client can use the schema version to retrieve the corresponding `SchemaInfo` and then use the `SchemaInfo` to deserialize data.

Schemas are versioned in succession. Schema storage happens in a broker that handles the associated topics so that version assignments can be made. 

Once a version is assigned/fetched to/for a schema, all subsequent messages produced by that producer are tagged with the appropriate version.

**Example**

The following example illustrates how the schema version works.

Suppose that a Pulsar [Java client](client-libraries-java.md) created using the code below attempts to connect to Pulsar and begins to send messages:

```java
PulsarClient client = PulsarClient.builder()
        .serviceUrl("pulsar://localhost:6650")
        .build();

Producer<SensorReading> producer = client.newProducer(JSONSchema.of(SensorReading.class))
        .topic("sensor-data")
        .sendTimeout(3, TimeUnit.SECONDS)
        .create();
```

The table below lists the possible scenarios when this connection attempt occurs and what happens in each scenario:

| Scenario |  What happens | 
| --- | --- |
|  <li>No schema exists for the topic. </li> |   (1) The producer is created using the given schema. (2) Since no existing schema is compatible with the `SensorReading` schema, the schema is transmitted to the broker and stored. (3) Any consumer created using the same schema or topic can consume messages from the `sensor-data` topic.  | 
|  <li>A schema already exists. </li><li>The producer connects using the same schema that is already stored. </li> |   (1) The schema is transmitted to the broker. (2) The broker determines that the schema is compatible. (3) The broker attempts to store the schema in [BookKeeper](concepts-architecture-overview.md#persistent-storage) but then determines that it's already stored, so it is used to tag produced messages.  |   <li>A schema already exists. </li><li>The producer connects using a new schema that is compatible. </li> |   (1) The schema is transmitted to the broker. (2) The broker determines that the schema is compatible and stores the new schema as the current version (with a new version number).  | 

## Schema AutoUpdate

If a schema passes the schema compatibility check, Pulsar producer automatically updates this schema to the topic it produces by default. 

### AutoUpdate for producer

For a producer, the `AutoUpdate` happens in the following cases:

* If a **topic doesn’t have a schema**, Pulsar registers a schema automatically.

* If a **topic has a schema**:

  * If a **producer doesn’t carry a schema**:

  * If `isSchemaValidationEnforced` or `schemaValidationEnforced` is **disabled** in the namespace to which the topic belongs, the producer is allowed to connect to the topic and produce data. 
  
  * If `isSchemaValidationEnforced` or `schemaValidationEnforced` is **enabled** in the namespace to which the topic belongs, the producer is rejected and disconnected.

  * If a **producer carries a schema**:
  
  A broker performs the compatibility check based on the configured compatibility check strategy of the namespace to which the topic belongs.
  
  * If the schema is registered, a producer is connected to a broker. 
  
  * If the schema is not registered:
  
     * If `isAllowAutoUpdateSchema` sets to **false**, the producer is rejected to connect to a broker.
  
      * If `isAllowAutoUpdateSchema` sets to **true**:
   
          * If the schema passes the compatibility check, then the broker registers a new schema automatically for the topic and the producer is connected.
      
          * If the schema does not pass the compatibility check, then the broker does not register a schema and the producer is rejected to connect to a broker.

![AutoUpdate Producer](/assets/schema-producer.png)

### AutoUpdate for consumer

For a consumer, the `AutoUpdate` happens in the following cases:

* If a **consumer connects to a topic without a schema** (which means the consumer receiving raw bytes), the consumer can connect to the topic successfully without doing any compatibility check.

* If a **consumer connects to a topic with a schema**.

  * If a topic does not have all of them (a schema/data/a local consumer and a local producer):
  
      * If `isAllowAutoUpdateSchema` sets to **true**, then the consumer registers a schema and it is connected to a broker.
      
      * If `isAllowAutoUpdateSchema` sets to **false**, then the consumer is rejected to connect to a broker.
      
  * If a topic has one of them (a schema/data/a local consumer and a local producer), then the schema compatibility check is performed.
  
      * If the schema passes the compatibility check, then the consumer is connected to the broker.
      
      * If the schema does not pass the compatibility check, then the consumer is rejected to connect to the broker.
      
![AutoUpdate Consumer](/assets/schema-consumer.png)