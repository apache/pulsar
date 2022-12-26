---
id: schema-understand
title: Understand schema
sidebar_label: "Understand schema"
---

````mdx-code-block
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
````


This section explains the basic concepts of Pulsar schema and provides additional references.

## Schema definition

Pulsar schema is defined in a data structure called `SchemaInfo`. It is stored and enforced on a per-topic basis and cannot be stored at the namespace or tenant level.

This is a string example of `SchemaInfo`.

```json
{
    "name": "test-string-schema",
    "type": "STRING",
    "schema": "",
    "properties": {}
}
```

The following table outlines the fields that each `SchemaInfo` consists of.

|  Field  |   Description  | 
| --- | --- |
|  `name`  |   Schema name (a string).  | 
|  `type`  |   [Schema type](#schema-type) that determines how to serialize and deserialize the schema data. | 
|  `schema`  |   Schema data, which is a sequence of 8-bit unsigned bytes and specific schema type.  | 
|  `properties`  |   A user-defined property as a string/string map, which can be used by applications to carry any application-specific logic.  |

## Schema type

Pulsar supports various schema types, which are mainly divided into two categories: 
* [Primitive type](#primitive-type) 
* [Complex type](#complex-type)

### Primitive type

The following table outlines the primitive types that Pulsar schema supports, and the conversions between **schema types** and **language-specific primitive types**.

| Primitive Type | Description | Java Type| Python Type | Go Type | C++ Type | C# Type|
|---|---|---|---|---|---|---|
| `BOOLEAN` | A binary value. | boolean | bool | bool | bool | bool |
| `INT8` | A 8-bit signed integer. | int | int | int8 | int8_t | byte |
| `INT16` | A 16-bit signed integer. | int | int | int16 | int16_t | short |
| `INT32` | A 32-bit signed integer. | int | int | int32 | int32_t | int |
| `INT64` | A 64-bit signed integer. | int | int | int64 | int64_t | long |
| `FLOAT` | A single precision (32-bit) IEEE 754 floating-point number. | float | float | float32 | float | float |
| `DOUBLE` | A double-precision (64-bit) IEEE 754 floating-point number. | double | double | float64| double | double |
| `BYTES` | A sequence of 8-bit unsigned bytes. | byte[], ByteBuffer, ByteBuf | bytes | []byte | void * | byte[], ReadOnlySequence<byte\> |
| `STRING` | An Unicode character sequence. | string | str | string| std::string | string |
| `TIMESTAMP` (`DATE`, `TIME`) |  A logic type represents a specific instant in time with millisecond precision. <br />It stores the number of milliseconds since `January 1, 1970, 00:00:00 GMT` as an `INT64` value. |  java.sql.Timestamp (java.sql.Time, java.util.Date) | N/A | N/A | N/A | DateTime,TimeSpan |
| `INSTANT`| A single instantaneous point on the timeline with nanoseconds precision. | java.time.Instant | N/A | N/A | N/A | N/A |
| `LOCAL_DATE` | An immutable date-time object that represents a date, often viewed as year-month-day. | java.time.LocalDate | N/A | N/A | N/A | N/A |
| `LOCAL_TIME` | An immutable date-time object that represents a time, often viewed as hour-minute-second. Time is represented to nanosecond precision. | java.time.LocalDateTime | N/A | N/A  | N/A | N/A |
| LOCAL_DATE_TIME | An immutable date-time object that represents a date-time, often viewed as year-month-day-hour-minute-second. | java.time.LocalTime | N/A | N/A | N/A | N/A |

:::note

Pulsar does not store any schema data in `SchemaInfo` for primitive types. Some of the primitive schema implementations can use the `properties` parameter to store implementation-specific tunable settings. For example, a string schema can use `properties` to store the encoding charset to serialize and deserialize strings.

:::

For more instructions and examples, see [Construct a string schema](schema-get-started.md#string).


### Complex type

The following table outlines the complex types that Pulsar schema supports:

| Complex Type | Description |
|---|---|
| `KeyValue` | Represents a complex key/value pair. |
| `Struct` | Represents structured data, including `AvroBaseStructSchema`, `ProtobufNativeSchema` and `NativeAvroBytesSchema`. |

#### `KeyValue` schema

`KeyValue` schema helps applications define schemas for both key and value. Pulsar stores the `SchemaInfo` of the key schema and the value schema together.

Pulsar provides the following methods to encode a **single** key/value pair in a message：
* `INLINE` - Key/Value pairs are encoded together in the message payload.
* `SEPARATED` - The Key is stored as a message key, while the value is stored as the message payload. See [Construct a key/value schema](schema-get-started.md#keyvalue) for more details.

#### `Struct` schema

The following table outlines the `struct` types that Pulsar schema supports:

| Type                    | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
|-------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `AvroBaseStructSchema`  | Pulsar uses [Avro Specification](http://avro.apache.org/docs/current/spec.html) to declare the schema definition for `AvroBaseStructSchema`, which supports [`AvroSchema`](schema-get-started.md#avro), [`JsonSchema`](schema-get-started.md#json), and [`ProtobufSchema`](schema-get-started.md#protobuf).<br /><br />This allows Pulsar to:<br />- use the same tools to manage schema definitions.<br />- use different serialization or deserialization methods to handle data.                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `ProtobufNativeSchema`  | [`ProtobufNativeSchema`](schema-get-started.md#protobufnative) is based on protobuf native descriptor. <br /><br />This allows Pulsar to:<br />- use native protobuf-v3 to serialize or deserialize data.<br />- use `AutoConsume` to deserialize data.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `NativeAvroBytesSchema` | [`NativeAvroBytesSchema`](schema-get-started.md#native-avro) wraps a native Avro schema type `org.apache.avro.Schema`. The result is a schema instance that accepts a serialized Avro payload without validating it against the wrapped Avro schema. <br /><br />When you migrate or ingest event or messaging data from external systems (such as Kafka and Cassandra), the data is often already serialized in Avro format. The applications producing the data typically have validated the data against their schemas (including compatibility checks) and stored them in a database or a dedicated service (such as schema registry). The schema of each serialized data record is usually retrievable by some metadata attached to that record. In such cases, a Pulsar producer doesn't need to repeat the schema validation when sending the ingested events to a topic. All it needs to do is pass each message or event with its schema to Pulsar. |

Pulsar provides the following methods to use the `struct` schema. 
* `static`
* `generic`
* `SchemaDefinition`

This example shows how to construct a `struct` schema with these methods and use it to produce and consume messages.

````mdx-code-block
<Tabs 
  defaultValue="static"
  values={[{"label":"static","value":"static"},{"label":"generic","value":"generic"},{"label":"SchemaDefinition","value":"SchemaDefinition"}]}>

<TabItem value="static">

You can predefine the `struct` schema, which can be a POJO in Java, a `struct` in Go, or classes generated by Avro or Protobuf tools. 

**Example** 

Pulsar gets the schema definition from the predefined `struct` using an Avro library. The schema definition is the schema data stored as a part of the `SchemaInfo`.

1. Create the _User_ class to define the messages sent to Pulsar topics.

   ```java
   # If you use Lombok

   @Builder
   @AllArgsConstructor
   @NoArgsConstructor
   public static class User {
       public String name;
       public int age;
   }

   # If you DON'T use Lombok you will need to add the constructor like this
   # 
   #   public static class User {
   #    String name;
   #    int age;
   #    public User() { } 
   #    public User(String name, int age) { this.name = name; this.age = age; } }
   #}

   ```

2. Create a producer with a `struct` schema and send messages.

   ```java
   Producer<User> producer = client.newProducer(Schema.AVRO(User.class)).create();
   producer.newMessage().value(new User("pulsar-user", 1)).send();
   ```

3. Create a consumer with a `struct` schema and receive messages

   ```java
   Consumer<User> consumer = client.newConsumer(Schema.AVRO(User.class)).subscribe();
   User user = consumer.receive().getValue();
   ```

</TabItem>
<TabItem value="generic">

Sometimes applications do not have pre-defined structs, and you can use this method to define schema and access data.

You can define the `struct` schema using the `GenericSchemaBuilder`, generate a generic struct using `GenericRecordBuilder`, and consume messages into `GenericRecord`.

**Example** 

1. Use `RecordSchemaBuilder` to build a schema.

   ```java
   RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("schemaName");
   recordSchemaBuilder.field("intField").type(SchemaType.INT32);
   SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.AVRO);
   
   Consumer<GenericRecord> consumer = client.newConsumer(Schema.generic(schemaInfo))
        .topic(topicName)
        .subscriptionName(subscriptionName)
        .subscribe();
   Producer<GenericRecord> producer = client.newProducer(Schema.generic(schemaInfo))
        .topic(topicName)
        .create();
   ```

2. Use `RecordBuilder` to build the struct records.

   ```java
   GenericSchemaImpl schema = GenericAvroSchema.of(schemaInfo);
   // send message
   GenericRecord record = schema.newRecordBuilder().set("intField", 32).build();
   producer.newMessage().value(record).send();
   // receive message
   Message<GenericRecord> msg = consumer.receive();
   
   Assert.assertEquals(msg.getValue().getField("intField"), 32);
   ```

</TabItem>
<TabItem value="SchemaDefinition">

You can define the `schemaDefinition` to generate a `struct` schema.

**Example** 

1. Create the _User_ class to define the messages sent to Pulsar topics.

   ```java
   public static class User {
       public String name;
       public int age;
       public User(String name, int age) {
 	this.name = name;
	this.age = age
       }
       public User() {}
   }
   ```

2. Create a producer with a `SchemaDefinition` and send messages.

   ```java
   SchemaDefinition<User> schemaDefinition = SchemaDefinition.<User>builder().withPojo(User.class).build();
   Producer<User> producer = client.newProducer(Schema.AVRO(schemaDefinition)).create();
   producer.newMessage().value(new User ("pulsar-user", 1)).send();
   ```

3. Create a consumer with a `SchemaDefinition` schema and receive messages.

   ```java
   SchemaDefinition<User> schemaDefinition = SchemaDefinition.<User>builder().withPojo(User.class).build();
   Consumer<User> consumer = client.newConsumer(Schema.AVRO(schemaDefinition)).subscribe();
   User user = consumer.receive().getValue();
   ```

</TabItem>
</Tabs>
````

### Auto Schema

If there is no chance to know the schema type of a Pulsar topic in advance, you can use AUTO schemas to produce/consume generic records to/from brokers.

Auto schema contains two categories:
* `AUTO_PRODUCE` transfers data from a producer to a Pulsar topic that has a schema and helps the producer validate whether the outbound bytes are compatible with the schema of the topic. For more instructions, see [Construct an AUTO_PRODUCE schema](schema-get-started.md#auto_produce).
* `AUTO_CONSUME` transfers data from a Pulsar topic that has a schema to a consumer and helps the topic validate whether the out-bound bytes are compatible with the consumer. In other words, the topic deserializes messages into language-specific objects `GenericRecord` using the `SchemaInfo` retrieved from brokers. For more instructions, see [Construct an AUTO_CONSUME schema](schema-get-started.md#auto_consume).

## Schema validation enforcement

Schema validation enforcement enables brokers to reject producers/consumers without a schema.

By default, schema validation enforcement is only **disabled** (`isSchemaValidationEnforced`=`false`) for producers, which means:
* A producer without a schema can produce any messages to a topic with schemas, which may result in producing trash data to the topic. 
* Clients that don’t support schema are allowed to produce messages to a topic with schemas.

For how to enable schema validation enforcement, see [Manage schema validation](admin-api-schemas.md#manage-schema-validation).

## Schema evolution

Schemas store the details of attributes and types. To satisfy new business needs, schemas undergo evolution over time with [versioning](#schema-versioning). 

:::note

Schema evolution only applies to Avro, JSON, Protobuf, and ProtobufNative schemas. 

:::

Schema evolution may impact existing consumers. The following control measures have been designed to serve schema evolution and ensure the downstream consumers can seamlessly handle schema evolution:
* [Schema compatibility check](#schema-compatibility-check)
* [Schema `AutoUpdate`](#schema-autoupdate)

For further readings about schema evolution, see [Avro documentation](https://avro.apache.org/docs/1.10.2/spec.html#Schema+Resolution) and [Protobuf documentation](https://developers.google.com/protocol-buffers/docs/proto#optional).

### Schema versioning

Each `SchemaInfo` stored with a topic has a version. The schema version manages schema changes happening within a topic. 

Messages produced with `SchemaInfo` are tagged with a schema version. When a message is consumed by a Pulsar client, the client can use the schema version to retrieve the corresponding `SchemaInfo` and use the correct schema to deserialize data. Once a version is assigned to or fetched from a schema, all subsequent messages produced by that producer are tagged with the appropriate version.

Suppose you are using a Pulsar [Java client](client-libraries-java.md) to create a producer and send messages.

```java
PulsarClient client = PulsarClient.builder()
        .serviceUrl("pulsar://localhost:6650")
        .build();

Producer<SensorReading> producer = client.newProducer(JSONSchema.of(SensorReading.class))
        .topic("sensor-data")
        .sendTimeout(3, TimeUnit.SECONDS)
        .create();
```

The table below outlines the possible scenarios when this connection attempt occurs and the result of each scenario:

| Scenario                                                                                                        | Result                                                                                                                                                                                                                                                                                                                      |
|-----------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| <li>No schema exists for the topic. </li>                                                                       | (1) The producer is created with the given schema. <br /> (2) The schema is transmitted to the broker and stored since there is no existing schema. <br /> (3) Any consumer created using the same schema or topic can consume messages from the `sensor-data` topic.                                                       |
| <li>A schema already exists. </li><li>The producer connects using the same schema that is already stored. </li> | (1) The schema is transmitted to the broker.<br />  (2) The broker determines that the schema is compatible. <br /> (3) The broker attempts to store the schema in [BookKeeper](concepts-architecture-overview.md#persistent-storage) but then determines that it's already stored, so it is used to tag produced messages. |
| <li>A schema already exists. </li><li>The producer connects using a new schema that is compatible. </li>        | (1) The schema is transmitted to the broker. <br /> (2) The broker determines that the schema is compatible and stores the new schema as the current version (with a new version number).                                                                                                                                   |

### Schema compatibility check

The purpose of schema compatibility check is to ensure that existing consumers can process the introduced messages.

When receiving a `SchemaInfo` from producers, brokers recognize the schema type and deploy the schema compatibility checker ([`schemaRegistryCompatibilityCheckers`](https://github.com/apache/pulsar/blob/bf194b557c48e2d3246e44f1fc28876932d8ecb8/pulsar-broker-common/src/main/java/org/apache/pulsar/broker/ServiceConfiguration.java)) for that schema type to check if the `SchemaInfo` is compatible with the schema of the topic by applying the configured compatibility check strategy. 

The default value of `schemaRegistryCompatibilityCheckers` in the `conf/broker.conf` file is as follows.
   
```properties
schemaRegistryCompatibilityCheckers=org.apache.pulsar.broker.service.schema.JsonSchemaCompatibilityCheck,org.apache.pulsar.broker.service.schema.AvroSchemaCompatibilityCheck,org.apache.pulsar.broker.service.schema.ProtobufNativeSchemaCompatibilityCheck
```

Each schema type corresponds to one instance of the schema compatibility checker. Avro, JSON, and Protobuf schemas have their own compatibility checkers, while all the other schema types share the default compatibility checker that disables the schema evolution.

#### Schema compatibility check strategy

Suppose that you have a topic containing three schemas (V1, V2, and V3). V1 is the oldest and V3 is the latest. The following table outlines 8 schema compatibility strategies and how it works.

|  Compatibility check strategy  |   Definition  |   Changes allowed  |   Check against which schema  |
| --- | --- | --- | --- |
|  `ALWAYS_COMPATIBLE`  |   Disable schema compatibility check.  |   All changes are allowed  |   All previous versions  |
|  `ALWAYS_INCOMPATIBLE`  |   Disable schema evolution, that is, any schema change is rejected.  |   No change is allowed  |   N/A  | 
|  `BACKWARD`  |   Consumers using schema V3 can process data written by producers using the **last schema version** V2.  |   <li>Add optional fields </li><li>Delete fields </li> |   Latest version  |
|  `BACKWARD_TRANSITIVE`  |   Consumers using schema V3 can process data written by producers using **all previous schema versions** V2 and V1.  |   <li>Add optional fields </li><li>Delete fields </li> |   All previous versions  |
|  `FORWARD`  |   Consumers using the **last schema version** V2 can process data written by producers using a new schema V3, even though they may not be able to use the full capabilities of the new schema.  |   <li>Add fields </li><li>Delete optional fields </li> |   Latest version  |
|  `FORWARD_TRANSITIVE`  |   Consumers using **all previous schema versions** V2 or V1 can process data written by producers using a new schema V3.  |   <li>Add fields </li><li>Delete optional fields </li> |   All previous versions  |
|  `FULL`  |   Schemas are both backward and forward compatible. <li>Consumers using the last schema V2 can process data written by producers using the new schema V3. </li><li>Consumers using the new schema V3 can process data written by producers using the last schema V2.</li>  |   Modify optional fields |   Latest version  | 
|  `FULL_TRANSITIVE`  |   Backward and forward compatible among schema V3, V2, and V1. <li>Consumers using the schema V3 can process data written by producers using schema V2 and V1. </li><li>Consumers using the schema V2 or V1 can process data written by producers using the schema V3.</li>  |   Modify optional fields |   All previous versions  |

:::tip

* The default schema compatibility check strategy varies depending on schema types.
  * For Avro and JSON, the default one is `FULL`.
  * For others, the default one is `ALWAYS_INCOMPATIBLE`.
* For more instructions about how to set the strategy, see [Manage schemas](admin-api-schemas.md#set-schema-compatibility-check-strategy).

:::

### Schema AutoUpdate

By default, schema `AutoUpdate` is enabled. When a schema passes the schema compatibility check, the producer automatically updates this schema to the topic it produces. 

#### Producer side

For a producer, the `AutoUpdate` happens in the following cases:

* If a **topic doesn’t have a schema** (meaning the data is in raw bytes), Pulsar registers the schema automatically.

* If a **topic has a schema** and the **producer doesn’t carry any schema** (meaning it produces raw bytes):

    * If [schema validation enforcement](#schema-validation-enforcement) is **disabled** (`schemaValidationEnforced`=`false`) in the namespace that the topic belongs to, the producer is allowed to connect to the topic and produce data. 
  
    * Otherwise, the producer is rejected.

  * If a **topic has a schema** and the **producer carries a schema**, see [How schema works on producer side](schema-overview.md#producer-side) for more information.

#### Consumer side

For a consumer, the `AutoUpdate` happens in the following cases:

* If a consumer connects to a topic **without a schema** (meaning it consumes raw bytes), the consumer can connect to the topic successfully without doing any compatibility check.

* If a consumer connects to a topic **with a schema**, see [How schema works on consumer side](schema-overview.md#consumer-side) for more information.

### Order of upgrading clients

To adapt to schema evolution and auto-update, you need to upgrade your client applications accordingly. The upgrade order may vary depending on the configured [schema compatibility check strategy](#schema-compatibility-check-strategy).

The following table outlines the mapping between the schema compatibility check strategy and the upgrade order of clients.

|  Compatibility check strategy  |   Upgrade order  | Description                                                                                                                                                                                                                                                                                                         | 
| --- | --- |---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|  `ALWAYS_COMPATIBLE`  |   Any order  | The compatibility check is disabled. Consequently, you can upgrade the producers and consumers in **any order**.                                                                                                                                                                                                    | 
|  `ALWAYS_INCOMPATIBLE`  |   N/A  | The schema evolution is disabled.                                                                                                                                                                                                                                                                                   | 
|  <li>`BACKWARD` </li><li>`BACKWARD_TRANSITIVE` </li> |   Consumer first  | There is no guarantee that consumers using the old schema can read data produced using the new schema. Consequently, **upgrade all consumers first**, and then start producing new data.                                                                                                                            | 
|  <li>`FORWARD` </li><li>`FORWARD_TRANSITIVE` </li> |   Producer first  | There is no guarantee that consumers using the new schema can read data produced using the old schema. Consequently, **upgrade all producers first** to use the new schema and ensure the data that has already been produced using the old schemas are not available to consumers, and then upgrade the consumers. | 
|  <li>`FULL` </li><li>`FULL_TRANSITIVE` </li> |   Any order  | It is guaranteed that consumers using the old schema can read data produced using the new schema and consumers using the new schema can read data produced using the old schema. Consequently, you can upgrade the producers and consumers in **any order**.                                                        |
