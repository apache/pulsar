---
id: schema-evolution-compatibility
title: Schema evolution and compatibility
sidebar_label: "Schema evolution and compatibility"
---

Normally, schemas do not stay the same over a long period of time. Instead, they undergo evolutions to satisfy new needs. 

This chapter introduces how Pulsar schema evolves and what compatibility check strategies it adopts.

## Schema evolution

The message produced with `SchemaInfo` is tagged with a schema version. When a message is consumed by a Pulsar client, the Pulsar client can use the schema version to retrieve the corresponding `SchemaInfo` and use the correct schema information to deserialize data.

Schemas store the details of attributes and types. To satisfy new business requirements,  you need to update schemas inevitably over time, which is called **schema evolution**. 

Any schema changes affect downstream consumers. Schema evolution ensures that the downstream consumers can seamlessly handle data encoded with both old schemas and new schemas. 

### How schema evolves? 

The answer is [schema compatibility check strategy](#schema-compatibility-check-strategy). It determines how schema compares old schemas with new schemas in topics.

### How Pulsar supports schema evolution?

The process of how Pulsar supports schema evolution is described as follows.

1. The producer/consumer/reader sends the `SchemaInfo` of its client to brokers. 
   
2. Brokers recognize the schema type and deploy the schema compatibility checker `schemaRegistryCompatibilityCheckers` for that schema type to enforce the schema compatibility check. By default, the value of `schemaRegistryCompatibilityCheckers` in the `conf/broker.conf` or `conf/standalone.conf` file is as follows.
   
   ```properties
   schemaRegistryCompatibilityCheckers=org.apache.pulsar.broker.service.schema.JsonSchemaCompatibilityCheck,org.apache.pulsar.broker.service.schema.AvroSchemaCompatibilityCheck,org.apache.pulsar.broker.service.schema.ProtobufNativeSchemaCompatibilityCheck
   ```

   :::note

   Each schema type corresponds to one instance of schema compatibility checker. Currently, Avro, JSON, and Protobuf have their own compatibility checkers, while all the other schema types share the default compatibility checker which disables the schema evolution. In a word, schema evolution is only available in Avro, JSON, and Protobuf schema.

   :::

3. Brokers use the schema compatibility checker to check if the `SchemaInfo` is compatible with the latest schema of the topic by applying its [compatibility check strategy](#schema-compatibility-check-strategy). Currently, the compatibility check strategy is configured at the namespace level and applied to all the topics within that namespace.

For more details, see [`schemaRegistryCompatibilityCheckers`](https://github.com/apache/pulsar/blob/bf194b557c48e2d3246e44f1fc28876932d8ecb8/pulsar-broker-common/src/main/java/org/apache/pulsar/broker/ServiceConfiguration.java).


## Schema compatibility check strategy

The following table outlines 8 schema compatibility check strategies and how it works.

Suppose that you have a topic containing three schemas (V1, V2, and V3). V1 is the oldest and V3 is the latest.

|  Compatibility check strategy  |   Definition  |   Changes allowed  |   Check against which schema  |   Upgrade first  | 
| --- | --- | --- | --- | --- |
|  `ALWAYS_COMPATIBLE`  |   Disable schema compatibility check.  |   All changes are allowed  |   All previous versions  |   Any order  | 
|  `ALWAYS_INCOMPATIBLE`  |   Disable schema evolution, that is, any schema change is rejected.  |   All changes are disabled  |   None  |   None  | 
|  `BACKWARD`  |   Consumers using schema V3 can process data written by producers using the **last schema version** V2.  |   <li>Add optional fields </li><li>Delete fields </li> |   Latest version  |   Consumers  | 
|  `BACKWARD_TRANSITIVE`  |   Consumers using schema V3 can process data written by producers using **all previous schema versions** V2 and V1.  |   <li>Add optional fields </li><li>Delete fields </li> |   All previous versions  |   Consumers  | 
|  `FORWARD`  |   Consumers using the **last schema version** V2 can process data written by producers using a new schema V3, even though they may not be able to use the full capabilities of the new schema.  |   <li>Add fields </li><li>Delete optional fields </li> |   Latest version  |   Producers  | 
|  `FORWARD_TRANSITIVE`  |   Consumers using **all previous schema versions** V2 or V1 can process data written by producers using a new schema V3.  |   <li>Add fields </li><li>Delete optional fields </li> |   All previous versions  |   Producers  | 
|  `FULL`  |   Schemas are both backward and forward compatible. <li>Consumers using the last schema V2 can process data written by producers using the new schema V3. </li><li>Consumers using the new schema V3 can process data written by producers using the last schema V2.</li>  |   <li>Modify optional fields </li> |   Latest version  |   Any order  | 
|  `FULL_TRANSITIVE`  |   Backward and forward compatible among schema V3, V2, and V1. <li>Consumers using the schema V3 can process data written by producers using schema V2 and V1. </li><li>Consumers using the schema V2 or V1 can process data written by producers using the schema V3.</li>  |   <li>Modify optional fields </li> |   All previous versions  |   Any order  | 

:::tip

* The default schema compatibility check strategy varies depending on schema types.
  * For Avro and JSON, the default one is `FULL`.
  * For others, the default one is `ALWAYS_INCOMPATIBLE`.
* You can set schema compatibility check strategy at the topic, namespace or broker level. For how to set the strategy, see [here](admin-api-schemas.md#set-schema-compatibility-check-strategy).

:::

### ALWAYS_COMPATIBLE example
  
  In some situations, an application needs to store events of several different types in the same Pulsar topic. 

  In particular, when developing a data model in an `Event Sourcing` style, you might have several kinds of events that affect the state of an entity. 

  For example, for a user entity, there are `userCreated`, `userAddressChanged` and `userEnquiryReceived` events. The application requires that those events are always read in the same order. 

  Consequently, those events need to go in the same Pulsar partition to maintain order. This application can use `ALWAYS_COMPATIBLE` to allow different kinds of events to co-exist on the same topic.

### ALWAYS_INCOMPATIBLE example

  Sometimes we also make incompatible changes. For example, you are modifying a field type from `string` to `int`.

  In this case, you need to:

  * Upgrade all producers and consumers to the new schema versions at the same time.

  * Optionally, create a new topic and start migrating applications to use the new topic and the new schema, avoiding the need to handle two incompatible versions in the same topic.

### BACKWARD and BACKWARD_TRANSITIVE example
  
* Example 1
  
  Remove a field.
  
  A consumer constructed to process events without one field can process events written with the old schema containing the field, and the consumer will ignore that field.

* Example 2
  
  You want to load all Pulsar data into a Hive data warehouse and run SQL queries against the data. 

  Same SQL queries must continue to work even if the data is changed. To support it, you can evolve the schemas using the `BACKWARD` strategy.

### FORWARD and FORWARD_TRANSITIVE example

* Example 1
  
  Add a field.
  
  In most data formats, consumers written to process events without new fields can continue doing so even when they receive new events containing new fields.

* Example 2
  
  If a consumer has an application logic tied to a full version of a schema, the application logic may not be updated instantly when the schema evolves.
  
  In this case, you need to project data with a new schema onto an old schema that the application understands. 
  
  Consequently, you can evolve the schemas using the `FORWARD` strategy to ensure that the old schema can process data encoded with the new schema.

### FULL and FULL_TRANSITIVE example

In some data formats, for example, Avro, you can define fields with default values. Consequently, adding or removing a field with a default value is a fully compatible change.


## Schema validation

When a producer or a consumer tries to connect to a topic, a broker performs some checks to verify a schema.

### Validation on producers

By default, `schemaValidationEnforced` is **disabled** for producers, which means:
* A producer without a schema can produce any kind of messages to a topic with schemas, which may result in producing trash data to the topic. 
* It allows non-java language clients that don’t support schema can produce messages to a topic with schemas.

However, if you want a stronger guarantee on the topics with schemas, you can enable `schemaValidationEnforced` across the whole cluster or on a per-namespace basis. 

With `schemaValidationEnforced` enabled, When a producer tries to connect to a topic (suppose ignore the schema auto-creation), the broker checks if the schema carried by the producer exists in the schema registry or not.
  * If the schema is already registered, then the producer is connected to a broker and produces messages with that schema.
  * If the schema is not registered, then Pulsar verifies if the schema is allowed to be registered based on the configured compatibility check strategy.
  
### Validation on consumers

When a consumer tries to connect to a topic, a broker checks if a carried schema is compatible with a registered schema based on the configured schema compatibility check strategy.

| Compatibility check strategy | Check logic              |
|------------------------------|--------------------------|
| `ALWAYS_COMPATIBLE`          | All pass                 |
| `ALWAYS_INCOMPATIBLE`        | No pass                  |
| `BACKWARD`                   | Can read the last schema |
| `BACKWARD_TRANSITIVE`        | Can read all schemas     |
| `FORWARD`                    | Can read the last schema |
| `FORWARD_TRANSITIVE`         | Can read the last schema |
| `FULL`                       | Can read the last schema |
| `FULL_TRANSITIVE`            | Can read all schemas     |

## Order of upgrading clients

The order of upgrading client applications is determined by the [schema compatibility check strategy](#schema-compatibility-check-strategy).

For example, the producers use schemas to write data to Pulsar and the consumers use schemas to read data from Pulsar. 

|  Compatibility check strategy  |   Upgrade first  | Description                                                                                                                                                                                                                                                                                                         | 
| --- | --- |---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|  `ALWAYS_COMPATIBLE`  |   Any order  | The compatibility check is disabled. Consequently, you can upgrade the producers and consumers in **any order**.                                                                                                                                                                                                    | 
|  `ALWAYS_INCOMPATIBLE`  |   None  | The schema evolution is disabled.                                                                                                                                                                                                                                                                                   | 
|  <li>`BACKWARD` </li><li>`BACKWARD_TRANSITIVE` </li> |   Consumers  | There is no guarantee that consumers using the old schema can read data produced using the new schema. Consequently, **upgrade all consumers first**, and then start producing new data.                                                                                                                            | 
|  <li>`FORWARD` </li><li>`FORWARD_TRANSITIVE` </li> |   Producers  | There is no guarantee that consumers using the new schema can read data produced using the old schema. Consequently, **upgrade all producers first**<li>to use the new schema and ensure that the data already produced using the old schemas are not available to consumers, and then upgrades the consumers. </li> | 
|  <li>`FULL` </li><li>`FULL_TRANSITIVE` </li> |   Any order  | It is guaranteed that consumers using the old schema can read data produced using the new schema and consumers using the new schema can read data produced using the old schema. Consequently, you can upgrade the producers and consumers in **any order**.                                                        | 