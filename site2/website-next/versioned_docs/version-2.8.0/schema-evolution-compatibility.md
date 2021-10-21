---
id: schema-evolution-compatibility
title: Schema evolution and compatibility
sidebar_label: Schema evolution and compatibility
original_id: schema-evolution-compatibility
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


Normally, schemas do not stay the same over a long period of time. Instead, they undergo evolutions to satisfy new needs. 

This chapter examines how Pulsar schema evolves and what Pulsar schema compatibility check strategies are.

## Schema evolution

Pulsar schema is defined in a data structure called `SchemaInfo`. 

Each `SchemaInfo` stored with a topic has a version. The version is used to manage the schema changes happening within a topic. 

The message produced with `SchemaInfo` is tagged with a schema version. When a message is consumed by a Pulsar client, the Pulsar client can use the schema version to retrieve the corresponding `SchemaInfo` and use the correct schema information to deserialize data.

### What is schema evolution?

Schemas store the details of attributes and types. To satisfy new business requirements,  you need to update schemas inevitably over time, which is called **schema evolution**. 

Any schema changes affect downstream consumers. Schema evolution ensures that the downstream consumers can seamlessly handle data encoded with both old schemas and new schemas. 

### How Pulsar schema should evolve? 

The answer is Pulsar schema compatibility check strategy. It determines how schema compares old schemas with new schemas in topics.

For more information, see [Schema compatibility check strategy](#schema-compatibility-check-strategy).

### How does Pulsar support schema evolution?

1. When a producer/consumer/reader connects to a broker, the broker deploys the schema compatibility checker configured by `schemaRegistryCompatibilityCheckers` to enforce schema compatibility check. 

    The schema compatibility checker is one instance per schema type. 
    
    Currently, Avro and JSON have their own compatibility checkers, while all the other schema types share the default compatibility checker which disables schema evolution.

2. The producer/consumer/reader sends its client `SchemaInfo` to the broker. 
   
3. The broker knows the schema type and locates the schema compatibility checker for that type. 

4. The broker uses the checker to check if the `SchemaInfo` is compatible with the latest schema of the topic by applying its compatibility check strategy. 
   
   Currently, the compatibility check strategy is configured at the namespace level and applied to all the topics within that namespace.

## Schema compatibility check strategy

Pulsar has 8 schema compatibility check strategies, which are summarized in the following table.

Suppose that you have a topic containing three schemas (V1, V2, and V3), V1 is the oldest and V3 is the latest:

|  Compatibility check strategy  |   Definition  |   Changes allowed  |   Check against which schema  |   Upgrade first  | 
| --- | --- | --- | --- | --- |
|  `ALWAYS_COMPATIBLE`  |   Disable schema compatibility check.  |   All changes are allowed  |   All previous versions  |   Any order  | 
|  `ALWAYS_INCOMPATIBLE`  |   Disable schema evolution.  |   All changes are disabled  |   None  |   None  | 
|  `BACKWARD`  |   Consumers using the schema V3 can process data written by producers using the schema V3 or V2.  |   * Add optional fields * Delete fields  |   Latest version  |   Consumers  | 
|  `BACKWARD_TRANSITIVE`  |   Consumers using the schema V3 can process data written by producers using the schema V3, V2 or V1.  |   * Add optional fields * Delete fields  |   All previous versions  |   Consumers  | 
|  `FORWARD`  |   Consumers using the schema V3 or V2 can process data written by producers using the schema V3.  |   * Add fields * Delete optional fields  |   Latest version  |   Producers  | 
|  `FORWARD_TRANSITIVE`  |   Consumers using the schema V3, V2 or V1 can process data written by producers using the schema V3.  |   * Add fields * Delete optional fields  |   All previous versions  |   Producers  | 
|  `FULL`  |   Backward and forward compatible between the schema V3 and V2.  |   * Modify optional fields  |   Latest version  |   Any order  | 
|  `FULL_TRANSITIVE`  |   Backward and forward compatible among the schema V3, V2, and V1.  |   * Modify optional fields  |   All previous versions  |   Any order  | 

### ALWAYS_COMPATIBLE and ALWAYS_INCOMPATIBLE 

|  Compatibility check strategy  |   Definition  |   Note  | 
| --- | --- | --- |
|  `ALWAYS_COMPATIBLE`  |   Disable schema compatibility check.  |   None  | 
|  `ALWAYS_INCOMPATIBLE`  |   Disable schema evolution, that is, any schema change is rejected.  |   * For all schema types except Avro and JSON, the default schema compatibility check strategy is `ALWAYS_INCOMPATIBLE`. * For Avro and JSON, the default schema compatibility check strategy is `FULL`.  |  

#### Example 
  
* Example  1
  
    In some situations, an application needs to store events of several different types in the same Pulsar topic. 

    In particular, when developing a data model in an `Event Sourcing` style, you might have several kinds of events that affect the state of an entity. 

    For example, for a user entity, there are `userCreated`, `userAddressChanged` and `userEnquiryReceived` events. The application requires that those events are always read in the same order. 

    Consequently, those events need to go in the same Pulsar partition to maintain order. This application can use `ALWAYS_COMPATIBLE` to allow different kinds of events co-exist in the same topic.

* Example 2

    Sometimes we also make incompatible changes. 

    For example, you are modifying a field type from `string` to `int`.

    In this case, you need to:

    * Upgrade all producers and consumers to the new schema versions at the same time.

    * Optionally, create a new topic and start migrating applications to use the new topic and the new schema, avoiding the need to handle two incompatible versions in the same topic.

### BACKWARD and BACKWARD_TRANSITIVE 

Suppose that you have a topic containing three schemas (V1, V2, and V3), V1 is the oldest and V3 is the latest:

| Compatibility check strategy | Definition  | Description |
|---|---|---|
`BACKWARD` | Consumers using the new schema can process data written by producers using the **last schema**. | The consumers using the schema V3 can process data written by producers using the schema V3 or V2. |
`BACKWARD_TRANSITIVE` | Consumers using the new schema can process data written by producers using **all previous schemas**. | The consumers using the schema V3 can process data written by producers using the schema V3, V2, or V1. |

#### Example  
  
* Example 1
  
    Remove a field.
    
    A consumer constructed to process events without one field can process events written with the old schema containing the field, and the consumer will ignore that field.

* Example 2
  
    You want to load all Pulsar data into a Hive data warehouse and run SQL queries against the data. 

    Same SQL queries must continue to work even the data is changed. To support it, you can evolve the schemas using the `BACKWARD` strategy.

### FORWARD and FORWARD_TRANSITIVE 

Suppose that you have a topic containing three schemas (V1, V2, and V3), V1 is the oldest and V3 is the latest:

| Compatibility check strategy | Definition | Description |
|---|---|---|
`FORWARD` | Consumers using the **last schema** can process data written by producers using a new schema, even though they may not be able to use the full capabilities of the new schema. | The consumers using the schema V3 or V2 can process data written by producers using the schema V3. |
`FORWARD_TRANSITIVE` | Consumers using **all previous schemas** can process data written by producers using a new schema. | The consumers using the schema V3, V2, or V1 can process data written by producers using the schema V3. 

#### Example  
  
* Example 1
  
  Add a field.
  
  In most data formats, consumers written to process events without new fields can continue doing so even when they receive new events containing new fields.

* Example 2
  
  If a consumer has an application logic tied to a full version of a schema, the application logic may not be updated instantly when the schema evolves.
  
  In this case, you need to project data with a new schema onto an old schema that the application understands. 
  
  Consequently, you can evolve the schemas using the `FORWARD` strategy to ensure that the old schema can process data encoded with the new schema.

### FULL and FULL_TRANSITIVE 

Suppose that you have a topic containing three schemas (V1, V2, and V3), V1 is the oldest and V3 is the latest:

|  Compatibility check strategy  |   Definition  |   Description  |   Note  | 
| --- | --- | --- | --- |
|  `FULL`  |   Schemas are both backward and forward compatible, which means: Consumers using the last schema can process data written by producers using the new schema. AND Consumers using the new schema can process data written by producers using the last schema.  |   Consumers using the schema V3 can process data written by producers using the schema V3 or V2. AND Consumers using the schema V3 or V2 can process data written by producers using the schema V3.  |   * For Avro and JSON, the default schema compatibility check strategy is `FULL`. * For all schema types except Avro and JSON, the default schema compatibility check strategy is `ALWAYS_INCOMPATIBLE`.  | 
|  `FULL_TRANSITIVE`  |   The new schema is backward and forward compatible with all previously registered schemas.  |   Consumers using the schema V3 can process data written by producers using the schema V3, V2 or V1. AND Consumers using the schema V3, V2 or V1 can process data written by producers using the schema V3.  |   None  | 

#### Example  

In some data formats, for example, Avro, you can define fields with default values. Consequently, adding or removing a field with a default value is a fully compatible change.

## Schema verification

When a producer or a consumer tries to connect to a topic, a broker performs some checks to verify a schema.

### Producer

When a producer tries to connect to a topic (suppose ignore the schema auto creation), a broker does the following checks:

* Check if the schema carried by the producer exists in the schema registry or not.

    * If the schema is already registered, then the producer is connected to a broker and produce messages with that schema.
    
    * If the schema is not registered, then Pulsar verifies if the schema is allowed to be registered based on the configured compatibility check strategy.
    
### Consumer
When a consumer tries to connect to a topic, a broker checks if a carried schema is compatible with a registered schema based on the configured schema compatibility check strategy.

|  Compatibility check strategy  |   Check logic  | 
| --- | --- |
|  `ALWAYS_COMPATIBLE`  |   All pass  | 
|  `ALWAYS_INCOMPATIBLE`  |   No pass  | 
|  `BACKWARD`  |   Can read the last schema  | 
|  `BACKWARD_TRANSITIVE`  |   Can read all schemas  | 
|  `FORWARD`  |   Can read the last schema  | 
|  `FORWARD_TRANSITIVE`  |   Can read the last schema  | 
|  `FULL`  |   Can read the last schema  | 
|  `FULL_TRANSITIVE`  |   Can read all schemas  | 

## Order of upgrading clients

The order of upgrading client applications is determined by the compatibility check strategy.

For example, the producers using schemas to write data to Pulsar and the consumers using schemas to read data from Pulsar. 

|  Compatibility check strategy  |   Upgrade first  |   Description  | 
| --- | --- | --- |
|  `ALWAYS_COMPATIBLE`  |   Any order  |   The compatibility check is disabled. Consequently, you can upgrade the producers and consumers in **any order**.  | 
|  `ALWAYS_INCOMPATIBLE`  |   None  |   The schema evolution is disabled.  | 
|  * `BACKWARD` * `BACKWARD_TRANSITIVE`  |   Consumers  |   There is no guarantee that consumers using the old schema can read data produced using the new schema. Consequently, **upgrade all consumers first**, and then start producing new data.  | 
|  * `FORWARD` * `FORWARD_TRANSITIVE`  |   Producers  |   There is no guarantee that consumers using the new schema can read data produced using the old schema. Consequently, **upgrade all producers first** to use the new schema and ensure that the data already produced using the old schemas are not available to consumers, and then upgrade the consumers.  | 
|  * `FULL` * `FULL_TRANSITIVE`  |   Any order  |   There is no guarantee that consumers using the old schema can read data produced using the new schema and consumers using the new schema can read data produced using the old schema. Consequently, you can upgrade the producers and consumers in **any order**.  | 




