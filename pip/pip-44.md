# PIP-44: Separate schema compatibility checker for producer and consumer

* **Status**: Adopted, Release 2.5.0
* **Author**: Bo Cong, Penghui Li
* **Pull Request**: 
* **Mailing List discussion**:
* **Release**: 

## Motivation

Pulsar schema use one schema compatibility checker for producers and consumers, this caused problems when using pulsar schema. For example, [issue-4737](https://github.com/apache/pulsar/issues/4737) illustrates that producers can’t publish messages with the correct schema and this is a very typical use case while using multi-version schema. So, this proposal will discuss some issues we found when using multi-version schema and how to handle them properly in pulsar. 

## Issues

The documentation of [schema evolution and compatibility]([http://pulsar.apache.org/docs/en/next/schema-evolution-compatibility](http://pulsar.apache.org/docs/en/next/schema-evolution-compatibility/))describes the details of compatibility guarantees currently Pulsar has. We analyze the issues encountered for different compatibility strategies based on the current implementation.

First of all, the following table illustrates the current schema compatibility check logic for producers and consumers when using muti-version schema.

| Compatibility Check Strategy | Check for producers/consumers                        |
| ---------------------------- | ---------------------------------------------------- |
| BACKWARD                     | Client schema **can read** the last version schema   |
| BACKWARD_TRANSITIVE          | Client schema **can read** all schemas               |
| FORWARD                      | Client schema **can be read** by last version schema |
| FORWARD_TRANSITIVE           | Client schema **can be read** by all schemas         |
| FULL                         | Both BACKWARD and FORWARD                            |
| FULL_TRANSITIVE              | Both BACKWARD_TRANSITIVE and FORWARD_TRANSITIVE      |

Suppose that you have a topic containing three schemas (V1, V2, and V3)

**BACKWARD**

Current implementation works well for consumers. But for producers, if a producer with schema V2 attempt to connect to broker, the schema compatibility checker checks the client schema V2 can read the last schema (V3), this will lead to incompatible schema exception. 

**BACKWARD_TRANSITIVE**

Same as  BACKWARD, Current implementation works well for consumers. But for producers, if a producer with schema V2 attempts to connect to broker, the schema compatibility checker checks  whether the client schema V2 can read all existing schemas (V1, V2, V3), this will potentially lead to incompatible schema exception. 

**FORWARD**

Current implementation works well for producers. But for consumers, if a consumer with schema V2 attempts to connect to broker, the schema compatibility checker checks the client schema V2 can be read by the last schema (V3), this will lead to incompatible schema exception.

**FORWARD_TRANSITIVE**

Same as  FORWARD, current implementation works well for producers. But for consumers, if a consumer with schema V1 attempts to connect to broker, the schema compatibility checker checks the client schema V1 can be read by all exists schemas (V1, V2, V3), this will lead to incompatible schema exception.

**FULL and FULL_TRANSITIVE**

Current implementation works well for producers and consumers.

## Approach

So in this proposal, we are proposing a new approach to separate the schema compatibility check logic into two parts:

1. Schema compatibility check for schema evolution
2. Schema verification for producers and consumers

**Schema verification for producers**

For a producer which tries to connect to a topic (omitting the schema auto creation part), Broker has to do the following checks:

1. Check if the schema carried by the producer exists in the schema registry or not.
2. If the schema is already registered, allow producers connects and produce messages with that schema.
3. If the schema is not registered yet, verify if the producer schema is allowed to be registered according to the configured compatibility check strategy. 

**Schema verification for consumers**

Different from Producers, when a consumer connects to a topic, Pulsar brokers will check if the carried schema is compatible with the registered schemas according to the configured schema compatibility check strategy.

| Compatibility Check Strategy | Check logic          |
| ---------------------------- | -------------------- |
| BACKWARD                     | Can read last schema |
| BACKWARD_TRANSITIVE          | Can read all schemas |
| FORWARD                      | Can read last schema |
| FORWARD_TRANSITIVE           | Can read last schema |
| FULL                         | Can read last schema |
| FULL_TRANSITIVE              | Can read all schemas |

## Test Plan

Existing tests don't work well with this PIP, need add new tests for the PIP
