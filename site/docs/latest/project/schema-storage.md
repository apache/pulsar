---
title: Custom schema storage
tags: [schema, schema registry]
---

By default, Pulsar stores data type [schemas](../../getting-started/ConceptsAndArchitecture#schema-registry) in [Apache BookKeeper](https://bookkeeper.apache.org) (which is deployed alongside Pulsar). You can, however, use another storage system if you wish. This

## Interface

In order to use a non-default (i.e. non-BookKeeper) storage system for Pulsar schemas, you need to implement two Java interfaces: [`SchemaStorage`]() and [`SchemaStorageFactory`]()

### `SchemaStorage`

## Implementation

```java

```

## Deployment