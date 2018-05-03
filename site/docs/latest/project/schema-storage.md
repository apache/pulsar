---
title: Custom schema storage
tags: [schema, schema registry]
---

By default, Pulsar stores data type [schemas](../../getting-started/ConceptsAndArchitecture#schema-registry) in [Apache BookKeeper](https://bookkeeper.apache.org) (which is deployed alongside Pulsar). You can, however, use another storage system if you wish. This doc walks you through creating your own schema storage implementation.

## Interface

In order to use a non-default (i.e. non-BookKeeper) storage system for Pulsar schemas, you need to implement two Java interfaces: [`SchemaStorage`](#schema-storage) and [`SchemaStorageFactory`](#schema-storage-factory).

### The `SchemaStorage` interface {#schema-storage}

```java
public interface SchemaStorage {

    CompletableFuture<SchemaVersion> put(String key, byte[] value, byte[] hash);

    CompletableFuture<StoredSchema> get(String key, SchemaVersion version);

    CompletableFuture<SchemaVersion> delete(String key);

    SchemaVersion versionFromBytes(byte[] version);

    void start() throws Exception;

    void close() throws Exception;
}
```

### The `SchemaStorageFactory` interface {#schema-storage-factory}

```java
public interface SchemaStorageFactory {
    @NotNull
    SchemaStorage create(PulsarService pulsar) throws Exception;
}
```

## Implementation

You can implement these 

```java

```

## Deployment

In order to use your custom schema storage implementation, you'll need to:

1. Package the implementation in a [JAR](https://docs.oracle.com/javase/tutorial/deployment/jar/basicsindex.html) file.
1. Add that jar to the `lib` folder in your Pulsar [binary or source distribution](../../getting-started/LocalCluster#installing-pulsar).
1. Start up Pulsar.