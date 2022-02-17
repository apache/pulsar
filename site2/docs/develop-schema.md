---
id: develop-schema
title: Custom schema storage
sidebar_label: Custom schema storage
original_id: develop-schema
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


By default, Pulsar stores data type [schemas](concepts-schema-registry) in [Apache BookKeeper](https://bookkeeper.apache.org) (which is deployed alongside Pulsar). You can, however, use another storage system if you wish. This doc walks you through creating your own schema storage implementation.

In order to use a non-default (i.e. non-BookKeeper) storage system for Pulsar schemas, you need to implement two Java interfaces: [`SchemaStorage`](#schemastorage-interface) and [`SchemaStorageFactory`](#schemastoragefactory-interface).

## SchemaStorage interface

The `SchemaStorage` interface has the following methods:

```java

public interface SchemaStorage {
    // How schemas are updated
    CompletableFuture<SchemaVersion> put(String key, byte[] value, byte[] hash);

    // How schemas are fetched from storage
    CompletableFuture<StoredSchema> get(String key, SchemaVersion version);

    // How schemas are deleted
    CompletableFuture<SchemaVersion> delete(String key);

    // Utility method for converting a schema version byte array to a SchemaVersion object
    SchemaVersion versionFromBytes(byte[] version);

    // Startup behavior for the schema storage client
    void start() throws Exception;

    // Shutdown behavior for the schema storage client
    void close() throws Exception;
}

```

> For a full-fledged example schema storage implementation, see the [`BookKeeperSchemaStorage`](https://github.com/apache/pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/service/schema/BookkeeperSchemaStorage.java) class.

## SchemaStorageFactory interface 

```java

public interface SchemaStorageFactory {
    @NotNull
    SchemaStorage create(PulsarService pulsar) throws Exception;
}

```

> For a full-fledged example schema storage factory implementation, see the [`BookKeeperSchemaStorageFactory`](https://github.com/apache/pulsar/blob/master/pulsar-broker/src/main/java/org/apache/pulsar/broker/service/schema/BookkeeperSchemaStorageFactory.java) class.

## Deployment

In order to use your custom schema storage implementation, you'll need to:

1. Package the implementation in a [JAR](https://docs.oracle.com/javase/tutorial/deployment/jar/basicsindex.html) file.
1. Add that jar to the `lib` folder in your Pulsar [binary or source distribution](getting-started-standalone.md#installing-pulsar).
1. Change the `schemaRegistryStorageClassName` configuration in [`broker.conf`](reference-configuration.md#broker) to your custom factory class (i.e. the `SchemaStorageFactory` implementation, not the `SchemaStorage` implementation).
1. Start up Pulsar.
