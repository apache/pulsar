---
title: Schema storage cookbook
tags: [admin, schemas, schema registry]
---

By default, Pulsar's [schema registry](../../getting-started/ConceptsAndArchitecture#schema-registry) feature stores schemas in [Apache BookKeeper](https://bookkeeper.apache.org), which is used by Pulsar for message storage. You can, however, use other storage systems to store schemas. This cookbook tells you how to create your own

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

```java
public class FileSystemStorage implements SchemaStorage {

}
```

Your custom implementation must then be

* compiled into a [JAR file](https://docs.oracle.com/javase/tutorial/deployment/jar/basicsindex.html) that is
* stored on the classpath when starting Pulsar