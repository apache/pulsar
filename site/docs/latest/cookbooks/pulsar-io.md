---
title: The Pulsar IO cookbook
---

## Managing connectors

Pulsar connectors can be managed using the [`source`](../../reference/CliTools#pulsar-admin-source) and [`sink`](../../reference/CliTools#pulsar-admin-sink) commands of the [`pulsar-admin`](../../reference/CliTools#pulsar-admin).

This command, for example, would create a new connector in a Pulsar cluster:

```bash
$ bin/pulsar-admin source create \
  --name 
```

## Available connectors

At the moment, the following connectors are available for Pulsar:

{% include connectors.html %}

If you'd like to create a connector 

## Custom sources and sinks

```xml
<!-- in your <properties> block -->
<pulsar.version>{{ site.current_version }}</pulsar.version>

<!-- in your <dependencies> block -->
<dependency>
  <groupId>org.apache.pulsar</groupId>
  <artifactId>pulsar-io</artifactId>
  <version>${pulsar.version}</version>
</dependency>
```

### Custom sources

[`PushSource`](https://github.com/apache/incubator-pulsar/blob/master/pulsar-io/core/src/main/java/org/apache/pulsar/io/core/PushSource.java) interface

```java
public abstract class PushSource<T> extends AutoCloseable {
    abstract public void open(Map<String, Object> config) throws Exception;

    abstract public void setConsumer(Consumer<Record<T>> consumer);
}
```

### Custom sinks

```java
public interface Sink<T> extends AutoCloseable {
    void open(final Map<String, Object> config) throws Exception;

    CompletableFuture<Void> write(T value);
}
```