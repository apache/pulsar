---
title: The Pulsar Functions API
---

Pulsar Functions provides an easy-to-use API that develoeprs can use to 

## Core programming model

Pulsar Functions

### Source and sink topics

All Pulsar Functions have one or more **source topics** that supply messages to the function.

### Sink topic

At the moment, Pulsar Functions can have at most one **sink topic** to which processing results are published.

### SerDe

SerDe stands for **Ser**ialization and **De**serialization

## Context

Both the [Java](#java-functions-with-context) and [Python](#python-functions-with-context) APIs provide optional access to a **context object** that can be used by the function. This context object provides a wide variety of information to the function:

* The name and ID of the Pulsar Function
* The message ID of each message
* The name of the topic on which the message was sent
* The names of all [source topics](#source-topics) and the [sink topics](#sink-topic) associated with the function
* The name of the class used for [SerDe](#serde)
* The {% popover tenant %} and {% popover namespace %} associated with the function
* The ID of the Pulsar Functions instance running the function
* The version of the function
* The logger object used by the function

## Java

Writing Pulsar Functions in Java involves implementing one of two interfaces:

* The [`java.util.Function`](https://docs.oracle.com/javase/8/docs/api/java/util/function/Function.html) interface
* The {% javadoc PulsarFunction client org.apache.pulsar.functions.api.Function %} interface. This interface works much like the `java.util.Function` ihterface, but with the important difference

### Java functions without context

If your function doesn't require access to its [context](#context), you can implement the [`java.util.Function`](https://docs.oracle.com/javase/8/docs/api/java/util/function/Function.html) interface, which has this very simply, one-method signature:

```java
public interface Function<I, O> {
    O apply(I input);
}
```

Here's a simple example that takes a string as its input, adds an exclamation point to the end of the string, and then publishes the resulting string:

```java
import java.util.Function;

public class ExclamationFunction implements Function<String, String> {

}
```

### Void functions

Pulsar Functions can publish results to an output {% popover topic %}, but this isn't required. Functions can also 



```java
import java.util.Function;
public class ExclamationFunction implements Function<String, String> {
    @Override
    public String apply(String input) { return String.format("%s!", input); }
}
```

### Java functions with context

```java
public interface PulsarFunction<I, O> {
    O process(I input, Context context) throws Exception;
}
```

Context interface:

```java
public interface Context {
    byte[] getMessageId();
    String getTopicName();
    Collection<String> getSourceTopics();
    String getSinkTopic();
    String getOutputSerdeClassName();
    String getTenant();
    String getNamespace();
    String getFunctionName();
    String getFunctionId();
    String getInstanceId();
    String getFunctionVersion();
    Logger getLogger();
    void incrCounter(String key, long amount);
    String getUserConfigValue(String key);
    void recordMetric(String metricName, double value);
    <O> CompletableFuture<Void> publish(String topicName, O object, String serDeClassName);
    <O> CompletableFuture<Void> publish(String topicName, O object);
    CompletableFuture<Void> ack(byte[] messageId, String topic);
}
```

### SerDe

> Serde stands for **Ser**ialization and **De**serialization.

Built-in vs. custom. For custom, you need to implement this interface:

```java
public interface SerDe<T> {
    T deserialize(byte[] input);
    byte[] serialize(T input);
}
```

The built-in is the `org.apache.pulsar.functions.api.DefaultSerDe` class:

```java

```

The built-in should work fine for basic Java types. For more advanced types,


## Python

```python
def process(input):
```

### With context

```python
def 
```