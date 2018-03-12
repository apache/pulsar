---
title: The Pulsar Functions API
new: true
---

Pulsar Functions provides an easy-to-use API that developers can use to create and manage processing logic for the Apache Pulsar messaging system. With Pulsar Functions, you can write functions of any level of complexity in [Java](#java) or [Python](#python) and run them in conjunction with a Pulsar cluster without needing to run a separate stream processing engine.

{% include admonition.html type="info" content="For a more in-depth overview of the Pulsar Functions feature, see the [Pulsar Functions overview](../overview)." %}

## Core programming model

Pulsar Functions provide a wide range of functionality but are based on a very simple programming model. You can think of Pulsar Functions as lightweight processes that

* {% popover consume %} messages from one or more Pulsar {% popover topics %} and then
* apply some user-defined processing logic (just about anything you want). This could involve
  * {% popover producing %} the resulting, processed message on another Pulsar topic or
  * doing something else with the message, like [storing state](#state-storage), incrementing a [counter](#counter), writing results to an external database, etc.

You could use Pulsar Functions, for example, to set up the following processing chain:

* A [Python](#python) function listens on the `raw-sentences` topic and "[sanitizes](#example-function)" incoming strings (removing extraneous whitespace and converting all characters to lower case) and then publishes the results to a `sanitized-sentences` topic
* A [Java](#java) function listens on the `sanitized-sentences` topic, counts the number of times each word appears within a specified time window, and publishes the results to a `results` topic
* Finally, a Python function listens on the `results` topic and writes the results to a MySQL table

### Example function

Here's an example of the "input sanitizer" Python function method above:

```python
def clean_string(input_string):
    return input_string.strip().lower()

def process(input):
    return clean_string(input)
```

Some things to note about this Pulsar Function:

* There is no client, producer, or consumer object involved. All message "plumbing" is already taken care of for you.
* No topics, subscription types, {% popover tenants %}, or {% popover namespaces %} are specified in the function logic itself. Instead, topics are specified upon [deployment](#example-deployment).

### Example deployment

Deploying Pulsar Functions is handled by the [`pulsar-admin`](../../reference/CliTools#pulsar-admin) CLI tool, in particular the [`functions`](../../reference/CliTools#pulsar-admin-functions) command.

### Serialization and deserialization (SerDe) {#serde}

SerDe stands for **Ser**ialization and **De**serialization. Whenever you use Pulsar Functions.

By default, Pulsar Functions supports several basic types:

* Strings
* Integers

If there's a custom type outside of this list that you'd like to use, you'll need to create your own SerDe interface for that type. See the docs for [Java](#java-serde) and [Python](#python-serde) for language-specific instructions.

## Context

Both the [Java](#java-functions-with-context) and [Python](#python-functions-with-context) APIs provide optional access to a **context object** that can be used by the function. This context object provides a wide variety of information and functionality to the function:

* The name and ID of the Pulsar Function
* The message ID of each message. Each Pulsar {% popover message %} is automatically assigned an ID.
* The name of the topic on which the message was sent
* The names of all [source topics](#source-topics) and the [sink topics](#sink-topic) associated with the function
* The name of the class used for [SerDe](#serde)
* The {% popover tenant %} and {% popover namespace %} associated with the function
* The ID of the Pulsar Functions instance running the function
* The version of the function
* The [logger object](#logging) used by the function, which can be used to create function log messages
* A built-in distributed [counter](#counters) that can be incremented on a per-key basis
* Access to arbitrary [user config](#user-config) values supplied via the CLI
* An interface for recording [metrics](../metrics-and-stats)

## Logging

TODO.

## Counters

For example, a function might have 

## User config

```bash
$ bin/pulsar-admin functions create \
  --userConfig key=value
```

## Java

Writing Pulsar Functions in Java involves implementing one of two interfaces:

* The [`java.util.Function`](https://docs.oracle.com/javase/8/docs/api/java/util/function/Function.html) interface
* The {% javadoc PulsarFunction client org.apache.pulsar.functions.api.PulsarFunction %} interface. This interface works much like the `java.util.Function` ihterface, but with the important difference

### Getting started

To get started developing Pulsar Functions in Java, you'll need to add a dependency on the `pulsar-functions-api` artifact to your project.

{% include admonition.html type='success' content='An easy way to get up and running with Pulsar Functions in Java is to clone the [`pulsar-functions-java-starter`](https://github.com/streamlio/pulsar-functions-java-starter) repo and follow the instructions there.' %}

#### Maven

Add the following to your `pom.xml` configuration file:

```xml
<properties>
    <pulsar.version>2.0.0-incubating-SNAPSHOT</pulsar.version>
</properties>

<dependencies>
    <dependency>
        <groupId>org.apache.pulsar</groupId>
        <artifactId>pulsar-functions-api</artifactId>
        <version>${pulsar.version}</version>
    </dependency>
</dependencies>
```

#### Gradle

Add the following to your `build.gradle` configuration file:

```groovy
dependencies {
  compile group: 'org.apache.pulsar', name: 'pulsar-functions-api', version: '2.0.0-incubating-SNAPSHOT'
}

// Alternatively:
dependencies {
  compile 'org.apache.pulsar:pulsar-functions-api:2.0.0-incubating-SNAPSHOT'
}
```

### Java functions without context

If your function doesn't require access to its [context](#context), you can implement the [`java.util.Function`](https://docs.oracle.com/javase/8/docs/api/java/util/function/Function.html) interface, which has this very simple, single-method signature:

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

### Void functions

Pulsar Functions can publish results to an output {% popover topic %}, but this isn't required. You can also have functions that simply produce a log, increment a [counter](#counters), write results to a database, etc.



```java
public class IncrementFunction implements PulsarFunction<String, Void> {
    @Override
    public String apply(String input, Context context) {
        String counterKey = input;
        context.incrCounter(counterKey, 1);
        return null;
    }
}
```

{% include admonition.html type="warning" content="When using Java functions that return `Void`, the function must *always* return `null`." %}

### Java SerDe

> Serde stands for **Ser**ialization and **De**serialization.

The following Java types are supported by default:

* `String`
* `Double`
* `Integer`
* `Float`
* `Long`
* `Short`
* `Byte`

Built-in vs. custom. For custom, you need to implement this interface:

```java
public interface SerDe<T> {
    T deserialize(byte[] input);
    byte[] serialize(T input);
}
```

```bash
$ bin/pulsar-admin functions create \
  --ser
```

### Java logging



## Python

```python
def process(input):
```

### With context

```python
def 
```