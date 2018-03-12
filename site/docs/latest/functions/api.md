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
def clean_string(s):
    return s.strip().lower()

def process(input):
    return clean_string(input)
```

Some things to note about this Pulsar Function:

* There is no client, producer, or consumer object involved. All message "plumbing" is already taken care of for you.
* No topics, subscription types, {% popover tenants %}, or {% popover namespaces %} are specified in the function logic itself. Instead, topics are specified upon [deployment](#example-deployment). This means that you can use Pulsar Functions across topics, tenants, and namespaces without needing to hard-code those attributes.

### Example deployment

Deploying Pulsar Functions is handled by the [`pulsar-admin`](../../reference/CliTools#pulsar-admin) CLI tool, in particular the [`functions`](../../reference/CliTools#pulsar-admin-functions) command. Here's an example command that would run our [sanitizer](#example-function) function above in [local run](../deployment#local-run-mode) mode:

```bash
$ bin/pulsar-admin functions localrun \
  --py sanitizer.py \
  --className sanitizer \
  --tenant sample \
  --namespace ns1
```

For instructions on running functions in your Pulsar cluster, see the [Deploying Pulsar Functions](../deployment) guide.

### The Pulsar Functions SDK {#sdk}

In both Java and Python, you have two options for writing Pulsar Functions:

Interface | Description | Use cases
:---------|:------------|:---------
"Native" interface | No Pulsar-specific libraries or special dependencies required (only core libraries from Java/Python) | Functions that don't require access to the function's [context](#context)
Pulsar Function SDK for Java/Python | Pulsar-specific libraries that provide a range of functionality not provided by "native" interfaces | Functions that require access to the function's [context](#context)

In Python, for example, this "native" function, which adds an exclamation point to all incoming strings and publishes the resulting string to a topic, would have no external dependencies:

```python
def process(input):
    return "{}!".format(input)
```

This function, however, would use the Pulsar Functions [SDK for Python](#python-sdk):

```python
from pulsarfunction import pulsar_function

class DisplayFunctionName(pulsar_function.Function):
    def process(self, input, context):
        function_name = context.function_name()
        return "The function processing this message is called {0}".format(function_name)
```

### Serialization and deserialization (SerDe) {#serde}

SerDe stands for **Ser**ialization and **De**serialization. All Pulsar Functions use SerDe for message handling. How SerDe works by default depends on the language you're using for a particular function:

* In [Python](#python-serde), the default SerDe is identity, meaning that the type is serialized as whatever the producer
* In [Java](#java-serde), a number of commonly used types (`String`s, `Integer`s, etc.) are supported by default

In both languages, however, you can write your own custom SerDe logic for more complex, application-specific types. See the docs for [Java](#java-serde) and [Python](#python-serde) for language-specific instructions.

## Context

Both the [Java](#java-functions-with-context) and [Python](#python-functions-with-context) APIs provide optional access to a **context object** that can be used by the function. This context object provides a wide variety of information and functionality to the function:

* The name and ID of the Pulsar Function
* The message ID of each message. Each Pulsar {% popover message %} is automatically assigned an ID.
* The name of the topic on which the message was sent
* The names of all input topics as well as the output topic associated with the function
* The name of the class used for [SerDe](#serde)
* The {% popover tenant %} and {% popover namespace %} associated with the function
* The ID of the Pulsar Functions instance running the function
* The version of the function
* The [logger object](#logging) used by the function, which can be used to create function log messages
* A built-in distributed [counter](#counters) that can be incremented on a per-key basis
* Access to arbitrary [user config](#user-config) values supplied via the CLI
* An interface for recording [metrics](../metrics-and-stats)

## Counters

All Pulsar Functions that use the Pulsar Functions SDK have access to a distributed counter

## User config

When you run or update Pulsar Functions created using the [SDK](#sdk), you can pass arbitrary key/values to them via the command line with the `--userConfig` flag. Key/values must be specified as JSON. Here's an example:

```bash
$ bin/pulsar-admin functions create \
  --name \
  # Other function configs
  --userConfig '{"forbidden-subject":"fight club"}'
```

```python
from pulsarfunction import pulsar_function

class SubjectFilter(pulsar_function.PulsarFunction):
    def process(self, context, input):
        forbidden_subject = context.user_config()["forbidden-subject"]

        # Don't publish the message if it pertains to the user-specified
        # forbidden subject
        if input.subject == forbidden_subject:
            pass
        # Otherwise publish the message
        else:
            return input
```

## Java

Writing Pulsar Functions in Java involves implementing one of two interfaces:

* The [`java.util.Function`](https://docs.oracle.com/javase/8/docs/api/java/util/function/Function.html) interface
* The {% javadoc PulsarFunction client org.apache.pulsar.functions.api.PulsarFunction %} interface. This interface works much like the `java.util.Function` ihterface, but with the important difference

### Getting started


### Java native functions

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
    @Override
    public String process(String input) {
        return String.format("%s!", input);
    }
}
```

In general, you should use native functions when you don't need access to the function's [context](#context). If you do need access to the function's context, then we recommend using the [Pulsar Functions Java SDK](#java-sdk).

the [Java SDK](#java-sdk)

### Java SDK functions {#java-sdk}

To get started developing Pulsar Functions using the Java SDK, you'll need to add a dependency on the `pulsar-functions-api` artifact to your project.

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

#### 

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

Pulsar Functions use [SerDe](#serde) when publishing data to and consuming data from Pulsar topics. When you're writing Pulsar Functions in Java, the following basic Java types are built in and supported by default:

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

#### SerDe example

Imagine that you're writing Pulsar Functions in Java that are processing tweet objects. Here's a simple example `Tweet` class:

```java
public class Tweet {
    private String username;
    private String tweetContent;

    public Tweet(String username, String tweetContent) {
        this.username = username;
        this.tweetContent = tweetContent;
    }

    // Standard setters and getters
}
```

In order to be able to pass `Tweet` objects directly between Pulsar Functions, you'll need to provide a custom SerDe class. In the example below, `Tweet` objects are basically strings in which the username and tweet content are separated by a `|`.

```java
package com.example.serde;

import org.apache.pulsar.functions.api.SerDe;

import java.util.regex.Pattern;

public class TweetSerde implements SerDe<Tweet> {
    public Tweet deserialize(byte[] input) {
        String s = new String(input);
        String[] fields = s.split(Pattern.quote("|"));
        return new Tweet(fields[0], fields[1]);
    }

    public byte[] serialize(Tweet input) {
        return "%s|%s".format(input.getUsername(), input.getTweetContent()).getBytes();
    }
}
```

To apply this custom SerDe to a particular Pulsar Function, you would need to:

* Package the `Tweet` and `TweetSerde` classes into a JAR
* Specify a path to the JAR and SerDe class name when deploying the function

Here's an example [`create`](../../reference/CliTools#pulsar-admin-functions-create) operation:

```bash
$ bin/pulsar-admin functions create \
  --jar /path/to/your.jar \
  --outputSerdeClassName com.example.serde.TweetSerde \
  # Other function attributes
```

{% include admonition.html type="warning" title="Custom SerDe classes must be packaged with your function JARs" content="
Pulsar does not store your custom SerDe classes separately from your Pulsar Functions. That means that you'll need to always include your SerDe classes in your function JARs. If not, Pulsar will return an error.
" %}

### Java logging



## Python

### Getting started

```bash
$ pip install pulsarfunctions
```

### Basic example

```python
def process(input):
```

### Python SDK example

### With context

```python
from pulsarfunction import pulsar_function

class CounterIncrementingFunction(pulsar_function.Function):
    def process(self, context, input):
        log = context.
```