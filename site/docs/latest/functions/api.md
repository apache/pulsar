---
title: The Pulsar Functions API
preview: true
---

[Pulsar Functions](../overview) provides an easy-to-use API that developers can use to create and manage processing logic for the Apache Pulsar messaging system. With Pulsar Functions, you can write functions of any level of complexity in [Java](#java) or [Python](#python) and run them in conjunction with a Pulsar cluster without needing to run a separate stream processing engine.

{% include admonition.html type="info" content="For a more in-depth overview of the Pulsar Functions feature, see the [Pulsar Functions overview](../overview)." %}

## Core programming model

Pulsar Functions provide a wide range of functionality but are based on a very simple programming model. You can think of Pulsar Functions as lightweight processes that

* {% popover consume %} messages from one or more Pulsar {% popover topics %} and then
* apply some user-defined processing logic to each incoming message. That processing logic could be just about anything you want, including
  * {% popover producing %} the resulting, processed message on another Pulsar topic, or
  * doing something else with the message, such as writing results to an external database.

You could use Pulsar Functions, for example, to set up the following processing chain:

* A [Python](#python) function listens on the `raw-sentences` topic and "[sanitizes](#example-function)" incoming strings (removing extraneous whitespace and converting all characters to lower case) and then publishes the results to a `sanitized-sentences` topic
* A [Java](#java) function listens on the `sanitized-sentences` topic, counts the number of times each word appears within a specified time window, and publishes the results to a `results` topic
* Finally, a Python function listens on the `results` topic and writes the results to a MySQL table

### Example function

Here's an example "input sanitizer" function written in Python and stored in a `sanitizer.py` file:

```python
def clean_string(s):
    return s.strip().lower()

def process(input):
    return clean_string(input)
```

Some things to note about this Pulsar Function:

* There is no client, producer, or consumer object involved. All message "plumbing" is already taken care of for you, enabling you to worry only about processing logic.
* No topics, subscription types, {% popover tenants %}, or {% popover namespaces %} are specified in the function logic itself. Instead, topics are specified upon [deployment](#example-deployment). This means that you can use and re-use Pulsar Functions across topics, tenants, and namespaces without needing to hard-code those attributes.

### Example deployment

Deploying Pulsar Functions is handled by the [`pulsar-admin`](../../reference/CliTools#pulsar-admin) CLI tool, in particular the [`functions`](../../reference/CliTools#pulsar-admin-functions) command. Here's an example command that would run our [sanitizer](#example-function) function from above in [local run](../deployment#local-run) mode:

```bash
$ bin/pulsar-admin functions localrun \
  --py sanitizer.py \
  --className sanitizer \
  --tenant sample \
  --namespace ns1
```

For instructions on running functions in your Pulsar cluster, see the [Deploying Pulsar Functions](../deployment) guide.

### Available APIs {#apis}

In both Java and Python, you have two options for writing Pulsar Functions:

Interface | Description | Use cases
:---------|:------------|:---------
Language-native interface | No Pulsar-specific libraries or special dependencies required (only core libraries from Java/Python) | Functions that don't require access to the function's [context](#context)
Pulsar Function SDK for Java/Python | Pulsar-specific libraries that provide a range of functionality not provided by "native" interfaces | Functions that require access to the function's [context](#context)

In Python, for example, this language-native function, which adds an exclamation point to all incoming strings and publishes the resulting string to a topic, would have no external dependencies:

```python
def process(input):
    return "{}!".format(input)
```

This function, however, would use the Pulsar Functions [SDK for Python](#python-sdk):

```python
from pulsar import Function

class DisplayFunctionName(Function):
    def process(self, input, context):
        function_name = context.function_name()
        return "The function processing this message has the name {0}".format(function_name)
```

### Serialization and deserialization (SerDe) {#serde}

SerDe stands for **Ser**ialization and **De**serialization. All Pulsar Functions use SerDe for message handling. How SerDe works by default depends on the language you're using for a particular function:

* In [Python](#python-serde), the default SerDe is identity, meaning that the type is serialized as whatever type the producer function returns
* In [Java](#java-serde), a number of commonly used types (`String`s, `Integer`s, etc.) are supported by default

In both languages, however, you can write your own custom SerDe logic for more complex, application-specific types. See the docs for [Java](#java-serde) and [Python](#python-serde) for language-specific instructions.

### Context

Both the [Java](#java-sdk) and [Python](#python-sdk) SDKs provide access to a **context object** that can be used by the function. This context object provides a wide variety of information and functionality to the function:

* The name and ID of the Pulsar Function
* The message ID of each message. Each Pulsar {% popover message %} is automatically assigned an ID.
* The name of the topic on which the message was sent
* The names of all input topics as well as the output topic associated with the function
* The name of the class used for [SerDe](#serde)
* The {% popover tenant %} and {% popover namespace %} associated with the function
* The ID of the Pulsar Functions instance running the function
* The version of the function
* The [logger object](#logging) used by the function, which can be used to create function log messages
* Access to arbitrary [user config](#user-config) values supplied via the CLI
* An interface for recording [metrics](../metrics)

### User config

When you run or update Pulsar Functions created using the [SDK](#apis), you can pass arbitrary key/values to them via the command line with the `--userConfig` flag. Key/values must be specified as JSON. Here's an example of a function creation command that passes a user config key/value to a function:

```bash
$ bin/pulsar-admin functions create \
  --name word-filter \
  # Other function configs
  --userConfig '{"forbidden-word":"rosebud"}'
```

If the function were a Python function, that config value could be accessed like this:

```python
from pulsar import Function

class WordFilter(Function):
    def process(self, context, input):
        forbidden_word = context.user_config()["forbidden-word"]

        # Don't publish the message if it contains the user-supplied
        # forbidden word
        if forbidden_word in input:
            pass
        # Otherwise publish the message
        else:
            return input
```

## Pulsar Functions for Java {#java}

Writing Pulsar Functions in Java involves implementing one of two interfaces:

* The [`java.util.Function`](https://docs.oracle.com/javase/8/docs/api/java/util/function/Function.html) interface
* The {% javadoc Function client org.apache.pulsar.functions.api.Function %} interface. This interface works much like the `java.util.Function` ihterface, but with the important difference that it provides a {% javadoc Context client org.apache.pulsar.functions.api.Context %} object that you can use in a [variety of ways](#context)

### Getting started

In order to write Pulsar Functions in Java, you'll need to install the proper [dependencies](#java-dependencies) and package your function [as a JAR](#java-packaging).

#### Dependencies {#java-dependencies}

How you get started writing Pulsar Functions in Java depends on which API you're using:

* If you're writing [Java native function](#java-native), you won't need any external dependencies.
* If you're writing a [Java SDK](#java-sdk) function, you'll need to import the `pulsar-functions-api` library.

  Here's an example for a Maven `pom.xml` configuration file:

  ```xml
  <dependency>
      <groupId>org.apache.pulsar</groupId>
      <artifactId>pulsar-functions-api</artifactId>
      <version>2.0.0-incubating-SNAPSHOT</version>
  </dependency>
  ```

  Here's an example for a Gradle `build.gradle` configuration file:

  ```groovy
  dependencies {
    compile group: 'org.apache.pulsar', name: 'pulsar-functions-api', version: '2.0.0-incubating-SNAPSHOT'
  }
  ```

#### Packaging {#java-packaging}

Whether you're writing Java Pulsar Functions using the [native](#java-native) Java `java.util.Function` interface or using the [Java SDK](#java-sdk), you'll need to package your function(s) as a "fat" JAR.

{% include admonition.html type="success" title="Starter repo" content="If you'd like to get up and running quickly, you can use [this repo](https://github.com/streamlio/pulsar-functions-java-starter), which contains the necessary Maven configuration to build a fat JAR as well as some example functions." %}

### Java native functions {#java-native}

If your function doesn't require access to its [context](#context), you can create a Pulsar Function by implementing the [`java.util.Function`](https://docs.oracle.com/javase/8/docs/api/java/util/function/Function.html) interface, which has this very simple, single-method signature:

```java
public interface Function<I, O> {
    O apply(I input);
}
```

Here's an example function that takes a string as its input, adds an exclamation point to the end of the string, and then publishes the resulting string:

```java
import java.util.Function;

public class ExclamationFunction implements Function<String, String> {
    @Override
    public String process(String input) {
        return String.format("%s!", input);
    }
}
```

In general, you should use native functions when you don't need access to the function's [context](#context). If you *do* need access to the function's context, then we recommend using the [Pulsar Functions Java SDK](#java-sdk).

#### Java native examples

There is one example Java native function in [this folder](https://github.com/apache/incubator-pulsar/tree/master/pulsar-functions/java-examples/src/main/java/org/apache/pulsar/functions/api/examples):

* [`ExclamationFunction`](https://github.com/apache/incubator-pulsar/blob/master/pulsar-functions/java-examples/src/main/java/org/apache/pulsar/functions/api/examples/JavaNativeExclmationFunction.java)

### Java SDK functions {#java-sdk}

To get started developing Pulsar Functions using the Java SDK, you'll need to add a dependency on the `pulsar-functions-api` artifact to your project. Instructions can be found [above](#java-dependencies).

{% include admonition.html type='success' content='An easy way to get up and running with Pulsar Functions in Java is to clone the [`pulsar-functions-java-starter`](https://github.com/streamlio/pulsar-functions-java-starter) repo and follow the instructions there.' %}

#### Java SDK examples

There are several example Java SDK functions in [this folder](https://github.com/apache/incubator-pulsar/tree/master/pulsar-functions/java-examples/src/main/java/org/apache/pulsar/functions/api/examples):

Function name | Description
:-------------|:-----------
[`ContextFunction`](https://github.com/apache/incubator-pulsar/blob/master/pulsar-functions/java-examples/src/main/java/org/apache/pulsar/functions/api/examples/ContextFunction.java) | Illustrate [context](#context)-specific functionality like [logging](#java-logging) and [metrics](#java-metrics)

### Java context object {#java-context}

The {% javadoc Context client org.apache.pulsar.functions.api.Context %} interface provides a number of methods that you can use to access the functions [context](#context). The various method signatures for the `Context` interface are listed below:

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
    String getUserConfigValue(String key);
    void recordMetric(String metricName, double value);
    <O> CompletableFuture<Void> publish(String topicName, O object, String serDeClassName);
    <O> CompletableFuture<Void> publish(String topicName, O object);
    CompletableFuture<Void> ack(byte[] messageId, String topic);
}
```

Here's an example function that uses several methods available via the `Context` object:

```java
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.slf4j.Logger;

import java.util.stream.Collectors;

public class ContextFunction implements Function<String, Void> {
    public Void process(String input, Context context) {
        Logger LOG = context.getLogger();
        String inputTopics = context.getInputTopics().stream().collect(Collectors.joining(", "));
        String functionName = context.getFunctionName();

        String logMessage = String.format("A message with a value of \"%s\" has arrived on one of the following topics: %s\n",
                input,
                inputTopics);

        LOG.info(logMessage);

        String metricName = String.format("function-%s-messages-received", functionName);
        context.recordMetric(metricName, 1);

        return null;
    }
}
```

### Void functions

Pulsar Functions can publish results to an output {% popover topic %}, but this isn't required. You can also have functions that simply produce a log, write results to a database, etc. Here's a function that writes a simple log every time a message is received:

```java
import org.slf4j.Logger;

public class LogFunction implements PulsarFunction<String, Void> {
    public String apply(String input, Context context) {
        Logger LOG = context.getLogger();
        LOG.info("The following message was received: {}", input);
        return null;
    }
}
```

{% include admonition.html type="warning" content="When using Java functions in which the output type is `Void`, the function must *always* return `null`." %}

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

#### Java SerDe example

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
Pulsar does not store your custom SerDe classes separately from your Pulsar Functions. That means that you'll need to always include your SerDe classes in your function JARs. If not, Pulsar will return an error." %}

### Java logging

Pulsar Functions that use the [Java SDK](#java-sdk) have access to an [SLF4j](https://www.slf4j.org/) [`Logger`](https://www.slf4j.org/api/org/apache/log4j/Logger.html) object that can be used to create logs. Here's a simple example function that logs either a `WARNING`- or `INFO`-level log based on whether the incoming string contains the word `danger`:

```java
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.slf4j.Logger;

public class LoggingFunction implements Function<String, Void> {
    @Override
    public void apply(String input, Context context) {
        Logger LOG = context.getLogger();
        String messageId = new String(context.getMessageId());

        if (input.contains("danger")) {
            LOG.warn("A warning was received in message {}", messageId);
        } else {
            LOG.info("Message {} received\nContent: {}", messageId, input);
        }

        return null;
    }
}
```

If you want your function to produce logs, you need to specify a log topic when creating or running the function. Here's an example:

```bash
$ bin/pulsar-admin functions create \
  --jar my-functions.jar \
  --className my.package.LoggingFunction \
  --logTopic persistent://sample/standalone/ns1/logging-function-logs \
  # Other function configs
```

Now, all logs produced by the `LoggingFunction` above can be accessed via the `persistent://sample/standalone/ns1/logging-function-logs` topic.

### Java user config

The Java SDK's [`Context`](#java-context) object enables you to access key/value pairs provided to the Pulsar Function via the command line (as JSON). Here's an example function creation command that passes a key/value pair:

```bash
$ bin/pulsar-admin functions create \
  # Other function configs
  --userConfig '{"word-of-the-day":"verdure"}'
```

To access that value in a Java function:

```java
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.slf4j.Logger;

import java.util.Optional;

public class UserConfigFunction implements Function<String, Void> {
    @Override
    public void apply(String input, Context context) {
        Logger LOG = context.getLogger();
        Optional<String> wotd = context.getUserConfigValue("word-of-the-day");
        if (wotd.isPresent()) {
            LOG.info("The word of the day is {}", wotd);
        } else {
            LOG.warn("No word of the day provided");
        }
        return null;
    }
}
```

The `UserConfigFunction` function will log the string `"The word of the day is verdure"` every time the function is invoked (i.e. every time a message arrives). The `word-of-the-day` user config will be changed only when the function is updated with a new config value via the command line.

{% include admonition.html type="info" content="For all key/value pairs passed to Java Pulsar Functions, both the key *and* the value are `String`s. If you'd like the value to be of a different type, you will need to deserialize from the `String` type." %}

## Java metrics

You can record metrics using the [`Context`](#java-context) object on a per-key basis. You can, for example, set a metric for the key `process-count` and a different metric for the key `elevens-count` every time the function processes a message. Here's an example:

```java
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

public class MetricRecorderFunction implements Function<Integer, Void> {
    @Override
    public void apply(Integer input, Context context) {
        // Records the metric 1 every time a message arrives
        context.recordMetric("hit-count", 1);
        
        // Records the metric only if the arriving number equals 11
        if (input == 11) {
            context.recordMetric("elevens-count", 1);
        }
    }
}
```

{% include admonition.html type="info" content="For instructions on reading and using metrics, see the [Monitoring](../../deployment/Monitoring) guide." %}

## Pulsar Functions for Python {#python}

Documentation for the Python SDK for Pulsar Functions is coming soon.