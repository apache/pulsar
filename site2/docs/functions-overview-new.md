---
id: functions-overview-new
title: Pulsar Functions overview
sidebar_label: Overview
---
> This new documentation of Pulsar Functions is under construction. For complete Pulsar Functions documentation, refer to the [earlier](functions-overview.md) version.

**Pulsar Functions** are lightweight compute processes that

* consume messages from one or more Pulsar topics,
* apply a user-supplied processing logic to each message,
* publish the results of the computation to another topic.

## Goals
With Pulsar Functions, you can create complex processing logic without deploying a separate neighboring system (such as [Apache Storm](http://storm.apache.org/), [Apache Heron](https://apache.github.io/incubator-heron), [Apache Flink](https://flink.apache.org/)). Pulsar Functions are computing infrastructure of Pulsar messaging system. The core goal is tied to a series of other goals:

* Developer productivity ([language-native](#language-native-functions) vs [Pulsar Functions SDK](#the-pulsar-functions-sdk) functions)
* Easy troubleshooting
* Operational simplicity (no need for an external processing system)

## Inspirations
Pulsar Functions are inspired by (and take cues from) several systems and paradigms:

* Stream processing engines such as [Apache Storm](http://storm.apache.org/), [Apache Heron](https://apache.github.io/incubator-heron), and [Apache Flink](https://flink.apache.org)
* "Serverless" and "Function as a Service" (FaaS) cloud platforms like [Amazon Web Services Lambda](https://aws.amazon.com/lambda/), [Google Cloud Functions](https://cloud.google.com/functions/), and [Azure Cloud Functions](https://azure.microsoft.com/en-us/services/functions/)

Pulsar Functions can be described as

* [Lambda](https://aws.amazon.com/lambda/)-style functions that are
* specifically designed to use Pulsar as a message bus.

## Programming model
Pulsar Functions provide a wide range of functionality, and the core programming model is simple. Functions receive messages from one or more **input [topics](reference-terminology.md#topic)**. Each time a message is received, the function will complete the following tasks.   

  * Apply some processing logic to the input and write output to:
    * An **output topic** in Pulsar
    * [Apache BookKeeper](#state-storage)
  * Write logs to a **log topic** (potentially for debugging purposes)
  * Increment a [counter](#word-count-example)

![Pulsar Functions core programming model](assets/pulsar-functions-overview.png)

You can use Pulsar Functions to set up the following processing chain:

* A [Python](#functions-for-python) function listens for the `raw-sentences` topic and "[sanitizes](#example-function)" incoming strings (removing extraneous whitespace and converting all characters to lowercase) and then publishes the results to a `sanitized-sentences` topic.
* A [Java](#functions-for-java) function listens for the `sanitized-sentences` topic, counts the number of times each word appears within a specified time window, and publishes the results to a `results` topic
* Finally, a Python function listens for the `results` topic and writes the results to a MySQL table.

### Function example

The following is an "input sanitizer" function example written in Python and stored in a `sanitizer.py` file.

```python
def clean_string(s):
    return s.strip().lower()

def process(input):
    return clean_string(input)
```

In the above example, no client, producer, or consumer object is involved. You only need to care the processing topic. 
In addition, no topics, subscription types, tenants, or namespaces are specified in the function logic. Instead, topics are specified in [deployment](#example-deployment). This means that you can use and re-use Pulsar Functions across topics, tenants, and namespaces without needing to hard-code those attributes.

### Deployment example

Deploying Pulsar Functions is handled by the [`pulsar-admin`](reference-pulsar-admin.md) CLI tool, in particular the [`functions`](reference-pulsar-admin.md#functions) command. The following command example runs the [sanitizer](#example-function) function in [local run](functions-deploying.md#local-run-mode) mode.

```bash
$ bin/pulsar-admin functions localrun \
  --py sanitizer.py \          # The Python file with the function's code
  --classname sanitizer \      # The class or function holding the processing logic
  --tenant public \            # The function's tenant (derived from the topic name by default)
  --namespace default \        # The function's namespace (derived from the topic name by default)
  --name sanitizer-function \  # The name of the function (the class name by default)
  --inputs dirty-strings-in \  # The input topic(s) for the function
  --output clean-strings-out \ # The output topic for the function
  --log-topic sanitizer-logs   # The topic to which all functions logs are published
```

For instructions on running functions in your Pulsar cluster, see the [Deploying Pulsar Functions](functions-deploying.md) guide.
 
### Word count example

If you implement the classic word count example using Pulsar Functions, it looks something like this:

![Pulsar Functions word count example](assets/pulsar-functions-word-count.png)

To write the function in [Java](functions-api.md#functions-for-java) with [Pulsar Functions SDK for Java](functions-api.md#java-sdk-functions), you can write the function as follows.

```java
package org.example.functions;

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

import java.util.Arrays;

public class WordCountFunction implements Function<String, Void> {
    // This function is invoked every time a message is published to the input topic
    @Override
    public Void process(String input, Context context) throws Exception {
        Arrays.asList(input.split(" ")).forEach(word -> {
            String counterKey = word.toLowerCase();
            context.incrCounter(counterKey, 1);
        });
        return null;
    }
}
```

Bundle and build the JAR file to be deployed. You can find approaches in [Creating an Uber JAR](#creating-an-uber-jar) and [Creating a NAR package](#creating-a-nar-package).
Then [deploy it](#cluster-run-mode) in your Pulsar cluster using the [command line](#command-line-interface) as follows.

```bash
$ bin/pulsar-admin functions create \
  --jar target/my-jar-with-dependencies.jar \
  --classname org.example.functions.WordCountFunction \
  --tenant public \
  --namespace default \
  --name word-count \
  --inputs persistent://public/default/sentences \
  --output persistent://public/default/count
```

### Content-based routing example
Pulsar Functions are used in many cases. The following is a sophisticated example that involves content-based routing.

For example, a function takes items (strings) as input and publishes them to either a `fruits` or `vegetables` topic, depending on the item. Or, if an item is neither fruit nor vegetable, a warning is logged to a [log topic](#logging). The following is a visual representation.

![Pulsar Functions routing example](assets/pulsar-functions-routing-example.png)

If you implement this routing functionality in Python, it looks something like this:

```python
from pulsar import Function

class RoutingFunction(Function):
    def __init__(self):
        self.fruits_topic = "persistent://public/default/fruits"
        self.vegetables_topic = "persistent://public/default/vegetables"

    def is_fruit(item):
        return item in ["apple", "orange", "pear", "other fruits..."]

    def is_vegetable(item):
        return item in ["carrot", "lettuce", "radish", "other vegetables..."]

    def process(self, item, context):
        if self.is_fruit(item):
            context.publish(self.fruits_topic, item)
        elif self.is_vegetable(item):
            context.publish(self.vegetables_topic, item)
        else:
            warning = "The item {0} is neither a fruit nor a vegetable".format(item)
            context.get_logger().warn(warning)
```

### Functions, messages and message types
Pulsar Functions take byte arrays as inputs and spit out byte arrays as output. However in languages that support typed interfaces(Java), you can write typed Functions, and bind messages to types in the following ways. 
* [Schema Registry](#Schema-Registry)
* [SerDe](#SerDe)

### Schema registry
Pulsar has a built in [Schema Registry](concepts-schema-registry) and comes bundled with a variety of popular schema types(avro, json and protobuf). Pulsar Functions can leverage existing schema information from input topics and derive the input type. The schema registry applies for output topic as well.

### SerDe
SerDe stands for **Ser**ialization and **De**serialization. All Pulsar Functions use SerDe for message handling. How SerDe works by default depends on the language you use for a particular function.

* In [Python](#python-serde), the default SerDe is identity, meaning that the type is serialized as whatever type the producer function returns.
* In [Java](#java-serde), a number of commonly used types (`String`s, `Integer`s, and so on) are supported by default.

In both languages, however, you can write custom SerDe logic for more complex, application-specific types. For language-specific instructions, see [Java](#java-serde) and [Python](#python-serde) documents.

### Context
Both the [Java](#java-sdk-functions) and [Python](#python-sdk-functions) SDKs provide access to a **context object** that can be used by a function. This context object provides a wide variety of information and functionality to the function.

* The name and ID of a Pulsar Function.
* The message ID of each message. Each Pulsar message is automatically assigned with an ID.
* The name of the topic to which the message is sent.
* The names of all input topics as well as the output topic associated with the function.
* The name of the class used for [SerDe](#serialization-and-deserialization-serde).
* The [tenant](reference-terminology.md#tenant) and namespace associated with the function.
* The ID of the Pulsar Functions instance running the function.
* The version of the function.
* The [logger object](functions-overview.md#logging) used by the function, which can be used to create function log messages.
* Access to arbitrary [user configuration](#user-configuration) values supplied via the CLI.
* An interface for recording [metrics](functions-metrics.md).
* An interface for storing and retrieving state in [state storage](functions-overview.md#state-storage).

### User configuration
When you run or update Pulsar Functions created using SDK, you can pass arbitrary key/values to them with the command line with the `--userConfig` flag. Key/values must be specified as JSON. The following function creation command passes a user configured key/value to a function.

```bash
$ bin/pulsar-admin functions create \
  --name word-filter \
  # Other function configs
  --user-config '{"forbidden-word":"rosebud"}'
```

In Python function, you can access the configuration value like this.

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

## Fully Qualified Function Name (FQFN)
Each Pulsar Function has a **Fully Qualified Function Name** (FQFN) that consists of three elements: the function tenant, namespace, and function name. FQFN looks like this:

```http
tenant/namespace/name
```

FQFNs enable you to create multiple functions with the same name provided that they are in different namespaces.

## Supported languages
Currently, you can write Pulsar Functions in [Java](functions-api.md#functions-for-java), [Python](functions-api.md#functions-for-python), and [Go](functions-api.md#functions-for-go). Support for additional languages is coming soon.

## Processing guarantees
Pulsar Functions provide three different messaging semantics that you can apply to any function.

Delivery semantics | Description
:------------------|:-------
**At-most-once** delivery | Each message sent to the function is likely to be processed, or not to be processed (hence "at most").
**At-least-once** delivery | Each message sent to the function can be processed more than once (hence the "at least").
**Effectively-once** delivery | Each message sent to the function will have one output associated with it.


### Apply processing guarantees to a function
You can set the processing guarantees for a Pulsar Function when you create the Function. The [`pulsar-function create`](reference-pulsar-admin.md#create-1) command applies effectively-once guarantees to the Function.

```bash
$ bin/pulsar-admin functions create \
  --processing-guarantees EFFECTIVELY_ONCE \
  # Other function configs
```

The available options are:

* `ATMOST_ONCE`
* `ATLEAST_ONCE`
* `EFFECTIVELY_ONCE`

The following command runs a function in the cluster mode with effectively-once guarantees applied.

```bash
$ bin/pulsar-admin functions create \
  --name my-effectively-once-function \
  --processing-guarantees EFFECTIVELY_ONCE \
  # Other function configs
```

> By default, Pulsar Functions provide at-least-once delivery guarantees. So if you create a function without supplying a value for the `--processingGuarantees` flag, the function provides at-least-once guarantees.

### Update the processing guarantees of a function
You can change the processing guarantees applied to a function using the [`update`](reference-pulsar-admin.md#update-1) command. The following is an example.

```bash
$ bin/pulsar-admin functions update \
  --processing-guarantees ATMOST_ONCE \
  # Other function configs
```

## Metrics
Pulsar Functions can publish arbitrary metrics to the metrics interface which can be queried. 

> If a Pulsar Function uses the language-native interface for [Java](functions-api.md#java-native-functions) or [Python](#python-native-functions), that function is not able to publish metrics and stats to Pulsar.

### Access metrics
To access metrics created by Pulsar Functions, refer to [Monitoring](deploy-monitoring.md) in Pulsar. 

## State storage
Pulsar Functions use [Apache BookKeeper](https://bookkeeper.apache.org) as a state storage interface. Pulsar installation, including the local standalone installation, includes deployment of BookKeeper bookies.

Since Pulsar 2.1.0 release, Pulsar integrates with Apache BookKeeper [table service](https://docs.google.com/document/d/155xAwWv5IdOitHh1NVMEwCMGgB28M3FyMiQSxEpjE-Y/edit#heading=h.56rbh52koe3f) to store the `State` for functions. For example, A `WordCount` function can store its `counters` state into BookKeeper table service via Pulsar Functions State API.