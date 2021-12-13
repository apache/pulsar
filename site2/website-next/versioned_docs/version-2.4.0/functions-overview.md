---
id: functions-overview
title: Pulsar Functions overview
sidebar_label: "Overview"
original_id: functions-overview
---

**Pulsar Functions** are lightweight compute processes that

* consume messages from one or more Pulsar topics,
* apply a user-supplied processing logic to each message,
* publish the results of the computation to another topic.

The following is an example of a Pulsar Function written in Java (using the [native interface](functions-api.md#java-native-functions)).

```java

import java.util.Function;

public class ExclamationFunction implements Function<String, String> {
    @Override
    public String apply(String input) { return String.format("%s!", input); }
}

```

The following is an example of a Pulsar Function written in Python (using the [native interface](functions-api.md#python-native-functions)).

```python

def process(input):
    return "{0}!".format(input)

```

The following is an example of a Pulsar Function written in Go.

```

import (
	"fmt"
	"context"

	"github.com/apache/pulsar/pulsar-function-go/pf"
)

func HandleRequest(ctx context.Context, in []byte) error {
	fmt.Println(string(in) + "!")
	return nil
}

func main() {
	pf.Start(HandleRequest)
}

```

A Pulsar Function is executed each time a message is published to its input topic. For example, if a function has an input topic called `tweet-stream`, the function runs each time a message is published to `tweet-stream`.

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

The core programming model of Pulsar Functions is simple. Functions receive messages from one or more **input [topics](reference-terminology.md#topic)**. Each time a message is received, the function will complete the following tasks.   

  * Apply some processing logic to the input and write output to:
    * An **output topic** in Pulsar
    * [Apache BookKeeper](#state-storage)
  * Write logs to a **log topic** (potentially for debugging purposes)
  * Increment a [counter](#word-count-example)

![Pulsar Functions core programming model](/assets/pulsar-functions-overview.png)

### Word count example

If you implement the classic word count example using Pulsar Functions, it looks something like this:

![Pulsar Functions word count example](/assets/pulsar-functions-word-count.png)

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

![Pulsar Functions routing example](/assets/pulsar-functions-routing-example.png)

If you implement this routing functionality in Python, it looks something like this:

```python

from pulsar import Function

class RoutingFunction(Function):
    def __init__(self):
        self.fruits_topic = "persistent://public/default/fruits"
        self.vegetables_topic = "persistent://public/default/vegetables"

    @staticmethod
    def is_fruit(item):
        return item in ["apple", "orange", "pear", "other fruits..."]

    @staticmethod
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

## Command-line interface

Pulsar Functions are managed using the [`pulsar-admin`](reference-pulsar-admin) CLI tool (in particular the [`functions`](reference-pulsar-admin.md#functions) command). The following example runs a function in the [local run mode](#local-run-mode).

```bash

$ bin/pulsar-admin functions localrun \
  --inputs persistent://public/default/test_src \
  --output persistent://public/default/test_result \
  --jar examples/api-examples.jar \
  --classname org.apache.pulsar.functions.api.examples.ExclamationFunction

```

## Fully Qualified Function Name (FQFN)
Each Pulsar Function has a **Fully Qualified Function Name** (FQFN) that consists of three elements: the function tenant, namespace, and function name. FQFN looks like this:

```http

tenant/namespace/name

```

FQFNs enable you to create multiple functions with the same name provided that they are in different namespaces.

## Configuration
You can configure a Pulsar Function in the following ways:

* Via [command-line arguments](#command-line-interface) passed to the [`pulsar-admin functions`](reference-pulsar-admin.md#functions) interface
* Via [YAML](http://yaml.org/) configuration files

If you use a YAML configuration file, you must specify a path to the file on the command line. The following is an example.

```bash

$ bin/pulsar-admin functions create \
  --function-config-file ./my-function.yaml

```

The following is an example of the `my-function.yaml` file.

```yaml

name: my-function
tenant: public
namespace: default
jar: ./target/my-functions.jar
className: org.example.pulsar.functions.MyFunction
inputs:
- persistent://public/default/test_src
output: persistent://public/default/test_result

```

You can specify some function attributes via CLI arguments or in a configuration file in YAML format.

## Supported languages
Currently, you can write Pulsar Functions in [Java](functions-api.md#functions-for-java), [Python](functions-api.md#functions-for-python), and [Go](functions-api.md#functions-for-go). Support for additional languages is coming soon.

## Pulsar Functions API

Pulsar Functions API enables you to create processing logic that is:

* Type safe. Pulsar Functions can process raw bytes or more complex, application-specific types.
* Based on SerDe (**Ser**ialization/**De**serialization). A variety of types are supported "out of the box" but you can also create your own custom SerDe logic.

### Function context
Each Pulsar Function created using [Pulsar Functions SDK](#the-pulsar-functions-sdk) has access to a context object that both provides:

1. A wide variety of information about the function, including:
  * The name of the function
  * The tenant and namespace of the function
  * [User-supplied configuration](#user-configuration) values
2. Special functionality, including:
  * The ability to produce [logs](#logging) to a specified logging topic
  * The ability to produce [metrics](#metrics)

### Language-native functions
"Native" functions are supported in Java and Python, which means a Pulsar Function can have no dependencies.

The benefit of native functions is that they do not have any dependencies beyond what's already available in Java/Python "out of the box." The downside is that they do not provide access to the function [context](#function-context), which is necessary for a variety of functionalities, including [logging](#logging), [user configuration](#user-configuration), and more.

## Pulsar Functions SDK   
To enable a Pulsar Function to access to a [context object](#function-context), you can use **Pulsar Functions SDK**, available for [Java](functions-api.md#functions-for-java), [Python](functions-api.md#functions-for-python), and [Go](functions-api.md#functions-for-go).

### Java
The following is a Java function example that uses information about its context.

```java

import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.slf4j.Logger;

public class ContextAwareFunction implements Function<String, Void> {
    @Override
    public Void process(String input, Context, context) {
        Logger LOG = context.getLogger();
        String functionTenant = context.getTenant();
        String functionNamespace = context.getNamespace();
        String functionName = context.getFunctionName();
        LOG.info("Function tenant/namespace/name: {}/{}/{}", functionTenant, functionNamespace, functionName);
        return null;
    }
}

```

### Python
The following is a Python function example that uses information about its context.

```python

from pulsar import Function

class ContextAwareFunction(Function):
    def process(self, input, context):
        log = context.get_logger()
        function_tenant = context.get_function_tenant()
        function_namespace = context.get_function_namespace()
        function_name = context.get_function_name()
        log.info("Function tenant/namespace/name: {0}/{1}/{2}".format(function_tenant, function_namespace, function_name))

```

### Go
The following is a Go function example that uses information about its context.

```

import (
	"context"
	"fmt"

    "github.com/apache/pulsar/pulsar-function-go/log"
	"github.com/apache/pulsar/pulsar-function-go/pf"
)

func contextFunc(ctx context.Context) {
	if fc, ok := pf.FromContext(ctx); ok {
		tenant := fc.GetFuncTenant()
		namespace := fc.GetFuncNamespace()
		name := fc.GetFuncName()
		log.Info("Function tenant/namespace/name: %s/%s/%s\n", tenant, namespace, name)
	}
}

func main() {
	pf.Start(contextFunc)
}

```

## Deployment
Pulsar Functions support a variety of deployment options. You can deploy a Pulsar Function in the following ways.

Deployment mode | Description
:---------------|:-----------
[Local run mode](#local-run-mode) | The function runs in your local environment, for example, on your laptop.
[Cluster mode](#cluster-run-mode) | The function runs *inside of* your Pulsar cluster, on the same machines as your Pulsar [brokers](reference-terminology.md#broker).

### Local run mode

If you run a Pulsar Function in the **local run** mode, you run it on the machine where you run commands(for example, your laptop, an [AWS EC2](https://aws.amazon.com/ec2/) instance). The following example is about the [`localrun`](reference-pulsar-admin.md#localrun) command.

```bash

$ bin/pulsar-admin functions localrun \
  --py myfunc.py \
  --classname myfunc.SomeFunction \
  --inputs persistent://public/default/input-1 \
  --output persistent://public/default/output-1

```

By default, the function connects to a Pulsar cluster running on the same machine, via a local broker service URL of `pulsar://localhost:6650`. If you run a function with the local run mode, and connect it to a non-local Pulsar cluster, specify a different broker URL using the `--brokerServiceUrl` flag. The following is an example.

```bash

$ bin/pulsar-admin functions localrun \
  --broker-service-url pulsar://my-cluster-host:6650 \
  # Other function parameters

```

### Cluster mode
When you run Pulsar Functions in the **cluster mode**, the function code is uploaded to a Pulsar broker and runs *alongside the broker* rather than in your [local environment](#local-run-mode). You can run a function in the cluster mode using the [`create`](reference-pulsar-admin.md#create-1) command.  The following is an example.

```bash

$ bin/pulsar-admin functions create \
  --py myfunc.py \
  --classname myfunc.SomeFunction \
  --inputs persistent://public/default/input-1 \
  --output persistent://public/default/output-1

```

This command uploads `myfunc.py` to Pulsar, which uses the code to start one [or more](#parallelism) instances of the function.

### Run instances in parallel

When you create Pulsar Functions and run in the [cluster mode](#cluster-mode), only one **instance** of Pulsar Functions is running by default. However, you can run multiple instances in parallel. Specify the number of instances when you create Pulsar Functions, or update an existing single-instance function with a new parallel factor.

This command, for example, creates and runs a function with 5 instances in parallel.

```bash

$ bin/pulsar-admin functions create \
  --name parallel-fun \
  --tenant public \
  --namespace default \
  --py func.py \
  --classname func.ParallelFunction \
  --parallelism 5

```

### Function instance resources

When you run Pulsar Functions in the [cluster mode](#cluster-mode), you can specify the resources that are assigned to each function [instance](#run-instances-in-parallel).

Resource | Specified as... | Runtimes
:--------|:----------------|:--------
CPU | The number of cores | Docker (coming soon)
RAM | The number of bytes | Process, Docker
Disk space | The number of bytes | Docker

The following example allocates 8 cores, 8 GB of RAM, and 10 GB of disk space to a function.

```bash

$ bin/pulsar-admin functions create \
  --jar target/my-functions.jar \
  --classname org.example.functions.MyFunction \
  --cpu 8 \
  --ram 8589934592 \
  --disk 10737418240

```

For more information on resources, see the [Deploying and Managing Pulsar Functions](functions-deploying.md#resources) documentation.

### Logging

Pulsar Functions created using [Pulsar Functions SDK](#the-pulsar-functions-sdk) can send logs to a log topic that you specify as part of the function configuration. The function created using the following command produces all logs on the `persistent://public/default/my-func-1-log` topic.

```bash

$ bin/pulsar-admin functions create \
  --name my-func-1 \
  --log-topic persistent://public/default/my-func-1-log \
  # Other configs

```

The following is an example of [Java function](functions-api.md#java-logging) that logs at different log levels based on the function input.

```java

public class LoggerFunction implements Function<String, Void> {
    @Override
    public Void process(String input, Context context) {
        Logger LOG = context.getLogger();
        if (input.length() <= 100) {
            LOG.info("This string has a length of {}", input);
        } else {
            LOG.warn("This string is getting too long! It has {} characters", input);
        }
    }
}

```

The following is an example of [Go function](functions-api.md#go-logging) that logs at different log levels based on the function input.

```

import (
	"context"

	"github.com/apache/pulsar/pulsar-function-go/log"
	"github.com/apache/pulsar/pulsar-function-go/pf"
)

func loggerFunc(ctx context.Context, input []byte) {
	if len(input) <= 100 {
		log.Infof("This input has a length of: %d", len(input))
	} else {
		log.Warnf("This input is getting too long! It has {%d} characters", len(input))
	}
}

func main() {
	pf.Start(loggerFunc)
}

```

When you use `logTopic` related functionalities in Go Function, import `github.com/apache/pulsar/pulsar-function-go/log`, and you do not have to use the `getLogger()` context object. The approach is different from Java Function and Python Function.

### User configuration

You can pass arbitrary key-values to Pulsar Functions via the command line (both keys and values must be string). This set of key-values is called the functions **user configuration**. User configuration must consist of JSON strings.

The following example passes user configuration to a function.

```bash

$ bin/pulsar-admin functions create \
  --user-config '{"key-1":"value-1","key-2","value-2"}' \
  # Other configs

```

The following example accesses that configuration map.

```java

public class ConfigMapFunction implements Function<String, Void> {
    @Override
    public Void process(String input, Context context) {
        String val1 = context.getUserConfigValue("key1").get();
        String val2 = context.getUserConfigValue("key2").get();
        context.getLogger().info("The user-supplied values are {} and {}", val1, val2);
        return null;
    }
}

```

### Trigger Pulsar Functions

You can [trigger](functions-deploying.md#triggering-pulsar-functions) a Pulsar Function running in the [cluster mode](#cluster-mode) with the [command line](#command-line-interface). When triggering a Pulsar Function, you can pass a specific value to the Function and get the return value *without* creating a client. Triggering is useful for, but not limited to, testing and debugging purposes.

:::note

Triggering a function is no different from invoking a function by producing a message on one of the function input topics. The [`pulsar-admin functions trigger`](reference-pulsar-admin.md#trigger) command is a convenient mechanism for sending messages to functions without using the [`pulsar-client`](reference-cli-tools.md#pulsar-client) tool or a language-specific client library.

:::

The following is an example of Pulsar Functions written in Python (using the [native interface](functions-api.md#python-native-functions)) that simply reverses string inputs.

```python

def process(input):
    return input[::-1]

```

If the function is running in a Pulsar cluster, you can trigger it with the following commands.

```bash

$ bin/pulsar-admin functions trigger \
  --tenant public \
  --namespace default \
  --name reverse-func \
  --trigger-value "snoitcnuf raslup ot emoclew"

```

And then `welcome to Pulsar Functions` is displayed in the console output.

:::note

Instead of passing a string via the CLI, you can trigger Pulsar Functions with the contents of a file using the `--triggerFile` flag.

:::

## Processing guarantees

Pulsar Functions provide three different messaging semantics that you can apply to any function.

Delivery semantics | Description
:------------------|:-------
**At-most-once** delivery | Each message sent to the function is likely to be processed, or not to be processed (hence "at most").
**At-least-once** delivery | Each message sent to the function can be processed more than once (hence the "at least").
**Effectively-once** delivery | Each message sent to the function will have one output associated with it.

This command, for example, runs a function in the [cluster mode](#cluster-mode) with effectively-once guarantees applied.

```bash

$ bin/pulsar-admin functions create \
  --name my-effectively-once-function \
  --processing-guarantees EFFECTIVELY_ONCE \
  # Other function configs

```

## Metrics
Pulsar Functions that use [Pulsar Functions SDK](#the-pulsar-functions-sdk) can publish metrics to Pulsar. For more information, see [Metrics for Pulsar Functions](functions-metrics).

## State storage
Pulsar Functions use [Apache BookKeeper](https://bookkeeper.apache.org) as a state storage interface. Pulsar installation, including the local standalone installation, includes deployment of BookKeeper bookies.