---
title: Pulsar Functions overview
lead: A bird's-eye look at Pulsar's lightweight, developer-friendly compute platform
preview: true
---

**Pulsar Functions** are lightweight compute processes that

* consume {% popover messages %} from one or more Pulsar {% popover topics %},
* apply a user-supplied processing logic to each message,
* publish the results of the computation to another topic

Here's an example Pulsar Function for Java:

```java
import java.util.Function;

public class ExclamationFunction implements Function<String, String> {
    @Override
    public String apply(String input) { return String.format("%s!", input); }
}
```

Functions are executed each time a message is published to the input topic. If a function is listening on the topic `tweet-stream`, for example, then the function would be run each time a message is published to that topic.

## Goals

The core goal behind Pulsar Functions is to enable you to easily create processing logic of any level of complexity without needing to deploy a separate neighboring system (such as [Apache Storm](http://storm.apache.org/), [Apache Heron](https://apache.github.io/incubator-heron), [Apache Flink](https://flink.apache.org/), etc.). Pulsar Functions is essentially ready-made compute infrastructure at your disposal as part of your Pulsar messaging system. This core goal is tied to a series of other goals:

* Developer productive ([language-native](#native) vs. [Pulsar Functions SDK](#sdk) functions)
* easy troubleshooting
* Operational simplicity (no need for an external system)

## Inspirations

The Pulsar Functions feature was inspired by (and takes cues from) several systems and paradigms:

* Stream processing engines such as [Apache Storm](http://storm.apache.org/), [Apache Heron](https://apache.github.io/incubator-hero), and [Apache Flink](https://flink.apache.org)
* "Serverless" and "Function as a Service" (FaaS) cloud platforms like [Amazon Web Services Lambda](https://aws.amazon.com/lambda/), [Google Cloud Functions](https://cloud.google.com/functions/), and [Azure Cloud Functions](https://azure.microsoft.com/en-us/services/functions/)

Pulsar Functions could be described as

* [Lambda](https://aws.amazon.com/lambda/)-style functions that are
* specifically designed to work with Pulsar

## Command-line interface {#cli}

Pulsar Functions are managed using the [`pulsar-admin`](../../reference/CliTools#pulsar-admin) CLI tool (in particular the [`functions`](../../reference/CliTools#pulsar-admin-functions) command). Here's an example command that would run a function in [local run mode](#local-run):

```bash
$ bin/pulsar-functions localrun \
  --inputs persistent://sample/standalone/ns1/test_src \
  --output persistent://sample/standalone/ns1/test_result \
  --jar examples/api-examples.jar \
  --className org.apache.pulsar.functions.api.examples.ExclamationFunction
```

## Configuration

Pulsar Functions can be configured in two ways:

* Via [command-line arguments](#cli) passed to the [`pulsar-admin functions`](../../reference/CliTools#pulsar-admin-functions) interface
* Via [YAML](http://yaml.org/) configuration files

If you're supplying a YAML configuration, you must specify a path to the file on the command line. Here's an example:

```bash
$ bin/pulsar-admin functions create \
  --functionConfigFile ./my-function.yaml
```

And here's an example `my-function.yaml` file:

```yaml
name: my-function
tenant: sample
namespace: ns1
jar: ./target/my-functions.jar
className: org.example.pulsar.functions.MyFunction
inputs:
- persistent://sample/standalone/ns1/test_src
output: persistent://sample/standalone/ns1/test_result
```

You can also mix and match configuration methods by specifying some function attributes via the CLI and others via YAML configuration.

## Supported languages

Pulsar Functions can currently be written in [Java](../../functions/api#java) and [Python](../../functions/api#python). Support for additional languages is coming soon.

## Function context {#context}

Each Pulsar Function created using the [Pulsar Functions SDK](#sdk) has access to a context object that both provides:

1. A wide variety of information about the function, including:
  * The name of the function
  * The {% popover tenant %} and {% popover namespace %} of the function
  * [User-supplied configuration]() values

### Language-native functions {#native}

Both Java and Python support writing "native" functions, i.e. Pulsar Functions with no dependencies.

The benefit of native functions is that they don't have any dependencies beyond what's already available in Java/Python "out of the box." The downside is that they don't provide access to the function's [context](#context)

### The Pulsar Functions SDK {#sdk}

If you'd like a Pulsar Function to have access to a [context object](#context), you can use the Pulsar Functions SDK, available for both [Java](../api#java-sdk) and [Pythnon](../api#python-sdk).

Here's an example Java function that uses information about its context:

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
        String functionName = context.getName();
        LOG.info("{}/{}/{}", functionTenant, functionNamespace, functionName);
        return null;
    }
}
```

## Deployment modes

The Pulsar Functions feature was built to support a variety of deployment options. At the moment, there are two ways to run Pulsar Functions:

Deployment mode | Description
:---------------|:-----------
Local run mode | The function runs in your local environment, for example on your laptop
Cluster mode | The function runs *inside of* your Pulsar cluster, on the same machines as your Pulsar {% popover brokers %}

### Local run mode {#local-run}

If you run a Pulsar Function in **local run** mode, it will run on the machine from which the command is run (this could be your laptop, an [AWS EC2](https://aws.amazon.com/ec2/) instance, etc.). Here's an example [`localrun`](../../CliTools#pulsar-admin-functions-localrun) command:

```bash
$ bin/pulsar-admin functions localrun \
  --py myfunc.py \
  --className myfunc.SomeFunction \
  --inputs persistent://sample/standalone/ns1/input-1 \
  --output persistent://sample/standalone/ns1/output-1
```

By default, the function will connect to a Pulsar cluster running on the same machine, via a local {% popover broker %} service URL of `pulsar://localhost:6650`. If you'd like to use local run mode to run a function but connect it to a non-local Pulsar cluster, you can specify a different broker URL using the `--brokerServiceUrl` flag. Here's an example:

```bash
$ bin/pulsar-admin functions localrun \
  --brokerServiceUrl pulsar://my-cluster-host:6650 \
  # Other function parameters
```

### Cluster run mode {#cluster-run}

When you run a Pulsar Function in **cluster mode**, the function code will be uploaded to a Pulsar {% popover broker %} and run *alongside the broker* rather than in your [local environment](#local-run). You can run a function in cluster mode using the [`create`](../../CliTools#pulsar-admin-functions-create) command. Here's an example:

```bash
$ bin/pulsar-admin functions create \
  --py myfunc.py \
  --className myfunc.SomeFunction \
  --inputs persistent://sample/standalone/ns1/input-1 \
  --output persistent://sample/standalone/ns1/output-1
```

This command will upload `myfunc.py` to Pulsar, which will use the code to start one [or more](#parallelism) instances of the function.

### Parallelism

By default, only one **instance** of a Pulsar Function runs when you create and run it in [cluster run mode](#cluster-run). You can also, however, run multiple instances in parallel. You can specify the number of instances when you create the function, or update an existing single-instance function with a new parallelism factor.

This command, for example, would create and run a function with a parallelism of 5 (i.e. 5 instances):

```bash
$ bin/pulsar-admin functions create \
  --name parallel-fun \
  --tenant sample \
  --namespace ns1 \
  --py func.py \
  --className func.ParallelFunction \
  --parallelism 5
```

### Logging

Pulsar Functions created using the [Pulsar Functions SDK(#sdk) can send logs to a log topic that you specify as part of the function's configuration. The function created using the command below, for example, would produce all logs on the `persistent://sample/standalone/ns1/my-func-1-log` topic:

```bash
$ bin/pulsar-admin functions create \
  --name my-func-1 \
  --logTopic persistent://sample/standalone/ns1/my-func-1-log \
  # Other configs
```

Here's an example [Java function](../api#java-logging) that logs at different log levels based on the function's input:

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

### User configuration {#user-config}

Pulsar Functions can be passed arbitrary key-values via the command line (both keys and values must be strings). This set of key-values is called the functions **user configuration**. User configurations must consist of JSON strings.

Here's an example of passing a user configuration to a function:

```bash
$ bin/pulsar-admin functions create \
  --userConfig '{"key-1":"value-1","key-2","value-2"}' \
  # Other configs
```

Here's an example of a function that accesses that config map:

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

## Metrics

Pulsar Functions that use the [Pulsar Functions SDK](#sdk) can publish metrics to Pulsar. For more information, see [Metrics for Pulsar Functions](../metrics).