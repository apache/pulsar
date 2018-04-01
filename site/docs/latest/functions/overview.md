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
* "Serverles" and "Function as a Service" (FAS) cloud platforms like [Amazon Web Services Lambda](https://aws.amazon.com/lambda/), [Google Cloud Functions](https://cloud.google.com/functions/), and [Azure Cloud Functions](https://azure.microsoft.com/en-us/services/functions/)

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

* Via [command-line arguments](#cli)
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

## Supported languages

Pulsar Functions can currently be written in [Java](../../functions/api#java) and [Python](../../functions/api#python). Support for additional languages is coming soon.

### Language-native functions {#native}

Both Java and Python support writing "native" functions, i.e. Pulsar Functions with no dependencies.

The benefit of native functions is that they don't have any dependencies beyond what's already available in Java/Python "out of the box." The downside is that they don't provide access to the function's [context](#context)

### The Pulsar Functions SDK {#sdk}

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

This command will upload `myfunc.py` to Pulsar, which will use the code to start one [or more]()

### Parallelism

{% include admonition.html type="info" %}

### Logging

## Delivery semantics

* At most once
* At least once
* Effectively once

## Metrics

Here's an example function that publishes a value of 1 to the `my-metric` metric.

```java
public class MetricsFunction implements PulsarFunction<String, Void> {
    @Override
    public Void process(String input, Context context) {
        context.recordMetric("my-metric", 1);
        return null;
    }
}
```

