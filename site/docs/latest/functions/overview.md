---
title: Pulsar Functions overview
lead: A bird's-eye look at Pulsar's lightweight, developer-friendly compute platform
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

Functions are executed each time a message is published to the input topic. If a function is listening on the topic `tweet-stream`, for example, then the function would be run each time a message.

> Pulsar features automatic message deduplication

### Goals

Core goal: make Pulsar do real heavy lifting without needing to deploy a neighboring system (Storm, Heron, Flink, etc.). Ready-made compute infrastructure at your disposal.

* Developer productivity (easy troubleshooting and deployment)
  * "Serverless" philosophy
* No need for a separate SPE

### Inspirations

* AWS Lambda, Google Cloud Functions, etc.
* FaaS
* Serverless/NoOps philosophy

### Command-line interface

You can manage Pulsar Functions using the [`pulsar-functions`](../../reference/CliTools#pulsar-functions) CLI tool. Here's an example command that would

```bash
$ bin/pulsar-functions localrun \
  --inputs persistent://sample/standalone/ns1/test_src \
  --output persistent://sample/standalone/ns1/test_result \
  --jar examples/api-examples.jar \
  --className org.apache.pulsar.functions.api.examples.ExclamationFunction
```

### Supported languages

Pulsar Functions can currently be written in [Java](../../functions/api#java) and [Python](../../functions/api#python). Support for additional languages is coming soon.

### Runtime

### Deployment modes

* Local run
* Cluster run

### Delivery semantics

* At most once
* At least once
* Effectively once

### State storage

### Metrics

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

### Logging

### Data types

* Strongly typed
