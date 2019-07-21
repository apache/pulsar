---
id: version-2.1.0-incubating-functions-metrics
title: Metrics for Pulsar Functions
sidebar_label: Metrics
original_id: functions-metrics
---

Pulsar Functions can publish arbitrary metrics to the metrics interface which can then be queried. This doc contains instructions for publishing metrics using the [Java](#java-sdk) and [Python](#python-sdk) Pulsar Functions SDKs.

> #### Metrics and stats not available through language-native interfaces
> If a Pulsar Function uses the language-native interface for [Java](functions-api.md#java-native-functions) or [Python](#python-native-functions), that function will not be able to publish metrics and stats to Pulsar.

## Accessing metrics

For a guide to accessing metrics created by Pulsar Functions, see the guide to [Monitoring](deploy-monitoring.md) in Pulsar.

## Java SDK

If you're creating a Pulsar Function using the [Java SDK](functions-api.md#java-sdk-functions), the {@inject: javadoc:Context:/pulsar-functions/org/apache/pulsar/functions/api/Context} object has a `recordMetric` method that you can use to register both a name for the metric and a value. Here's the signature for that method:

```java
void recordMetric(String metricName, double value);
```

Here's an example function:

```java
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

public class MetricRecordingFunction implements Function<String, Void> {
    @Override
    public void apply(String input, Context context) {
        context.recordMetric("number-of-characters", input.length());
        return null;
    }
}
```

This function counts the length of each incoming message (of type `String`) and then registers that under the `number-of-characters` metric.

## Python SDK

Documentation for the [Python SDK](functions-api.md#python-sdk-functions) is coming soon.
