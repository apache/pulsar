---
title: Metrics and stats for Pulsar Functions
---

Pulsar Functions can publish arbitrary metrics to the metrics interface (which can then be queried).

## Java API

To publish a metric to the metrics interface:

```java
void recordMetric(String metricName, double value);
```

Here's an example:

```java
Context.recordMetric("my-custom-metrics", 475);
```