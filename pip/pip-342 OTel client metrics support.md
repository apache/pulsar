# PIP 342: Support OpenTelemetry metrics in Pulsar client

## Motivation

Current support for metric instrumentation in Pulsar client is very limited and poses a lot of
issues for integrating the metrics into any telemetry system.

We have 2 ways that metrics are exposed today:

1. Printing logs every 1 minute: While this is ok as it comes out of the box, it's very hard for
   any application to get the data or use it in any meaningful way.
2. `producer.getStats()` or `consumer.getStats()`: Calling these methods will get access to
   the rate of events in the last 1-minute interval. This is problematic because out of the
   box the metrics are not collected anywhere. One would have to start its own thread to
   periodically check these values and export them to some other system.

Neither of these mechanism that we have today are sufficient to enable application to easily
export the telemetry data of Pulsar client SDK.

## Goal

Provide a good way for applications to retrieve and analyze the usage of Pulsar client operation,
in particular with respect to:

1. Maximizing compatibility with existing telemetry systems
2. Minimizing the effort required to export these metrics

## Why OpenTelemetry?

[OpenTelemetry](https://opentelemetry.io/) is quickly becoming the de-facto standard API for metric and
tracing instrumentation. In fact, as part of [PIP-264](https://github.com/apache/pulsar/blob/master/pip/pip-264.md),
we are already migrating the Pulsar server side metrics to use OpenTelemetry.

For Pulsar client SDK, we need to provide a similar way for application builder to quickly integrate and
export Pulsar metrics.

### Why exposing OpenTelemetry directly in Pulsar API

When deciding how to expose the metrics exporter configuration there are multiple options: 

 1. Accept an `OpenTelemetry` object directly in Pulsar API
 2. Build a pluggable interface that describe all the Pulsar client SDK events and allow application to
    provide an implementation, perhaps providing an OpenTelemetry included option.

For this proposal, we are following the (1) option. Here are the reasons:

 1. In a way, OpenTelemetry can be compared to [SLF4J](https://www.slf4j.org/), in the sense that it provides an API
    on top of which different vendor can build multiple implementations. Therefore, there is no need to create a new
    Pulsar-specific interface
 2. OpenTelemetry has 2 main artifacts: API and SDK. For the context of Pulsar client, we will only depend on its
    API. Applications that are going to use OpenTelemetry, will include the OTel SDK
 3. Providing a custom interface has several drawbacks:
     1. Applications need to update their implementations every time a new metric is added in Pulsar SDK
     2. The surface of this plugin API can become quite big when there are several metrics
     3. If we imagine an application that uses multiple libraries, like Pulsar SDK, and each of these has its own
        custom way to expose metrics, we can see the level of integration burden that is pushed to application
        developers
 4. It will always be easy to use OpenTelemetry to collect the metrics and export them using a custom metrics API. There
    are several examples of this in OpenTelemetry documentation.

## Public API changes

### Enabling OpenTelemetry

When building a `PulsarClient` instance, it will be possible to pass an `OpenTelemetry` object:

```java
interface ClientBuilder {
    // ...
    ClientBuilder openTelemetry(io.opentelemetry.api.OpenTelemetry openTelemetry);

    ClientBuilder openTelemetryMetricsCardinality(MetricsCardinality metricsCardinality);
}
```

The common usage for an application would be something like:

```java
// Creates a OpenTelemetry instance using environment variables to configure it
OpenTelemetry otel=AutoConfiguredOpenTelemetrySdk.builder()
        .build().getOpenTelemetrySdk();

        PulsarClient client=PulsarClient.builder()
        .serviceUrl("pulsar://localhost:6650")
        .build();

// ....
```

Cardinality enum will allow to select a default cardinality label to be attached to the
metrics:

```java
public enum MetricsCardinality {
    /**
     * Do not add additional labels to metrics
     */
    None,

    /**
     * Label metrics by tenant
     */
    Tenant,

    /**
     * Label metrics by tenant and namespace
     */
    Namespace,

    /**
     * Label metrics by topic
     */
    Topic,

    /**
     * Label metrics by each partition
     */
    Partition,
}
```

The labels are addictive. For example, selecting `Topic` level would mean that the metrics will be
labeled like:

```
pulsar_client_received_total{namespace="public/default",tenant="public",topic="persistent://public/default/pt"} 149.0
```

While selecting `Namespace` level:

```
pulsar_client_received_total{namespace="public/default",tenant="public"} 149.0
```

### Deprecating the old stats methods

The old way of collecting stats will be disabled by default, deprecated and eventually removed
in Pulsar 4.0 release.

Methods to deprecate:

```java
interface ClientBuilder {
    // ...
    @Deprecated
    ClientBuilder statsInterval(long statsInterval, TimeUnit unit);
}

interface Producer {
    @Deprecated
    ProducerStats getStats();
}

interface Consumer {
    @Deprecated
    ConsumerStats getStats();
}
```

## Initial set of metrics to include

Based on the experience of Pulsar Go client SDK metrics (
see: https://github.com/apache/pulsar-client-go/blob/master/pulsar/internal/metrics.go),
this is the proposed initial set of metrics to export.

Additional metrics could be added later on, though it's better to start with the set of most important metrics
and then evaluate any missing information.

| OTel metric name                                | Type      | Unit        | Description                                                                                    |
|-------------------------------------------------|-----------|-------------|------------------------------------------------------------------------------------------------|
| `pulsar.client.connections.opened`              | Counter   | connections | Counter of connections opened                                                                  |
| `pulsar.client.connections.closed`              | Counter   | connections | Counter of connections closed                                                                  |
| `pulsar.client.connections.failed`              | Counter   | connections | Counter of connections establishment failures                                                  |
| `pulsar.client.session.opened`                  | Counter   | sessions    | Counter of sessions opened. `type="producer"` or `consumer`                                    |
| `pulsar.client.session.closed`                  | Counter   | sessions    | Counter of sessions closed. `type="producer"` or `consumer`                                    |
| `pulsar.client.received`                        | Counter   | messages    | Number of messages received                                                                    |
| `pulsar.client.received`                        | Counter   | bytes       | Number of bytes received                                                                       |
| `pulsar.client.consumer.preteched.messages`     | Gauge     | messages    | Number of messages currently sitting in the consumer pre-fetch queue                           |
| `pulsar.client.consumer.preteched`              | Gauge     | bytes       | Total number of bytes currently sitting in the consumer pre-fetch queue                        |
| `pulsar.client.consumer.ack`                    | Counter   | messages    | Number of ack operations                                                                       |
| `pulsar.client.consumer.nack`                   | Counter   | messages    | Number of negative ack operations                                                              |
| `pulsar.client.consumer.dlq`                    | Counter   | messages    | Number of messages sent to DLQ                                                                 |
| `pulsar.client.consumer.ack.timeout`            | Counter   | messages    | Number of ack timeouts events                                                                  |
| `pulsar.client.producer.latency`                | Histogram | seconds     | Publish latency experienced by the application, includes client batching time                  |
| `pulsar.client.producer.rpc.latency`            | Histogram | seconds     | Publish RPC latency experienced internally by the client when sending data to receiving an ack |
| `pulsar.client.producer.published`              | Counter   | bytes       | Bytes published                                                                                |
| `pulsar.client.producer.pending.messages.count` | Gauge     | messages    | Pending messages for this producer                                                             |
| `pulsar.client.producer.pending.count`          | Gauge     | bytes       | Pending bytes for this producer                                                                |

Topic lookup metric will be differentiated by the lookup type label and by the lookup transport
mechanism (`transport-type="binary|http"`):

| OTel metric name                           | Type      | Unit    | Description                                      |
|--------------------------------------------|-----------|---------|--------------------------------------------------|
| `pulsar.client.lookup{type="topic"}`       | Histogram | seconds | Counter of topic lookup operations               |
| `pulsar.client.lookup{type="metadata"}`    | Histogram | seconds | Counter of topic partitioned metadata operations |
| `pulsar.client.lookup{type="schema"}`      | Histogram | seconds | Counter of schema retrieval operations           |
| `pulsar.client.lookup{type="list-topics"}` | Histogram | seconds | Counter of namespace list topics operations      |

Additionally, all the histograms will have a `success=true|false` label to distinguish successful and failed
operations.
