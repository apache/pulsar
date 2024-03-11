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
}
```

The common usage for an application would be something like:

```java
// Creates a OpenTelemetry instance using environment variables to configure it
OpenTelemetry otel = AutoConfiguredOpenTelemetrySdk.builder().build()
        .getOpenTelemetrySdk();

PulsarClient client = PulsarClient.builder()
        .serviceUrl("pulsar://localhost:6650")
        .build();

// ....
```

Even without passing the `OpenTelemetry` instance to Pulsar client SDK, an application using the OpenTelemetry
agent, will be able to instrument the Pulsar client automatically, because we default to use `GlobalOpenTelemetry.get()`. 

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

| OTel metric name                                | Type          | Unit        | Description                                                                                                                               |
|-------------------------------------------------|---------------|-------------|-------------------------------------------------------------------------------------------------------------------------------------------|
| `pulsar.client.connection.opened`               | Counter       | connections | The number of connections opened                                                                                                          |
| `pulsar.client.connection.closed`               | Counter       | connections | The number of connections closed                                                                                                          |
| `pulsar.client.connection.failed`               | Counter       | connections | The number of failed connection attempts                                                                                                  |
| `pulsar.client.producer.opened`                 | Counter       | sessions    | The number of producer sessions opened                                                                                                    |
| `pulsar.client.producer.closed`                 | Counter       | sessions    | The number of producer sessions closed                                                                                                    |
| `pulsar.client.consumer.opened`                 | Counter       | sessions    | The number of consumer sessions opened                                                                                                    |
| `pulsar.client.consumer.closed`                 | Counter       | sessions    | The number of consumer sessions closed                                                                                                    |
| `pulsar.client.consumer.message.received.count` | Counter       | messages    | The number of messages explicitly received by the consumer application                                                                    |
| `pulsar.client.consumer.message.received.size`  | Counter       | bytes       | The number of bytes explicitly received by the consumer application                                                                       |
| `pulsar.client.consumer.receive_queue.count`    | UpDownCounter | messages    | Number of messages currently sitting in the consumer receive queue                                                                        |
| `pulsar.client.consumer.receive_queue.size`     | UpDownCounter | bytes       | Total number of bytes currently sitting in the consumer receive queue                                                                     |
| `pulsar.client.consumer.message.ack`            | Counter       | messages    | The number of acknowledged messages                                                                                                       |
| `pulsar.client.consumer.message.nack`           | Counter       | messages    | The number of negatively acknowledged messages                                                                                            |
| `pulsar.client.consumer.message.dlq`            | Counter       | messages    | The number of messages sent to DLQ                                                                                                        |
| `pulsar.client.consumer.message.ack.timeout`    | Counter       | messages    | The number of messages that were not acknowledged in the configured timeout period, hence, were requested by the client to be redelivered |
| `pulsar.client.producer.message.send.duration`  | Histogram     | seconds     | Publish latency experienced by the application, includes client batching time                                                             |
| `pulsar.client.producer.rpc.send.duration`      | Histogram     | seconds     | Publish RPC latency experienced internally by the client when sending data to receiving an ack                                            |
| `pulsar.client.producer.message.send.size`      | Counter       | bytes       | The number of bytes published                                                                                                             |
| `pulsar.client.producer.message.pending.count"` | UpDownCounter | messages    | The number of messages in the producer internal send queue, waiting to be sent                                                            |
| `pulsar.client.producer.message.pending.size`   | UpDownCounter | bytes       | The size of the messages in the producer internal queue, waiting to sent                                                                  |

Topic lookup metric will be differentiated by the lookup type label and by the lookup transport
mechanism (`pulsar.lookup.transport-type="binary|http"`):

| OTel metric name                                                  | Type      | Unit    | Description                                       |
|-------------------------------------------------------------------|-----------|---------|---------------------------------------------------|
| `pulsar.client.lookup.duration{pulsar.lookup.type="topic"}`       | Histogram | seconds | Duration of topic lookup operations               |
| `pulsar.client.lookup.duration{pulsar.lookup.type="metadata"}`    | Histogram | seconds | Duration of topic partitioned metadata operations |
| `pulsar.client.lookup.duration{pulsar.lookup.type="schema"}`      | Histogram | seconds | Duration of schema retrieval operations           |
| `pulsar.client.lookup.duration{pulsar.lookup.type="list-topics"}` | Histogram | seconds | Duration of namespace list topics operations      |

Additionally, all the histograms will have a `pulsar.response.status=success|failed` label to distinguish successful and failed
operations.

## Metrics cardinality

The metrics data point will be tagged with these attributes: 

   * `pulsar.tenant`
   * `pulsar.namespace`
   * `pulsar.topic`
   * `pulsar.partition`

By default the metrics will be exported with tenant and namespace attributes set. If an application wants to enable
a finer level, with higher cardinality, it can do so by using OpenTelemetry configuration.

