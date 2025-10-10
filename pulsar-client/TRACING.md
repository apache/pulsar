# OpenTelemetry Tracing for Pulsar Java Client

This document describes how to use OpenTelemetry distributed tracing with the Pulsar Java client.

## Overview

The Pulsar Java client provides built-in support for OpenTelemetry distributed tracing. This allows you to:

- Trace message publishing from producer to broker
- Trace message consumption from broker to consumer
- Propagate trace context across services via message properties
- Extract trace context from external sources (e.g., HTTP requests)
- Create end-to-end traces across your distributed system

## Features

### Producer Tracing

Producer tracing creates spans for:
- **send** - Span starts when `send()` or `sendAsync()` is called and completes when broker acknowledges receipt

### Consumer Tracing

Consumer tracing creates spans for:
- **process** - Span starts when message is received and completes when message is acknowledged, negatively acknowledged, or ack timeout occurs

### Trace Context Propagation

Trace context is automatically propagated using W3C TraceContext format:
- `traceparent` - Contains trace ID, span ID, and trace flags
- `tracestate` - Contains vendor-specific trace information

Context is injected into and extracted from message properties, enabling seamless trace propagation across services.

## Quick Start

### 1. Add Dependencies

The Pulsar client already includes OpenTelemetry API dependencies. You'll need to add the SDK and exporters:

```xml
<dependency>
    <groupId>io.opentelemetry</groupId>
    <artifactId>opentelemetry-sdk</artifactId>
    <version>${opentelemetry.version}</version>
</dependency>
<dependency>
    <groupId>io.opentelemetry</groupId>
    <artifactId>opentelemetry-exporter-otlp</artifactId>
    <version>${opentelemetry.version}</version>
</dependency>
```

### 2. Enable Tracing

There are three ways to enable tracing:

#### Option 1: Using OpenTelemetry Java Agent (Easiest)

```bash
# Start your application with the Java Agent
java -javaagent:opentelemetry-javaagent.jar \
     -Dotel.service.name=my-service \
     -Dotel.exporter.otlp.endpoint=http://localhost:4317 \
     -jar your-application.jar
```

```java
// Just enable tracing - uses GlobalOpenTelemetry from the agent
PulsarClient client = PulsarClient.builder()
    .serviceUrl("pulsar://localhost:6650")
    .enableTracing(true)  // That's it!
    .build();
```

#### Option 2: With Explicit OpenTelemetry Instance

```java
OpenTelemetry openTelemetry = // configure your OpenTelemetry instance

PulsarClient client = PulsarClient.builder()
    .serviceUrl("pulsar://localhost:6650")
    .openTelemetry(openTelemetry, true)  // Set OpenTelemetry AND enable tracing
    .build();
```

#### Option 3: Using GlobalOpenTelemetry

```java
// Configure GlobalOpenTelemetry once in your application
GlobalOpenTelemetry.set(myOpenTelemetry);

// Enable tracing in the client - uses GlobalOpenTelemetry
PulsarClient client = PulsarClient.builder()
    .serviceUrl("pulsar://localhost:6650")
    .enableTracing(true)
    .build();
```

**What happens when tracing is enabled:**
- **Create spans** for producer send operations
- **Inject trace context** into message properties automatically
- **Create spans** for consumer receive/ack operations
- **Extract trace context** from message properties automatically
- Link all spans to create end-to-end distributed traces

### 3. Manual Interceptor Configuration (Advanced)

If you prefer manual control, you can add interceptors explicitly:

```java
import org.apache.pulsar.client.impl.tracing.OpenTelemetryProducerInterceptor;
import org.apache.pulsar.client.impl.tracing.OpenTelemetryConsumerInterceptor;

// Create client (tracing not enabled globally)
PulsarClient client = PulsarClient.builder()
    .serviceUrl("pulsar://localhost:6650")
    .openTelemetry(openTelemetry)
    .build();

// Add interceptor manually to specific producer
Producer<String> producer = client.newProducer(Schema.STRING)
    .topic("my-topic")
    .intercept(new OpenTelemetryProducerInterceptor())
    .create();

// Add interceptor manually to specific consumer
Consumer<String> consumer = client.newConsumer(Schema.STRING)
    .topic("my-topic")
    .subscriptionName("my-subscription")
    .intercept(new OpenTelemetryConsumerInterceptor<>())
    .subscribe();
```

## Advanced Usage

### End-to-End Tracing Example

This example shows how to create a complete trace from an HTTP request through Pulsar to a consumer:

```java
// Service 1: HTTP API that publishes to Pulsar
@POST
@Path("/order")
public Response createOrder(@Context HttpHeaders headers, Order order) {
    // Extract trace context from incoming HTTP request
    Context context = TracingProducerBuilder.extractFromHeaders(
        convertHeaders(headers));

    // Publish to Pulsar with trace context
    TracingProducerBuilder tracingBuilder = new TracingProducerBuilder();
    producer.newMessage()
        .value(order)
        .let(builder -> tracingBuilder.injectContext(builder, context))
        .send();

    return Response.accepted().build();
}

// Service 2: Pulsar consumer that processes orders
Consumer<Order> consumer = client.newConsumer(Schema.JSON(Order.class))
    .topic("orders")
    .subscriptionName("order-processor")
    .intercept(new OpenTelemetryConsumerInterceptor<>())
    .subscribe();

while (true) {
    Message<Order> msg = consumer.receive();
    // Trace context is automatically extracted
    // Any spans created here will be part of the same trace
    processOrder(msg.getValue());
    consumer.acknowledge(msg);
}
```

### Custom Span Creation

You can create custom spans during message processing:

```java
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;

Tracer tracer = GlobalOpenTelemetry.get().getTracer("my-app");

Message<String> msg = consumer.receive();

// Create a custom span for processing
Span span = tracer.spanBuilder("process-message")
    .setSpanKind(SpanKind.INTERNAL)
    .startSpan();

try (Scope scope = span.makeCurrent()) {
    // Your processing logic
    processMessage(msg.getValue());
    span.setStatus(StatusCode.OK);
} catch (Exception e) {
    span.recordException(e);
    span.setStatus(StatusCode.ERROR);
    throw e;
} finally {
    span.end();
    consumer.acknowledge(msg);
}
```

## Configuration

### Compatibility with OpenTelemetry Java Agent

This implementation is **fully compatible** with the [OpenTelemetry Java Instrumentation](https://github.com/open-telemetry/opentelemetry-java-instrumentation/tree/main/instrumentation/pulsar) for Pulsar:

- Both use **W3C TraceContext** format (traceparent, tracestate headers)
- Both propagate context via **message properties**
- **No conflicts**: Our implementation checks if trace context is already present (from Java Agent) and avoids duplicate injection
- You can use either approach or both together

### Using OpenTelemetry Java Agent

The easiest way to enable tracing is using the OpenTelemetry Java Agent (automatic instrumentation):

```bash
java -javaagent:path/to/opentelemetry-javaagent.jar \
     -Dotel.service.name=my-service \
     -Dotel.exporter.otlp.endpoint=http://localhost:4317 \
     -jar your-application.jar
```

**Note**: When using the Java Agent, you don't need to call `.openTelemetry(otel, true)` as the agent automatically instruments Pulsar. However, calling it won't cause conflicts.

### Programmatic Configuration

You can also configure OpenTelemetry programmatically:

```java
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;

OtlpGrpcSpanExporter spanExporter = OtlpGrpcSpanExporter.builder()
    .setEndpoint("http://localhost:4317")
    .build();

SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
    .addSpanProcessor(BatchSpanProcessor.builder(spanExporter).build())
    .build();

OpenTelemetrySdk openTelemetry = OpenTelemetrySdk.builder()
    .setTracerProvider(tracerProvider)
    .buildAndRegisterGlobal();
```

### Environment Variables

Configure via environment variables:

```bash
export OTEL_SERVICE_NAME=my-service
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
export OTEL_TRACES_EXPORTER=otlp
export OTEL_METRICS_EXPORTER=otlp
```

## Span Attributes

The tracing implementation adds the following attributes to spans following the [OpenTelemetry messaging semantic conventions](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/):

### Producer Spans
- `messaging.system`: "pulsar"
- `messaging.destination.name`: Topic name
- `messaging.operation.name`: "send"
- `messaging.message.id`: Message ID (added when broker confirms)

**Span naming**: `send {topic}` (e.g., "send my-topic")

### Consumer Spans
- `messaging.system`: "pulsar"
- `messaging.destination.name`: Topic name
- `messaging.operation.name`: "process"
- `messaging.message.id`: Message ID
- `messaging.pulsar.acknowledgment.type`: How the message was acknowledged
  - `"acknowledge"`: Normal individual acknowledgment
  - `"cumulative_acknowledge"`: Cumulative acknowledgment
  - `"negative_acknowledge"`: Message negatively acknowledged (will retry)
  - `"ack_timeout"`: Acknowledgment timeout occurred (will retry)

**Span naming**: `process {topic}` (e.g., "process my-topic")

## Span Lifecycle and Acknowledgment Behavior

Understanding how spans are handled for different acknowledgment scenarios. Every consumer span includes a `messaging.pulsar.acknowledgment.type` attribute indicating how it was completed:

### Successful Acknowledgment
- Span ends with **OK** status
- Attribute: `messaging.pulsar.acknowledgment.type = "acknowledge"`

### Cumulative Acknowledgment
- Span ends with **OK** status
- Attribute: `messaging.pulsar.acknowledgment.type = "cumulative_acknowledge"`
- All spans up to the acknowledged position are ended with this attribute

### Negative Acknowledgment
- Span ends with **OK** status (not an error)
- Attribute: `messaging.pulsar.acknowledgment.type = "negative_acknowledge"`
- This is normal flow, not a failure - the message will be redelivered and a new span will be created

### Acknowledgment Timeout
- Span ends with **OK** status (not an error)
- Attribute: `messaging.pulsar.acknowledgment.type = "ack_timeout"`
- This is expected behavior when `ackTimeout` is configured - the message will be redelivered and a new span will be created

### Application Exception During Processing
- If your application code throws an exception, create a child span and mark it with ERROR status
- The consumer span itself will end normally when you call `negativeAcknowledge()`
- This provides clear separation between messaging operations (OK) and application logic (ERROR)

**Example - Separating messaging and application errors**:
```java
Message<String> msg = consumer.receive();
Span processingSpan = tracer.spanBuilder("business-logic").startSpan();
try (Scope scope = processingSpan.makeCurrent()) {
    processMessage(msg.getValue());
    processingSpan.setStatus(StatusCode.OK);
    consumer.acknowledge(msg);  // Consumer span ends with acknowledgment.type="acknowledge"
} catch (Exception e) {
    processingSpan.recordException(e);
    processingSpan.setStatus(StatusCode.ERROR);  // Business logic failed
    consumer.negativeAcknowledge(msg);  // Consumer span ends with acknowledgment.type="negative_acknowledge"
    throw e;
} finally {
    processingSpan.end();
}
```

### Querying by Acknowledgment Type

The `messaging.pulsar.acknowledgment.type` attribute allows you to filter and analyze spans:

**Example queries in your tracing backend**:
- Find all retried messages: `messaging.pulsar.acknowledgment.type = "negative_acknowledge" OR "ack_timeout"`
- Calculate retry rate: `count(negative_acknowledge) / count(acknowledge)`
- Identify timeout issues: `messaging.pulsar.acknowledgment.type = "ack_timeout"`
- Analyze cumulative vs individual acks: Group by `messaging.pulsar.acknowledgment.type`

## Best Practices

1. **Always use interceptors**: Add tracing interceptors to both producers and consumers for complete visibility.

2. **Propagate context from HTTP**: When publishing from HTTP endpoints, always extract and propagate the trace context.

3. **Handle errors properly**: Ensure spans are ended even when exceptions occur.

4. **Distinguish messaging vs. application errors**:
   - Messaging operations (nack, timeout) end with OK status + events
   - Application failures should be tracked in separate child spans with ERROR status

5. **Use meaningful span names**: The default span names include the topic name for easy identification.

6. **Consider performance**: Tracing adds minimal overhead, but in high-throughput scenarios, consider sampling.

7. **Clean up resources**: Ensure interceptors and OpenTelemetry SDK are properly closed when shutting down.

## Troubleshooting

### Traces not appearing

1. Verify OpenTelemetry SDK is configured and exporters are set up
2. Check that interceptors are added to producers/consumers
3. Verify trace exporter endpoint is reachable
4. Enable debug logging: `-Dio.opentelemetry.javaagent.debug=true`

### Missing parent-child relationships

1. Ensure trace context is being injected via `TracingProducerBuilder.injectContext()`
2. Verify message properties contain `traceparent` header
3. Check that both producer and consumer have tracing interceptors

### High overhead

1. Consider using sampling: `-Dotel.traces.sampler=parentbased_traceidratio -Dotel.traces.sampler.arg=0.1`
2. Use batch span processor (default)
3. Adjust batch processor settings if needed

## Examples

See the following files for complete examples:
- `TracingExampleTest.java` - Comprehensive usage examples
- `OpenTelemetryTracingTest.java` - Unit tests demonstrating API usage

## API Reference

### Main Classes

- `OpenTelemetryProducerInterceptor` - Producer interceptor for tracing
- `OpenTelemetryConsumerInterceptor` - Consumer interceptor for tracing
- `TracingContext` - Utility methods for span creation and context propagation
- `TracingProducerBuilder` - Helper for injecting trace context into messages

## Additional Resources

- [OpenTelemetry Java Documentation](https://opentelemetry.io/docs/instrumentation/java/)
- [W3C Trace Context Specification](https://www.w3.org/TR/trace-context/)
- [Pulsar Documentation](https://pulsar.apache.org/docs/)