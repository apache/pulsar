/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.impl.tracing;

import org.testng.annotations.Test;

/**
 * Example test demonstrating OpenTelemetry tracing usage patterns.
 * These are code examples for documentation purposes.
 */
public class TracingExampleTest {

    /**
     * Example 1: Basic producer with tracing.
     */
    @Test(enabled = false)
    public void exampleBasicProducerTracing() throws Exception {
        // Configure OpenTelemetry (or use auto-instrumentation)
        // OpenTelemetry openTelemetry = ...;

        // Create Pulsar client
        // PulsarClient client = PulsarClient.builder()
        //     .serviceUrl("pulsar://localhost:6650")
        //     .build();

        // Create producer with tracing interceptor
        // Producer<String> producer = client.newProducer(Schema.STRING)
        //     .topic("my-topic")
        //     .intercept(new OpenTelemetryProducerInterceptor())
        //     .create();

        // Send message - trace context is automatically propagated
        // producer.newMessage()
        //     .value("Hello World")
        //     .send();
    }

    /**
     * Example 2: Producer with automatic tracing.
     */
    @Test(enabled = false)
    public void exampleProducerWithAutomaticTracing() throws Exception {
        // Create Pulsar client with tracing enabled
        // PulsarClient client = PulsarClient.builder()
        //     .serviceUrl("pulsar://localhost:6650")
        //     .openTelemetry(openTelemetry, true)  // Enable automatic tracing
        //     .build();

        // Producer automatically has tracing enabled
        // Producer<String> producer = client.newProducer(Schema.STRING)
        //     .topic("my-topic")
        //     .create();

        // Send message - trace context is automatically injected
        // producer.newMessage()
        //     .value("Hello World")
        //     .send();
    }

    /**
     * Example 3: Basic consumer with tracing.
     */
    @Test(enabled = false)
    public void exampleBasicConsumerTracing() throws Exception {
        // Create Pulsar client
        // PulsarClient client = PulsarClient.builder()
        //     .serviceUrl("pulsar://localhost:6650")
        //     .build();

        // Create consumer with tracing interceptor
        // Consumer<String> consumer = client.newConsumer(Schema.STRING)
        //     .topic("my-topic")
        //     .subscriptionName("my-subscription")
        //     .intercept(new OpenTelemetryConsumerInterceptor<>())
        //     .subscribe();

        // Receive and process messages - trace context is automatically extracted
        // while (true) {
        //     Message<String> msg = consumer.receive();
        //     try {
        //         // Process message
        //         System.out.println("Received: " + msg.getValue());
        //         consumer.acknowledge(msg);
        //     } catch (Exception e) {
        //         consumer.negativeAcknowledge(msg);
        //     }
        // }
    }

    /**
     * Example 4: End-to-end tracing from HTTP to Pulsar (automatic).
     */
    @Test(enabled = false)
    public void exampleEndToEndTracing() throws Exception {
        // ===== HTTP Service =====
        // When the HTTP framework has OpenTelemetry instrumentation,
        // the active span context automatically propagates to Pulsar

        // Producer - trace context automatically injected from HTTP span
        // producer.newMessage()
        //     .value("Message from HTTP request")
        //     .send();

        // ===== Consumer Service =====
        // In another service/application

        // Consumer with tracing
        // Consumer<String> consumer = client.newConsumer(Schema.STRING)
        //     .topic("my-topic")
        //     .subscriptionName("my-subscription")
        //     .intercept(new OpenTelemetryConsumerInterceptor<>())
        //     .subscribe();

        // Process messages - trace continues from HTTP request
        // Message<String> msg = consumer.receive();
        // // Trace context is automatically extracted from message properties
        // processMessage(msg.getValue());
        // consumer.acknowledge(msg);

        // The entire flow from HTTP request -> Producer -> Consumer is now traced!
    }

    /**
     * Example 5: Custom span creation in message processing.
     */
    @Test(enabled = false)
    public void exampleCustomSpanCreation() throws Exception {
        // When you need to create custom spans during message processing

        // Consumer with tracing
        // Consumer<String> consumer = client.newConsumer(Schema.STRING)
        //     .topic("my-topic")
        //     .subscriptionName("my-subscription")
        //     .intercept(new OpenTelemetryConsumerInterceptor<>())
        //     .subscribe();

        // Get tracer
        // Tracer tracer = GlobalOpenTelemetry.get().getTracer("my-application");

        // Process messages
        // Message<String> msg = consumer.receive();

        // The consumer interceptor already created a span, so we're in that context
        // Create a child span for custom processing
        // Span processingSpan = tracer.spanBuilder("process-message")
        //     .setSpanKind(SpanKind.INTERNAL)
        //     .startSpan();

        // try (Scope scope = processingSpan.makeCurrent()) {
        //     // Your processing logic here
        //     // Any spans created here will be children of processingSpan
        //     doSomeProcessing(msg.getValue());
        //     processingSpan.setStatus(StatusCode.OK);
        // } catch (Exception e) {
        //     processingSpan.recordException(e);
        //     processingSpan.setStatus(StatusCode.ERROR);
        //     throw e;
        // } finally {
        //     processingSpan.end();
        //     consumer.acknowledge(msg);
        // }
    }
}