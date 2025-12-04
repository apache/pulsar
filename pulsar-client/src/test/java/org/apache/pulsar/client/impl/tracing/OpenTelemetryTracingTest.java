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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.impl.MessageImpl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests for OpenTelemetry tracing integration.
 */
public class OpenTelemetryTracingTest {

    private InMemorySpanExporter spanExporter;
    private OpenTelemetrySdk openTelemetry;
    private Tracer tracer;
    private TextMapPropagator propagator;

    @BeforeClass
    public void setup() {
        spanExporter = InMemorySpanExporter.create();
        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                .build();

        openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .build();

        tracer = openTelemetry.getTracer("test-tracer");
        propagator = openTelemetry.getPropagators().getTextMapPropagator();
    }

    @AfterClass
    public void tearDown() {
        if (openTelemetry != null) {
            openTelemetry.close();
        }
    }

    @Test
    public void testCreateProducerSpan() {
        spanExporter.reset();

        String topic = "test-topic";
        Span span = TracingContext.createProducerSpan(tracer, topic, null);

        assertNotNull(span);
        assertTrue(span.isRecording());
        assertTrue(TracingContext.isValid(span));

        TracingContext.endSpan(span);

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(spans.size(), 1);

        SpanData spanData = spans.get(0);
        assertEquals(spanData.getName(), "send " + topic);
        assertEquals(spanData.getKind(), SpanKind.PRODUCER);
        assertEquals(spanData.getAttributes().get(
                io.opentelemetry.api.common.AttributeKey.stringKey("messaging.system")), "pulsar");
        assertEquals(spanData.getAttributes().get(
                io.opentelemetry.api.common.AttributeKey.stringKey("messaging.destination.name")), topic);
        assertEquals(spanData.getAttributes().get(
                io.opentelemetry.api.common.AttributeKey.stringKey("messaging.operation.name")), "send");
    }

    @Test
    public void testCreateConsumerSpan() {
        spanExporter.reset();

        String topic = "test-topic";
        String subscription = "test-sub";
        Map<String, String> properties = new HashMap<>();
        properties.put("test-key", "test-value");

        Message<?> message = createTestMessage(properties);

        Span span = TracingContext.createConsumerSpan(tracer, topic, subscription, message, propagator);

        assertNotNull(span);
        assertTrue(span.isRecording());
        assertTrue(TracingContext.isValid(span));

        TracingContext.endSpan(span);

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(spans.size(), 1);

        SpanData spanData = spans.get(0);
        assertEquals(spanData.getName(), "process " + topic);
        assertEquals(spanData.getKind(), SpanKind.CONSUMER);
        assertEquals(spanData.getAttributes().get(
                io.opentelemetry.api.common.AttributeKey.stringKey("messaging.system")), "pulsar");
        assertEquals(spanData.getAttributes().get(
                io.opentelemetry.api.common.AttributeKey.stringKey("messaging.destination.name")), topic);
        assertEquals(spanData.getAttributes().get(
                io.opentelemetry.api.common.AttributeKey.stringKey("messaging.destination.subscription.name")),
                subscription);
        assertEquals(spanData.getAttributes().get(
                io.opentelemetry.api.common.AttributeKey.stringKey("messaging.operation.name")), "process");
    }

    @Test
    public void testSpanWithException() {
        spanExporter.reset();

        String topic = "test-topic";
        Span span = TracingContext.createProducerSpan(tracer, topic, null);

        RuntimeException exception = new RuntimeException("Test exception");
        TracingContext.endSpan(span, exception);

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(spans.size(), 1);

        SpanData spanData = spans.get(0);
        assertEquals(spanData.getStatus().getStatusCode(), io.opentelemetry.api.trace.StatusCode.ERROR);
        assertFalse(spanData.getEvents().isEmpty());
    }

    @Test
    public void testContextPropagation() {
        spanExporter.reset();

        // Create a parent span
        Span parentSpan = tracer.spanBuilder("parent").startSpan();
        try (Scope scope = parentSpan.makeCurrent()) {
            // Create a producer span as child
            String topic = "test-topic";
            Span producerSpan = TracingContext.createProducerSpan(tracer, topic, Context.current());

            assertNotNull(producerSpan);
            assertTrue(TracingContext.isValid(producerSpan));

            TracingContext.endSpan(producerSpan);
        } finally {
            parentSpan.end();
        }

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(spans.size(), 2);

        // Verify parent-child relationship
        SpanData producerSpan = spans.get(0);
        SpanData parentSpanData = spans.get(1);

        assertEquals(producerSpan.getParentSpanId(), parentSpanData.getSpanId());
    }


    private Message<?> createTestMessage(Map<String, String> properties) {
        // Create a simple MessageMetadata with properties
        org.apache.pulsar.common.api.proto.MessageMetadata metadata =
                new org.apache.pulsar.common.api.proto.MessageMetadata();

        for (Map.Entry<String, String> entry : properties.entrySet()) {
            metadata.addProperty()
                    .setKey(entry.getKey())
                    .setValue(entry.getValue());
        }

        // Create a message with metadata
        MessageImpl<?> message = MessageImpl.create(
                metadata,
                java.nio.ByteBuffer.wrap("test".getBytes()),
                org.apache.pulsar.client.api.Schema.BYTES,
                "test-topic"
        );

        // Set a MessageId on the message
        message.setMessageId(new org.apache.pulsar.client.impl.MessageIdImpl(1L, 1L, -1));

        return message;
    }
}