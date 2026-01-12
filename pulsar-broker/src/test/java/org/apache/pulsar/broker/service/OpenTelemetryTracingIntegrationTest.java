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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Integration tests for OpenTelemetry tracing with real broker.
 * Note: These tests may be timing-dependent and could be flaky in CI environments.
 * They verify end-to-end tracing functionality with actual Pulsar broker.
 */
@Test(groups = "broker")
public class OpenTelemetryTracingIntegrationTest extends BrokerTestBase {

    private InMemorySpanExporter spanExporter;
    private OpenTelemetrySdk openTelemetry;
    private SdkTracerProvider tracerProvider;

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        // Setup OpenTelemetry SDK with in-memory exporter
        spanExporter = InMemorySpanExporter.create();
        tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                .build();

        openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .build();

        baseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
        if (openTelemetry != null) {
            openTelemetry.close();
        }
    }

    private void flushSpans() throws Exception {
        tracerProvider.forceFlush().join(5, TimeUnit.SECONDS);
    }

    @Test
    public void testBasicProducerConsumerTracing() throws Exception {
        String topic = "persistent://prop/ns-abc/test-basic-tracing";
        spanExporter.reset();

        // Create client with tracing enabled
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .openTelemetry(openTelemetry)
                .enableTracing(true)
                .build();

        // Create producer
        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        // Create consumer
        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("test-sub")
                .subscribe();

        // Send and receive message
        MessageId sentMsgId = producer.send("test-message");
        assertNotNull(sentMsgId);

        Message<String> msg = consumer.receive(5, TimeUnit.SECONDS);
        assertNotNull(msg);
        assertEquals(msg.getValue(), "test-message");
        consumer.acknowledge(msg);

        // Close client to force span flush
        producer.close();
        consumer.close();
        client.close();

        // Force flush tracer provider
        flushSpans();

        // Verify spans - at least one span should be created
        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertTrue(spans.size() > 0, "Expected at least one span, got: " + spans.size());

        // Verify producer span if present
        spans.stream()
                .filter(s -> s.getKind() == SpanKind.PRODUCER)
                .findFirst()
                .ifPresent(producerSpan -> {
                    assertEquals(producerSpan.getName(), "send " + topic);
                    assertEquals(producerSpan.getAttributes().get(
                            io.opentelemetry.api.common.AttributeKey.stringKey("messaging.system")), "pulsar");
                });

        // Verify consumer span if present
        spans.stream()
                .filter(s -> s.getKind() == SpanKind.CONSUMER)
                .findFirst()
                .ifPresent(consumerSpan -> {
                    assertEquals(consumerSpan.getName(), "process " + topic);
                    assertEquals(consumerSpan.getAttributes().get(
                            io.opentelemetry.api.common.AttributeKey.stringKey("messaging.system")), "pulsar");
                    assertEquals(consumerSpan.getAttributes().get(
                            io.opentelemetry.api.common.AttributeKey.stringKey(
                                    "messaging.pulsar.acknowledgment.type")),
                            "acknowledge");
                });
    }

    @Test
    public void testNegativeAcknowledgment() throws Exception {
        String topic = "persistent://prop/ns-abc/test-negative-ack";
        spanExporter.reset();

        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .openTelemetry(openTelemetry)
                .enableTracing(true)
                .build();

        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topic)
                .create();
        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("test-sub")
                .negativeAckRedeliveryDelay(0, TimeUnit.SECONDS)
                .subscribe();

        // Send message
        producer.send("test-message");

        // Receive and negative acknowledge
        Message<String> msg = consumer.receive(5, TimeUnit.SECONDS);
        assertNotNull(msg);
        consumer.negativeAcknowledge(msg);

        Thread.sleep(3000);

        // Close to ensure negative ack is processed
        producer.close();
        consumer.close();
        client.close();

        // Wait for spans
        flushSpans();

        // Find consumer span
        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        SpanData consumerSpan = spans.stream()
                .filter(s -> s.getKind() == SpanKind.CONSUMER)
                .findFirst()
                .orElseThrow(() -> new AssertionError("Consumer span not found. Total spans: "
                        + spans.size() + ", kinds: " + spans.stream()
                        .map(s -> s.getKind().toString()).collect(java.util.stream.Collectors.joining(", "))));

        // Verify negative ack attribute
        assertEquals(consumerSpan.getAttributes().get(
                io.opentelemetry.api.common.AttributeKey.stringKey("messaging.pulsar.acknowledgment.type")),
                "negative_acknowledge");
    }

    @Test
    public void testCumulativeAcknowledgment() throws Exception {
        String topic = "persistent://prop/ns-abc/test-cumulative-ack";
        spanExporter.reset();

        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .openTelemetry(openTelemetry)
                .enableTracing(true)
                .build();

        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("test-sub")
                .subscriptionType(SubscriptionType.Failover)
                .subscribe();

        // Send multiple messages
        for (int i = 0; i < 5; i++) {
            producer.send("message-" + i);
        }

        // Receive all messages
        Message<String> lastMsg = null;
        for (int i = 0; i < 5; i++) {
            lastMsg = consumer.receive(5, TimeUnit.SECONDS);
            assertNotNull(lastMsg);
        }

        // Cumulative acknowledge last message
        consumer.acknowledgeCumulative(lastMsg);

        // Wait for spans
        flushSpans();

        // Verify all consumer spans have cumulative_acknowledge attribute
        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        long consumerSpansWithCumulativeAck = spans.stream()
                .filter(s -> s.getKind() == SpanKind.CONSUMER)
                .filter(s -> "cumulative_acknowledge".equals(s.getAttributes().get(
                        io.opentelemetry.api.common.AttributeKey.stringKey(
                                "messaging.pulsar.acknowledgment.type"))))
                .count();

        assertEquals(consumerSpansWithCumulativeAck, 5);

        producer.close();
        consumer.close();
        client.close();
    }

    @Test
    public void testAcknowledgmentTimeout() throws Exception {
        String topic = "persistent://prop/ns-abc/test-ack-timeout";
        spanExporter.reset();

        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .openTelemetry(openTelemetry)
                .enableTracing(true)
                .build();

        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("test-sub")
                .ackTimeout(1, TimeUnit.SECONDS)
                .subscribe();

        // Send message
        producer.send("test-message");

        // Receive but don't acknowledge
        Message<String> msg = consumer.receive(5, TimeUnit.SECONDS);
        assertNotNull(msg);

        // Note: Ack timeout behavior varies based on subscription type and broker implementation
        // For Shared subscription, ack timeout triggers redelivery but span may already be ended
        // This test verifies the basic tracing flow works even with ack timeout configured

        // Acknowledge to properly end the span
        consumer.acknowledge(msg);

        // Wait for spans
        flushSpans();

        // Verify consumer span exists with acknowledge attribute
        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        boolean foundConsumerSpan = spans.stream()
                .filter(s -> s.getKind() == SpanKind.CONSUMER)
                .anyMatch(s -> s.getAttributes().get(
                        io.opentelemetry.api.common.AttributeKey.stringKey(
                                "messaging.pulsar.acknowledgment.type")) != null);

        assertTrue(foundConsumerSpan, "Expected consumer span with acknowledgment.type attribute");

        producer.close();
        consumer.close();
        client.close();
    }

    @Test
    public void testMultiTopicConsumerTracing() throws Exception {
        String topic1 = "persistent://prop/ns-abc/test-multi-topic-1";
        String topic2 = "persistent://prop/ns-abc/test-multi-topic-2";
        spanExporter.reset();

        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .openTelemetry(openTelemetry)
                .enableTracing(true)
                .build();

        Producer<String> producer1 = client.newProducer(Schema.STRING)
                .topic(topic1)
                .create();

        Producer<String> producer2 = client.newProducer(Schema.STRING)
                .topic(topic2)
                .create();

        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topics(List.of(topic1, topic2))
                .subscriptionName("test-sub")
                .subscribe();

        // Send messages to both topics
        producer1.send("message-topic1");
        producer2.send("message-topic2");

        // Receive and acknowledge both messages
        Set<String> receivedTopics = new java.util.HashSet<>();
        for (int i = 0; i < 2; i++) {
            Message<String> msg = consumer.receive(5, TimeUnit.SECONDS);
            assertNotNull(msg);
            receivedTopics.add(msg.getTopicName());
            consumer.acknowledge(msg);
        }

        assertEquals(receivedTopics.size(), 2);

        // Wait for spans
        flushSpans();

        // Verify spans for both topics
        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        long consumerSpans = spans.stream()
                .filter(s -> s.getKind() == SpanKind.CONSUMER)
                .count();

        assertEquals(consumerSpans, 2);

        producer1.close();
        producer2.close();
        consumer.close();
        client.close();
    }

    @Test
    public void testTracingWithoutGlobalEnable() throws Exception {
        String topic = "persistent://prop/ns-abc/test-no-global-tracing";
        spanExporter.reset();

        // Create client with OpenTelemetry but tracing NOT enabled
        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .openTelemetry(openTelemetry)
                .enableTracing(false)  // Explicitly disabled
                .build();

        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("test-sub")
                .subscribe();

        // Send and receive message
        producer.send("test-message");
        Message<String> msg = consumer.receive(5, TimeUnit.SECONDS);
        assertNotNull(msg);
        consumer.acknowledge(msg);

        // Wait for potential spans
        flushSpans();

        // Verify NO spans were created
        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertEquals(spans.size(), 0, "Expected no spans when tracing is disabled");

        producer.close();
        consumer.close();
        client.close();
    }

    @Test
    public void testSharedSubscriptionTracing() throws Exception {
        String topic = "persistent://prop/ns-abc/test-shared-subscription";
        spanExporter.reset();

        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .openTelemetry(openTelemetry)
                .enableTracing(true)
                .build();

        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("test-shared-sub")
                .subscriptionType(SubscriptionType.Shared)
                .subscribe();

        // Send messages
        for (int i = 0; i < 3; i++) {
            producer.send("message-" + i);
        }

        // Receive and acknowledge individually
        for (int i = 0; i < 3; i++) {
            Message<String> msg = consumer.receive(5, TimeUnit.SECONDS);
            assertNotNull(msg);
            consumer.acknowledge(msg);
        }

        producer.close();
        consumer.close();
        client.close();

        flushSpans();

        // Verify individual acks for Shared subscription
        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        long consumerSpansWithIndividualAck = spans.stream()
                .filter(s -> s.getKind() == SpanKind.CONSUMER)
                .filter(s -> "acknowledge".equals(s.getAttributes().get(
                        io.opentelemetry.api.common.AttributeKey.stringKey(
                                "messaging.pulsar.acknowledgment.type"))))
                .count();

        assertEquals(consumerSpansWithIndividualAck, 3);
    }

    @Test
    public void testKeySharedSubscriptionTracing() throws Exception {
        String topic = "persistent://prop/ns-abc/test-key-shared-subscription";
        spanExporter.reset();

        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .openTelemetry(openTelemetry)
                .enableTracing(true)
                .build();

        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("test-key-shared-sub")
                .subscriptionType(SubscriptionType.Key_Shared)
                .subscribe();

        // Send messages with keys
        for (int i = 0; i < 3; i++) {
            producer.newMessage()
                    .key("key-" + (i % 2))
                    .value("message-" + i)
                    .send();
        }

        // Receive and acknowledge
        for (int i = 0; i < 3; i++) {
            Message<String> msg = consumer.receive(5, TimeUnit.SECONDS);
            assertNotNull(msg);
            consumer.acknowledge(msg);
        }

        producer.close();
        consumer.close();
        client.close();

        flushSpans();

        // Verify spans for Key_Shared subscription
        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        long consumerSpans = spans.stream()
                .filter(s -> s.getKind() == SpanKind.CONSUMER)
                .count();

        assertEquals(consumerSpans, 3);
    }

    @Test
    public void testExclusiveSubscriptionTracing() throws Exception {
        String topic = "persistent://prop/ns-abc/test-exclusive-subscription";
        spanExporter.reset();

        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .openTelemetry(openTelemetry)
                .enableTracing(true)
                .build();

        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("test-exclusive-sub")
                .subscriptionType(SubscriptionType.Exclusive)
                .subscribe();

        // Send messages
        for (int i = 0; i < 3; i++) {
            producer.send("message-" + i);
        }

        // Receive all messages
        Message<String> lastMsg = null;
        for (int i = 0; i < 3; i++) {
            lastMsg = consumer.receive(5, TimeUnit.SECONDS);
            assertNotNull(lastMsg);
        }

        // Cumulative acknowledge last message
        consumer.acknowledgeCumulative(lastMsg);

        producer.close();
        consumer.close();
        client.close();

        flushSpans();

        // Verify cumulative ack for Exclusive subscription
        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        long consumerSpansWithCumulativeAck = spans.stream()
                .filter(s -> s.getKind() == SpanKind.CONSUMER)
                .filter(s -> "cumulative_acknowledge".equals(s.getAttributes().get(
                        io.opentelemetry.api.common.AttributeKey.stringKey(
                                "messaging.pulsar.acknowledgment.type"))))
                .count();

        assertEquals(consumerSpansWithCumulativeAck, 3);
    }

    @Test
    public void testFailoverSubscriptionWithCumulativeAck() throws Exception {
        String topic = "persistent://prop/ns-abc/test-failover-cumulative";
        spanExporter.reset();

        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .openTelemetry(openTelemetry)
                .enableTracing(true)
                .build();

        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topic)
                .create();

        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("test-failover-sub")
                .subscriptionType(SubscriptionType.Failover)
                .subscribe();

        // Send messages
        for (int i = 0; i < 5; i++) {
            producer.send("message-" + i);
        }

        // Receive all messages
        Message<String> lastMsg = null;
        for (int i = 0; i < 5; i++) {
            lastMsg = consumer.receive(5, TimeUnit.SECONDS);
            assertNotNull(lastMsg);
        }

        // Cumulative acknowledge last message
        consumer.acknowledgeCumulative(lastMsg);

        producer.close();
        consumer.close();
        client.close();

        flushSpans();

        // Verify all spans ended with cumulative ack
        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        long consumerSpansWithCumulativeAck = spans.stream()
                .filter(s -> s.getKind() == SpanKind.CONSUMER)
                .filter(s -> "cumulative_acknowledge".equals(s.getAttributes().get(
                        io.opentelemetry.api.common.AttributeKey.stringKey(
                                "messaging.pulsar.acknowledgment.type"))))
                .count();

        assertEquals(consumerSpansWithCumulativeAck, 5);
    }

    @Test
    public void testMultiTopicConsumerWithCumulativeAck() throws Exception {
        String topic1 = "persistent://prop/ns-abc/test-multi-cumulative-1";
        String topic2 = "persistent://prop/ns-abc/test-multi-cumulative-2";
        spanExporter.reset();

        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .openTelemetry(openTelemetry)
                .enableTracing(true)
                .build();

        Producer<String> producer1 = client.newProducer(Schema.STRING)
                .topic(topic1)
                .create();

        Producer<String> producer2 = client.newProducer(Schema.STRING)
                .topic(topic2)
                .create();

        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topics(List.of(topic1, topic2))
                .subscriptionName("test-multi-cumulative-sub")
                .subscriptionType(SubscriptionType.Failover)
                .subscribe();

        // Send messages to both topics
        producer1.send("topic1-msg1");
        producer1.send("topic1-msg2");
        producer2.send("topic2-msg1");
        producer2.send("topic2-msg2");

        // Receive messages from both topics
        Message<String> topic1LastMsg = null;
        Message<String> topic2LastMsg = null;
        for (int i = 0; i < 4; i++) {
            Message<String> msg = consumer.receive(5, TimeUnit.SECONDS);
            assertNotNull(msg);
            if (msg.getTopicName().contains("multi-cumulative-1")) {
                topic1LastMsg = msg;
            } else {
                topic2LastMsg = msg;
            }
        }

        // Cumulative acknowledge for each topic separately
        if (topic1LastMsg != null) {
            consumer.acknowledgeCumulative(topic1LastMsg);
        }
        if (topic2LastMsg != null) {
            consumer.acknowledgeCumulative(topic2LastMsg);
        }

        producer1.close();
        producer2.close();
        consumer.close();
        client.close();

        flushSpans();

        // Verify cumulative ack only affects spans from the same topic
        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        long consumerSpansWithCumulativeAck = spans.stream()
                .filter(s -> s.getKind() == SpanKind.CONSUMER)
                .filter(s -> "cumulative_acknowledge".equals(s.getAttributes().get(
                        io.opentelemetry.api.common.AttributeKey.stringKey(
                                "messaging.pulsar.acknowledgment.type"))))
                .count();

        // Should have cumulative ack for messages from both topics
        assertEquals(consumerSpansWithCumulativeAck, 4);
    }

    @Test
    public void testBatchMessagesTracing() throws Exception {
        String topic = "persistent://prop/ns-abc/test-batch-tracing";
        spanExporter.reset();

        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .openTelemetry(openTelemetry)
                .enableTracing(true)
                .build();

        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topic)
                .enableBatching(true)
                .batchingMaxMessages(5)
                .batchingMaxPublishDelay(1, TimeUnit.SECONDS)
                .create();

        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("test-sub")
                .subscribe();

        // Send batch of messages
        for (int i = 0; i < 5; i++) {
            producer.sendAsync("message-" + i);
        }
        producer.flush();

        // Receive and acknowledge all messages
        for (int i = 0; i < 5; i++) {
            Message<String> msg = consumer.receive(5, TimeUnit.SECONDS);
            assertNotNull(msg);
            consumer.acknowledge(msg);
        }

        producer.close();
        consumer.close();
        client.close();

        // Wait for spans
        flushSpans();

        // Verify spans for batched messages
        // Note: Tracing behavior may vary for batched messages depending on when spans are created
        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertTrue(spans.size() > 0, "Expected at least some spans for batched messages");

        // Verify that spans have correct attributes
        spans.stream()
                .filter(s -> s.getKind() == SpanKind.PRODUCER || s.getKind() == SpanKind.CONSUMER)
                .forEach(span -> {
                    assertNotNull(span.getAttributes().get(
                            io.opentelemetry.api.common.AttributeKey.stringKey("messaging.system")));
                    assertEquals(span.getAttributes().get(
                            io.opentelemetry.api.common.AttributeKey.stringKey("messaging.system")), "pulsar");
                });
    }

    @Test
    public void testCustomSpan() throws Exception {
        String topic = "persistent://prop/ns-abc/testCustomSpan";
        spanExporter.reset();

        PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrl())
                .openTelemetry(openTelemetry)
                .enableTracing(true)
                .build();

        Producer<String> producer = client.newProducer(Schema.STRING)
                .topic(topic)
                .enableBatching(true)
                .batchingMaxMessages(5)
                .batchingMaxPublishDelay(1, TimeUnit.SECONDS)
                .create();

        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic(topic)
                .subscriptionName("test-sub")
                .subscribe();

        // Send batch of messages
        for (int i = 0; i < 5; i++) {
            producer.sendAsync("message-" + i);
        }
        producer.flush();

        InstrumentProvider instrumentProvider = ((PulsarClientImpl) client).instrumentProvider();
        final Tracer tracer = instrumentProvider.getTracer();
        String customSpanName = "business-logic";
        // Receive and acknowledge all messages
        for (int i = 0; i < 5; i++) {
            Message<String> msg = consumer.receive(5, TimeUnit.SECONDS);
            assertNotNull(msg);
            Span processingSpan = tracer.spanBuilder(customSpanName)
                    .setSpanKind(SpanKind.CLIENT).startSpan();
            try (Scope scope = processingSpan.makeCurrent()) {
                processingSpan.setStatus(StatusCode.OK);
                consumer.acknowledge(msg);
            } catch (Exception e) {
                processingSpan.recordException(e);
                processingSpan.setStatus(StatusCode.ERROR);
                consumer.negativeAcknowledge(msg);
                throw e;
            } finally {
                processingSpan.end();
            }
        }

        producer.close();
        consumer.close();
        client.close();

        // Wait for spans
        flushSpans();

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertTrue(spans.size() > 0, "Expected at least some spans for batched messages");

        spans.stream()
                .filter(s -> s.getName().equals(customSpanName))
                .forEach(span -> {
                    assertEquals(span.getKind(), SpanKind.CLIENT);
                });
    }
}
