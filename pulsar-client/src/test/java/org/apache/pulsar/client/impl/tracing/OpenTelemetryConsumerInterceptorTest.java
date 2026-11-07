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

import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.EventData;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.TopicMessageId;
import org.apache.pulsar.client.api.TraceableMessageId;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.ChunkMessageIdImpl;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class OpenTelemetryConsumerInterceptorTest {

    private static final String TOPIC = "persistent://public/default/tracing-ack";
    private static final String OTHER_TOPIC = "persistent://public/default/tracing-ack-other";

    private InMemorySpanExporter spanExporter;
    private OpenTelemetrySdk openTelemetry;
    private OpenTelemetryConsumerInterceptor<byte[]> interceptor;
    private Consumer<byte[]> consumer;

    @BeforeMethod
    @SuppressWarnings("unchecked")
    public void setup() {
        spanExporter = InMemorySpanExporter.create();
        openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(SdkTracerProvider.builder()
                        .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                        .build())
                .build();
        interceptor = new OpenTelemetryConsumerInterceptor<>(new InstrumentProvider(openTelemetry));
        consumer = mock(Consumer.class);
        when(consumer.getTopic()).thenReturn(TOPIC);
        when(consumer.getSubscription()).thenReturn("tracing-subscription");
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() {
        if (interceptor != null) {
            interceptor.close();
        }
        if (openTelemetry != null) {
            openTelemetry.close();
        }
    }

    @DataProvider
    public Object[][] acknowledgmentCases() {
        List<Object[]> cases = new ArrayList<>();
        for (boolean rebuilt : new boolean[]{false, true}) {
            for (boolean failed : new boolean[]{false, true}) {
                cases.add(new Object[]{new MessageIdImpl(7, 11, -1), rebuilt, failed});
                cases.add(new Object[]{new BatchMessageIdImpl(7, 11, -1, 2, 4, null), rebuilt, failed});
                cases.add(new Object[]{new ChunkMessageIdImpl(new MessageIdImpl(7, 9, -1),
                        new MessageIdImpl(7, 11, -1)), rebuilt, failed});
            }
        }
        return cases.toArray(Object[][]::new);
    }

    @Test(dataProvider = "acknowledgmentCases")
    public void testIndividualAcknowledge(MessageIdAdv originalId, boolean rebuilt, boolean failed) throws Exception {
        Span span = consume(originalId);
        MessageIdAdv unrelatedId = originalId.getBatchIndex() >= 0
                ? new BatchMessageIdImpl(originalId.getLedgerId(), originalId.getEntryId(),
                        originalId.getPartitionIndex(), originalId.getBatchIndex() + 1, originalId.getBatchSize(), null)
                : new MessageIdImpl(7, 12, -1);
        Span unrelatedSpan = consume(unrelatedId);
        assertThat(interceptor.getPendingSpanCount()).as("tracked spans before ACK").isEqualTo(2);
        assertThat(spanExporter.getFinishedSpanItems()).as("spans must remain open until ACK").isEmpty();

        MessageId acknowledgedId = rebuilt ? MessageId.fromByteArray(originalId.toByteArray()) : originalId;
        if (rebuilt) {
            assertThat(acknowledgedId).as("deserialized ID").isEqualTo(originalId).isNotSameAs(originalId);
            assertThat(((TraceableMessageId) acknowledgedId).getTracingSpan())
                    .as("deserialized ID has no attached span").isNull();
        }
        Throwable exception = failed ? new IllegalStateException("ack failed") : null;
        interceptor.onAcknowledge(consumer, acknowledgedId, exception);

        assertThat(interceptor.getPendingSpanCount()).as("ACK removes only the matching record").isEqualTo(1);
        assertThat(span.isRecording()).as("acknowledged span ends before consumer close").isFalse();
        assertThat(unrelatedSpan.isRecording()).as("another message remains unacknowledged").isTrue();
        assertThat(((TraceableMessageId) acknowledgedId).getTracingSpan())
                .as("ACK clears any span attached to its ID").isNull();
        assertThat(spanExporter.getFinishedSpanItems()).as("one span completed on ACK").hasSize(1);
        SpanData finishedSpan = spanExporter.getFinishedSpanItems().get(0);
        assertThat(finishedSpan.getSpanId()).as("completed span belongs to the acknowledged message")
                .isEqualTo(span.getSpanContext().getSpanId());
        if (failed) {
            assertThat(finishedSpan.getStatus().getStatusCode()).as("failed ACK status").isEqualTo(StatusCode.ERROR);
            assertThat(finishedSpan.getEvents()).extracting(EventData::getName).containsExactly("exception");
            assertThat(finishedSpan.getEvents().get(0).getAttributes().get(stringKey("exception.message")))
                    .as("failed ACK exception").isEqualTo("ack failed");
        } else {
            assertThat(finishedSpan.getStatus().getStatusCode()).as("successful ACK status").isEqualTo(StatusCode.OK);
            assertThat(finishedSpan.getAttributes().get(stringKey("messaging.pulsar.acknowledgment.type")))
                    .as("individual ACK attribute").isEqualTo("acknowledge");
        }

        interceptor.onAcknowledge(consumer, acknowledgedId, exception);
        assertThat(interceptor.getPendingSpanCount()).as("duplicate ACK leaves unrelated records").isEqualTo(1);
        assertThat(spanExporter.getFinishedSpanItems()).as("duplicate ACK completes no additional span").hasSize(1);
        assertThat(unrelatedSpan.isRecording()).as("duplicate ACK leaves unrelated spans open").isTrue();

        interceptor.onAcknowledge(consumer, MessageId.fromByteArray(unrelatedId.toByteArray()), null);
        assertThat(interceptor.getPendingSpanCount()).as("all ACKed records are removed before close").isZero();
        assertThat(unrelatedSpan.isRecording()).as("remaining message ends only after its own ACK").isFalse();
        assertThat(spanExporter.getFinishedSpanItems()).as("both messages completed").hasSize(2);
    }

    @Test
    public void testRebuiltTopicMessageIdAcknowledgeIsolatesTopics() throws Exception {
        MessageId originalId = TopicMessageId.create(TOPIC, new MessageIdImpl(7, 11, 0));
        MessageId otherId = TopicMessageId.create(OTHER_TOPIC, new MessageIdImpl(7, 11, 0));
        Span span = consume(originalId);
        Span otherSpan = consume(otherId);
        assertThat(interceptor.getPendingSpanCount()).as("equal positions in different topics are tracked")
                .isEqualTo(2);

        MessageId acknowledgedId = TopicMessageId.create(TOPIC, MessageId.fromByteArray(originalId.toByteArray()));
        assertThat(((TraceableMessageId) acknowledgedId).getTracingSpan())
                .as("rebuilt topic message ID has no attached span").isNull();
        interceptor.onAcknowledge(consumer, acknowledgedId, null);

        assertThat(interceptor.getPendingSpanCount()).as("only the acknowledged topic record is removed").isEqualTo(1);
        assertThat(span.isRecording()).as("acknowledged topic span ends").isFalse();
        assertThat(otherSpan.isRecording()).as("same position in another topic remains open").isTrue();
        assertThat(spanExporter.getFinishedSpanItems()).hasSize(1);
        assertThat(spanExporter.getFinishedSpanItems().get(0).getSpanId())
                .as("completed span belongs to the acknowledged topic").isEqualTo(span.getSpanContext().getSpanId());

        interceptor.onAcknowledge(consumer, acknowledgedId, null);
        assertThat(interceptor.getPendingSpanCount()).as("duplicate ACK preserves the other topic").isEqualTo(1);
        assertThat(otherSpan.isRecording()).isTrue();
    }

    @SuppressWarnings("unchecked")
    private Span consume(MessageId messageId) {
        Message<byte[]> message = mock(Message.class);
        when(message.getMessageId()).thenReturn(messageId);
        when(message.getTopicName()).thenReturn(messageId instanceof TopicMessageId topicMessageId
                ? topicMessageId.getOwnerTopic() : TOPIC);
        when(message.getProperties()).thenReturn(Map.of());
        assertThat(interceptor.beforeConsume(consumer, message)).isSameAs(message);
        Span span = ((TraceableMessageId) messageId).getTracingSpan();
        assertThat(span).as("beforeConsume attaches a span").isNotNull();
        assertThat(span.getSpanContext().isValid()).as("consumer span has a valid context").isTrue();
        assertThat(span.isRecording()).as("consumer span records until ACK").isTrue();
        return span;
    }
}
