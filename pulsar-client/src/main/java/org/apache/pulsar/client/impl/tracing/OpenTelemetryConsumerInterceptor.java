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

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.propagation.TextMapPropagator;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerInterceptor;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TopicMessageId;
import org.apache.pulsar.client.api.TraceableMessageId;
import org.apache.pulsar.client.impl.ConsumerBase;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OpenTelemetry consumer interceptor that creates spans for message consumption.
 * <p>
 * This interceptor automatically retrieves the Tracer from the client's InstrumentProvider,
 * ensuring consistent OpenTelemetry configuration across the client.
 * <p>
 * <b>Span Storage Strategy:</b>
 * <ul>
 *   <li><b>Shared/Key_Shared subscriptions:</b> Spans are attached directly to {@link TraceableMessageId}
 *       instances with zero map overhead.</li>
 *   <li><b>Failover/Exclusive subscriptions:</b> A nested map is initialized eagerly to track message IDs
 *       and their spans in sorted order. This is necessary because cumulative ack must end spans
 *       for all messages up to the acked position.</li>
 * </ul>
 * <p>
 * <b>Multi-Topic Consumer Support:</b><br>
 * For {@link org.apache.pulsar.client.api.MultiTopicsConsumer} and pattern-based consumers, cumulative
 * acknowledgment only affects messages from the same topic partition. The interceptor uses a nested
 * map structure (topic partition → message IDs) and {@link TopicMessageId#getOwnerTopic()} to ensure
 * spans are only ended for messages from the acknowledged topic partition.
 */
public class OpenTelemetryConsumerInterceptor<T> implements ConsumerInterceptor<T> {

    private static final Logger log = LoggerFactory.getLogger(OpenTelemetryConsumerInterceptor.class);

    private Tracer tracer;
    private TextMapPropagator propagator;
    private String topic;
    private boolean initialized = false;

    /**
     * Used for cumulative acknowledgment support (Failover/Exclusive subscriptions).
     * Outer map: topic partition -> (message ID -> span)
     * Inner ConcurrentSkipListMap maintains sorted order for efficient range operations.
     * Initialized eagerly for Failover/Exclusive subscriptions.
     * <p>
     * The nested structure is necessary for multi-topic consumers where a single interceptor
     * instance handles messages from multiple topic partitions. Cumulative ack only affects
     * messages from the same topic partition.
     */
    private volatile Map<String, ConcurrentSkipListMap<MessageIdAdv, Span>> messageSpansByTopic;

    public OpenTelemetryConsumerInterceptor() {
        // Tracer and propagator will be initialized in beforeConsume when we have access to the consumer
    }

    /**
     * Get the topic key for a message ID.
     * For TopicMessageId, returns the owner topic. Otherwise returns the consumer's topic.
     */
    private String getTopicKey(MessageId messageId) {
        if (messageId instanceof TopicMessageId) {
            return ((TopicMessageId) messageId).getOwnerTopic();
        }
        return topic != null ? topic : "";
    }

    /**
     * Initialize the tracer from the consumer's client.
     * This is called lazily on the first message.
     */
    private void initializeIfNeeded(Consumer<T> consumer) {
        if (!initialized && consumer instanceof ConsumerBase) {
            ConsumerBase<?> consumerBase = (ConsumerBase<?>) consumer;
            PulsarClientImpl client = consumerBase.getClient();
            InstrumentProvider instrumentProvider = client.instrumentProvider();

            this.tracer = instrumentProvider.getTracer();
            this.propagator = GlobalOpenTelemetry.getPropagators().getTextMapPropagator();
            this.initialized = true;
            if (consumerBase.getConf().getSubscriptionType() == SubscriptionType.Exclusive
                    || consumerBase.getConf().getSubscriptionType() == SubscriptionType.Failover) {
                ensureMapInitialized();
            }
        }
    }

    /**
     * Ensure the map is initialized for cumulative acknowledgment support.
     * This is called when we detect cumulative ack is being used.
     */
    private void ensureMapInitialized() {
        if (messageSpansByTopic == null) {
            messageSpansByTopic = new ConcurrentHashMap<>();
            log.debug("Initialized message spans map for cumulative acknowledgment support");
        }
    }

    @Override
    public void close() {
        // Clean up any remaining spans for Failover/Exclusive subscriptions
        if (messageSpansByTopic != null) {
            messageSpansByTopic.values().forEach(topicSpans ->
                topicSpans.values().forEach(TracingContext::endSpan)
            );
            messageSpansByTopic.clear();
        }
    }

    @Override
    public Message<T> beforeConsume(Consumer<T> consumer, Message<T> message) {
        // Initialize tracer from consumer on first call
        initializeIfNeeded(consumer);

        if (tracer == null || propagator == null) {
            return message;
        }

        try {
            if (topic == null) {
                topic = consumer.getTopic();
            }

            // Create a consumer span for this message
            Span span = TracingContext.createConsumerSpan(tracer, topic, message, propagator);

            if (TracingContext.isValid(span)) {
                MessageId messageId = message.getMessageId();

                // Store in map for cumulative ack support (Failover/Exclusive)
                if (messageSpansByTopic != null && messageId instanceof MessageIdAdv) {
                    String topicKey = getTopicKey(messageId);
                    messageSpansByTopic.computeIfAbsent(topicKey,
                            k -> new ConcurrentSkipListMap<>()).put((MessageIdAdv) messageId, span);
                }

                // Always attach span to message ID for individual ack/nack
                if (messageId instanceof TraceableMessageId) {
                    ((TraceableMessageId) messageId).setTracingSpan(span);
                }

                log.debug("Created consumer span for message {} on topic {}", messageId, topic);
            }
        } catch (Exception e) {
            log.error("Error creating consumer span", e);
        }

        return message;
    }

    @Override
    public void onAcknowledge(Consumer<T> consumer, MessageId messageId, Throwable exception) {
        if (!(messageId instanceof TraceableMessageId)) {
            return;
        }

        Span span = ((TraceableMessageId) messageId).getTracingSpan();
        if (span != null) {
            try {
                if (exception != null) {
                    TracingContext.endSpan(span, exception);
                } else {
                    // Add attribute to indicate acknowledgment type
                    span.setAttribute("messaging.pulsar.acknowledgment.type", "acknowledge");
                    TracingContext.endSpan(span);
                }
                // Clear the span from the message ID
                ((TraceableMessageId) messageId).setTracingSpan(null);

                // Remove from map if it exists (Failover/Exclusive)
                if (messageSpansByTopic != null && messageId instanceof MessageIdAdv) {
                    String topicKey = getTopicKey(messageId);
                    ConcurrentSkipListMap<MessageIdAdv, Span> topicSpans = messageSpansByTopic.get(topicKey);
                    if (topicSpans != null) {
                        topicSpans.remove((MessageIdAdv) messageId);
                    }
                }
            } catch (Exception e) {
                log.error("Error ending consumer span on acknowledge", e);
            }
        }
    }

    @Override
    public void onAcknowledgeCumulative(Consumer<T> consumer, MessageId messageId, Throwable exception) {
        if (!(messageId instanceof MessageIdAdv)) {
            // Fallback to simple ack for non-adv message IDs
            if (messageId instanceof TraceableMessageId) {
                Span span = ((TraceableMessageId) messageId).getTracingSpan();
                if (span != null) {
                    try {
                        if (exception != null) {
                            TracingContext.endSpan(span, exception);
                        } else {
                            // Add attribute to indicate acknowledgment type
                            span.setAttribute("messaging.pulsar.acknowledgment.type", "cumulative_acknowledge");
                            TracingContext.endSpan(span);
                        }
                        ((TraceableMessageId) messageId).setTracingSpan(null);
                    } catch (Exception e) {
                        log.error("Error ending consumer span on cumulative acknowledge", e);
                    }
                }
            }
            return;
        }

        MessageIdAdv cumulativeAckPos = (MessageIdAdv) messageId;
        String topicKey = getTopicKey(messageId);

        // Get the topic-specific map
        ConcurrentSkipListMap<MessageIdAdv, Span> topicSpans = messageSpansByTopic != null
                ? messageSpansByTopic.get(topicKey) : null;

        // First, try to get the span for the cumulative ack position itself
        Span currentSpan = null;
        if (messageId instanceof TraceableMessageId) {
            currentSpan = ((TraceableMessageId) messageId).getTracingSpan();
        }

        // End spans for all messages in the topic-specific map up to the cumulative ack position
        if (topicSpans != null) {
            Iterator<Map.Entry<MessageIdAdv, Span>> iterator = topicSpans.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<MessageIdAdv, Span> entry = iterator.next();
                MessageIdAdv msgId = entry.getKey();

                // End spans for all messages <= cumulative ack position
                if (msgId.compareTo(cumulativeAckPos) <= 0) {
                    Span span = entry.getValue();
                    try {
                        if (exception != null) {
                            TracingContext.endSpan(span, exception);
                        } else {
                            // Add attribute to indicate acknowledgment type
                            span.setAttribute("messaging.pulsar.acknowledgment.type", "cumulative_acknowledge");
                            TracingContext.endSpan(span);
                        }

                        // Clear the span from the message ID
                        if (msgId instanceof TraceableMessageId) {
                            ((TraceableMessageId) msgId).setTracingSpan(null);
                        }
                    } catch (Exception e) {
                        log.error("Error ending consumer span on cumulative acknowledge for message {}", msgId, e);
                    }
                    iterator.remove();
                } else {
                    // Since the map is sorted, we can break early
                    break;
                }
            }

            // Clean up empty topic map
            if (topicSpans.isEmpty()) {
                messageSpansByTopic.remove(topicKey);
            }
        }

        // If the cumulative ack position span wasn't in the map, end it directly
        if (currentSpan != null && messageId instanceof TraceableMessageId) {
            try {
                if (exception != null) {
                    TracingContext.endSpan(currentSpan, exception);
                } else {
                    TracingContext.endSpan(currentSpan);
                }
                ((TraceableMessageId) messageId).setTracingSpan(null);
            } catch (Exception e) {
                log.error("Error ending consumer span on cumulative acknowledge", e);
            }
        }
    }

    @Override
    public void onNegativeAcksSend(Consumer<T> consumer, Set<MessageId> messageIds) {
        for (MessageId messageId : messageIds) {
            if (!(messageId instanceof TraceableMessageId)) {
                continue;
            }

            Span span = ((TraceableMessageId) messageId).getTracingSpan();
            if (span != null) {
                try {
                    // Add attribute to indicate negative acknowledgment (not an error, but normal flow)
                    span.setAttribute("messaging.pulsar.acknowledgment.type", "negative_acknowledge");
                    // End span normally - negative ack is expected behavior, not an error
                    TracingContext.endSpan(span);
                    // Clear the span from the message ID
                    ((TraceableMessageId) messageId).setTracingSpan(null);

                    // Remove from map if it exists (Failover/Exclusive)
                    if (messageSpansByTopic != null && messageId instanceof MessageIdAdv) {
                        String topicKey = getTopicKey(messageId);
                        ConcurrentSkipListMap<MessageIdAdv, Span> topicSpans = messageSpansByTopic.get(topicKey);
                        if (topicSpans != null) {
                            topicSpans.remove((MessageIdAdv) messageId);
                        }
                    }
                } catch (Exception e) {
                    log.error("Error ending consumer span on negative acknowledge", e);
                }
            }
        }
    }

    @Override
    public void onAckTimeoutSend(Consumer<T> consumer, Set<MessageId> messageIds) {
        for (MessageId messageId : messageIds) {
            if (!(messageId instanceof TraceableMessageId)) {
                continue;
            }

            Span span = ((TraceableMessageId) messageId).getTracingSpan();
            if (span != null) {
                try {
                    // Add attribute to indicate ack timeout (not an error, but expected behavior)
                    span.setAttribute("messaging.pulsar.acknowledgment.type", "ack_timeout");
                    // End span normally - ack timeout is expected behavior, not an error
                    TracingContext.endSpan(span);
                    // Clear the span from the message ID
                    ((TraceableMessageId) messageId).setTracingSpan(null);

                    // Remove from map if it exists (Failover/Exclusive)
                    if (messageSpansByTopic != null && messageId instanceof MessageIdAdv) {
                        String topicKey = getTopicKey(messageId);
                        ConcurrentSkipListMap<MessageIdAdv, Span> topicSpans = messageSpansByTopic.get(topicKey);
                        if (topicSpans != null) {
                            topicSpans.remove((MessageIdAdv) messageId);
                        }
                    }
                } catch (Exception e) {
                    log.error("Error ending consumer span on ack timeout", e);
                }
            }
        }
    }
}