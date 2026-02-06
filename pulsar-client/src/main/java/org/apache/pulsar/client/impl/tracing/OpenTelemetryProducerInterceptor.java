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
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapPropagator;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TraceableMessage;
import org.apache.pulsar.client.api.interceptor.ProducerInterceptor;
import org.apache.pulsar.client.impl.ProducerBase;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OpenTelemetry producer interceptor that creates spans for message publishing.
 * <p>
 * This interceptor automatically retrieves the Tracer from the client's InstrumentProvider,
 * ensuring consistent OpenTelemetry configuration across the client.
 * <p>
 * Spans are attached directly to {@link TraceableMessage} instances, eliminating the need
 * for external span tracking via maps.
 */
public class OpenTelemetryProducerInterceptor implements ProducerInterceptor {

    private static final Logger log = LoggerFactory.getLogger(OpenTelemetryProducerInterceptor.class);

    private Tracer tracer;
    private TextMapPropagator propagator;
    private String topic;
    private boolean initialized = false;

    public OpenTelemetryProducerInterceptor() {
        // Tracer and propagator will be initialized in beforeSend when we have access to the producer
    }

    /**
     * Initialize the tracer from the producer's client.
     * This is called lazily on the first message.
     */
    private void initializeIfNeeded(Producer producer) {
        if (!initialized && producer instanceof ProducerBase<?> producerBase) {
            PulsarClientImpl client = producerBase.getClient();
            InstrumentProvider instrumentProvider = client.instrumentProvider();

            this.tracer = instrumentProvider.getTracer();
            this.propagator = GlobalOpenTelemetry.getPropagators().getTextMapPropagator();
            this.initialized = true;
        }
    }

    @Override
    public void close() {
        // Producer will fail pending messages when it being closed,
        // which will trigger the `onSendAcknowledgement` events
    }

    @Override
    public boolean eligible(Message message) {
        return tracer != null && propagator != null;
    }

    @Override
    public Message beforeSend(Producer producer, Message message) {
        // Initialize tracer from producer on first call
        initializeIfNeeded(producer);

        if (!eligible(message)) {
            return message;
        }

        try {
            if (topic == null) {
                topic = producer.getTopic();
            }

            // Create a span for this message publication
            // The span will be linked to the current context, which may have been set by:
            // 1. An active span in the current thread (e.g., from HTTP request handling)
            // 2. Context propagated from upstream services
            Span span = TracingContext.createProducerSpan(tracer, topic, Context.current());

            if (TracingContext.isValid(span) && message instanceof TraceableMessage) {
                // Attach the span directly to the message
                ((TraceableMessage) message).setTracingSpan(span);
                log.debug("Created producer span for message on topic {}", topic);
            }
        } catch (Exception e) {
            log.error("Error creating producer span", e);
        }

        return message;
    }

    @Override
    public void onSendAcknowledgement(Producer producer, Message message, MessageId msgId, Throwable exception) {
        if (!(message instanceof TraceableMessage)) {
            return;
        }

        Span span = ((TraceableMessage) message).getTracingSpan();
        if (span != null) {
            try {
                if (msgId != null) {
                    span.setAttribute("messaging.message.id", msgId.toString());
                }

                if (exception != null) {
                    TracingContext.endSpan(span, exception);
                } else {
                    TracingContext.endSpan(span);
                }

                // Clear the span from the message
                ((TraceableMessage) message).setTracingSpan(null);
            } catch (Exception e) {
                log.error("Error ending producer span", e);
            }
        }
    }
}