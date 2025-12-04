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

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapSetter;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.jspecify.annotations.Nullable;

/**
 * Utility class for managing OpenTelemetry tracing context in Pulsar messages.
 */
public class TracingContext {

    private static final TextMapGetter<Map<String, String>> GETTER = new TextMapGetter<Map<String, String>>() {
        @Override
        public Iterable<String> keys(Map<String, String> carrier) {
            return carrier.keySet();
        }

        @Nullable
        @Override
        public String get(@Nullable Map<String, String> carrier, String key) {
            return carrier != null ? carrier.get(key) : null;
        }
    };

    private static final TextMapSetter<Map<String, String>> SETTER = (carrier, key, value) -> {
        if (carrier != null) {
            carrier.put(key, value);
        }
    };

    /**
     * Extract trace context from message properties.
     *
     * @param message the message to extract context from
     * @param propagator the text map propagator to use
     * @return the extracted context
     */
    public static Context extractContext(Message<?> message, TextMapPropagator propagator) {
        if (message == null || propagator == null) {
            return Context.current();
        }
        return propagator.extract(Context.current(), message.getProperties(), GETTER);
    }

    /**
     * Inject trace context into message properties.
     *
     * @param messageBuilder the message builder to inject context into
     * @param context the context to inject
     * @param propagator the text map propagator to use
     */
    public static <T> void injectContext(TypedMessageBuilder<T> messageBuilder, Context context,
                                          TextMapPropagator propagator) {
        if (messageBuilder == null || context == null || propagator == null) {
            return;
        }

        Map<String, String> carrier = new HashMap<>();
        propagator.inject(context, carrier, SETTER);

        for (Map.Entry<String, String> entry : carrier.entrySet()) {
            messageBuilder.property(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Create a producer span for message publishing.
     *
     * @param tracer the tracer to use
     * @param topic the topic name
     * @param parentContext the parent context (may be null)
     * @return the created span
     */
    public static Span createProducerSpan(Tracer tracer, String topic, @Nullable Context parentContext) {
        if (tracer == null) {
            return Span.getInvalid();
        }

        Context context = parentContext != null ? parentContext : Context.current();
        return tracer.spanBuilder("send " + topic)
                .setParent(context)
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute("messaging.system", "pulsar")
                .setAttribute("messaging.destination.name", topic)
                .setAttribute("messaging.operation.name", "send")
                .startSpan();
    }

    /**
     * Create a consumer span for message consumption.
     *
     * @param tracer the tracer to use
     * @param topic the topic name
     * @param subscription the subscription name
     * @param message the message being consumed
     * @param propagator the text map propagator to use for context extraction
     * @return the created span
     */
    public static Span createConsumerSpan(Tracer tracer, String topic, String subscription, Message<?> message,
                                           TextMapPropagator propagator) {
        if (tracer == null) {
            return Span.getInvalid();
        }

        Context parentContext = extractContext(message, propagator);

        return tracer.spanBuilder("process " + topic)
                .setParent(parentContext)
                .setSpanKind(SpanKind.CONSUMER)
                .setAttribute("messaging.system", "pulsar")
                .setAttribute("messaging.destination.name", topic)
                .setAttribute("messaging.destination.subscription.name", subscription)
                .setAttribute("messaging.operation.name", "process")
                .setAttribute("messaging.message.id", message.getMessageId().toString())
                .startSpan();
    }

    /**
     * Mark a span as successful and end it.
     *
     * @param span the span to end
     */
    public static void endSpan(Span span) {
        if (span != null && span.isRecording()) {
            span.setStatus(StatusCode.OK);
            span.end();
        }
    }

    /**
     * Mark a span as failed with an exception and end it.
     *
     * @param span the span to end
     * @param throwable the exception that caused the failure
     */
    public static void endSpan(Span span, Throwable throwable) {
        if (span != null && span.isRecording()) {
            span.setStatus(StatusCode.ERROR, throwable.getMessage());
            span.recordException(throwable);
            span.end();
        }
    }

    /**
     * Check if a span has a valid context.
     *
     * @param span the span to check
     * @return true if the span has a valid context
     */
    public static boolean isValid(Span span) {
        return span != null && span.getSpanContext() != null && span.getSpanContext().isValid();
    }

    /**
     * Get the span context from a span.
     *
     * @param span the span
     * @return the span context
     */
    public static SpanContext getSpanContext(Span span) {
        return span != null ? span.getSpanContext() : SpanContext.getInvalid();
    }
}