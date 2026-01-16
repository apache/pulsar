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

/**
 * OpenTelemetry tracing support for Pulsar Java client.
 *
 * <h2>Overview</h2>
 * This package provides OpenTelemetry distributed tracing capabilities for Pulsar producers and consumers.
 * It automatically creates spans for message publishing, consumption, and acknowledgment operations,
 * and propagates trace context across services using message properties.
 *
 * <h2>Producer Tracing</h2>
 * Producer tracing tracks:
 * <ul>
 *   <li><b>publish</b> - Span created when send is called</li>
 *   <li><b>published</b> - Span completed when broker confirms receipt</li>
 * </ul>
 *
 * <h3>Basic Producer Example</h3>
 * <pre>{@code
 * // Configure OpenTelemetry (or use auto-instrumentation)
 * OpenTelemetry openTelemetry = ...;
 *
 * // Create producer with tracing interceptor
 * Producer<String> producer = client.newProducer(Schema.STRING)
 *     .topic("my-topic")
 *     .intercept(new OpenTelemetryProducerInterceptor())
 *     .create();
 *
 * // Send message - trace context is automatically propagated
 * producer.newMessage()
 *     .value("Hello World")
 *     .send();
 * }</pre>
 *
 * <p>
 * Trace context is automatically injected into message properties from the current thread's context.
 * This means if your code is running within a traced HTTP request or any other active span,
 * the trace will automatically continue through Pulsar messages.
 *
 * <h2>Consumer Tracing</h2>
 * Consumer tracing tracks:
 * <ul>
 *   <li><b>consume</b> - Span created when message is received</li>
 *   <li><b>ack</b> - Span completed when message is acknowledged</li>
 *   <li><b>nack</b> - Span completed when message is negatively acknowledged</li>
 * </ul>
 *
 * <h3>Basic Consumer Example</h3>
 * <pre>{@code
 * // Create consumer with tracing interceptor
 * Consumer<String> consumer = client.newConsumer(Schema.STRING)
 *     .topic("my-topic")
 *     .subscriptionName("my-subscription")
 *     .intercept(new OpenTelemetryConsumerInterceptor<>())
 *     .subscribe();
 *
 * // Receive and process messages - trace context is automatically extracted
 * while (true) {
 *     Message<String> msg = consumer.receive();
 *     try {
 *         // Process message
 *         System.out.println("Received: " + msg.getValue());
 *         consumer.acknowledge(msg);
 *     } catch (Exception e) {
 *         consumer.negativeAcknowledge(msg);
 *     }
 * }
 * }</pre>
 *
 * <h2>End-to-End Tracing Example</h2>
 * <pre>{@code
 * // Service 1: HTTP endpoint that publishes to Pulsar
 * // When using auto-instrumentation or OpenTelemetry SDK, the HTTP request
 * // will have an active span context that automatically propagates to Pulsar
 * @POST
 * @Path("/publish")
 * public Response publishMessage(String body) {
 *     // Send message - trace context automatically injected!
 *     producer.newMessage()
 *         .value(body)
 *         .send();
 *
 *     return Response.ok().build();
 * }
 *
 * // Service 2: Consumer that processes messages
 * Consumer<String> consumer = client.newConsumer(Schema.STRING)
 *     .topic("my-topic")
 *     .subscriptionName("my-subscription")
 *     .intercept(new OpenTelemetryConsumerInterceptor<>())
 *     .subscribe();
 *
 * // Process messages - trace continues from HTTP request
 * Message<String> msg = consumer.receive();
 * // Trace context is automatically extracted from message properties
 * processMessage(msg.getValue());
 * consumer.acknowledge(msg);
 *
 * // The entire flow from HTTP request -> Producer -> Consumer is now traced!
 * }</pre>
 *
 * <h2>Configuration</h2>
 * OpenTelemetry can be configured via:
 * <ul>
 *   <li>Java Agent auto-instrumentation</li>
 *   <li>Environment variables (OTEL_*)</li>
 *   <li>System properties (otel.*)</li>
 *   <li>Programmatic configuration</li>
 * </ul>
 *
 * <h2>Trace Context Propagation</h2>
 * Trace context is propagated using W3C TraceContext format via message properties:
 * <ul>
 *   <li><b>traceparent</b> - Contains trace ID, span ID, and trace flags</li>
 *   <li><b>tracestate</b> - Contains vendor-specific trace information</li>
 * </ul>
 *
 * @see OpenTelemetryProducerInterceptor
 * @see OpenTelemetryConsumerInterceptor
 * @see TracingContext
 */
package org.apache.pulsar.client.impl.tracing;