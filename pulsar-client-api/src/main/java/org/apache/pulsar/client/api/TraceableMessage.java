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
package org.apache.pulsar.client.api;

import io.opentelemetry.api.trace.Span;

/**
 * Extension of {@link Message} interface that supports OpenTelemetry tracing.
 * <p>
 * This interface allows attaching OpenTelemetry spans directly to messages,
 * eliminating the need for external tracking via maps.
 * <p>
 * The span lifecycle:
 * <ul>
 *   <li>Producer: Span is created before send and attached to the message.
 *       When the send is acknowledged, the span is retrieved and completed.</li>
 *   <li>Consumer: Span is created when message is received and attached to the message.
 *       When the message is acknowledged, the span is retrieved and completed.</li>
 * </ul>
 */
public interface TraceableMessage {

    /**
     * Set the OpenTelemetry span associated with this message.
     * <p>
     * This method is called by tracing interceptors to attach a span to the message
     * for later retrieval when completing the span.
     *
     * @param span the span to associate with this message, or null to clear
     */
    void setTracingSpan(Span span);

    /**
     * Get the OpenTelemetry span associated with this message.
     *
     * @return the span associated with this message, or null if no span is set
     */
    Span getTracingSpan();
}