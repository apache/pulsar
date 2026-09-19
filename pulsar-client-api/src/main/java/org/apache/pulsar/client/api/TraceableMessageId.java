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
 * Extension interface that allows {@link MessageId} implementations to support OpenTelemetry tracing.
 * <p>
 * This interface enables attaching OpenTelemetry spans directly to message IDs,
 * allowing span retrieval in acknowledge callbacks which only receive MessageId,
 * not the full Message object.
 * <p>
 * This is particularly useful for consumer-side tracing where:
 * <ul>
 *   <li>A span is created when a message is received (in beforeConsume)</li>
 *   <li>The span is attached to the message's MessageId</li>
 *   <li>When the message is acknowledged, the span can be retrieved from the MessageId
 *       and completed, even though the acknowledge callback only provides MessageId</li>
 * </ul>
 */
public interface TraceableMessageId {

    /**
     * Set the OpenTelemetry span associated with this message ID.
     * <p>
     * This method is called by tracing interceptors to attach a span to the message ID
     * for later retrieval in acknowledge callbacks.
     *
     * @param span the span to associate with this message ID, or null to clear
     */
    void setTracingSpan(Span span);

    /**
     * Get the OpenTelemetry span associated with this message ID.
     *
     * @return the span associated with this message ID, or null if no span is set
     */
    Span getTracingSpan();
}
