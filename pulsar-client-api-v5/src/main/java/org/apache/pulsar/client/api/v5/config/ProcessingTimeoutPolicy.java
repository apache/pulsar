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
package org.apache.pulsar.client.api.v5.config;

import java.time.Duration;

/**
 * Optional safety net for slow / stalled queue consumers: if the application doesn't
 * process and acknowledge a delivered message within {@code timeout}, the <em>client</em>
 * gives up on that delivery and asks the broker to redeliver it (to this consumer or,
 * on a Shared subscription, to another consumer in the group). The bookkeeping is
 * client-side — the client tracks pending acks and, on timeout, sends a
 * {@code redeliverUnacknowledgedMessages} request to the broker.
 *
 * <p>{@code redeliveryBackoff} controls the cadence of those redeliveries — {@code null}
 * means "redeliver immediately on the next sweep", which is the historical default.
 *
 * <p>Disabled by default. Pass to
 * {@link org.apache.pulsar.client.api.v5.QueueConsumerBuilder#processingTimeout(ProcessingTimeoutPolicy)}
 * when the application's processing time is bounded and you want stalled deliveries to
 * be reattempted automatically.
 *
 * @param timeout            how long the client waits for the application to ack a
 *                           delivery before requesting redelivery. {@link Duration#ZERO}
 *                           disables.
 * @param redeliveryBackoff  optional backoff applied between redeliveries. May be
 *                           {@code null} for the default (no extra delay).
 */
public record ProcessingTimeoutPolicy(
        Duration timeout,
        BackoffPolicy redeliveryBackoff
) {
    public ProcessingTimeoutPolicy {
        if (timeout == null) {
            throw new IllegalArgumentException("timeout must not be null");
        }
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must not be negative");
        }
    }

    /**
     * Create a policy with just a timeout — the broker uses its default redelivery
     * cadence (no extra backoff between retries).
     */
    public static ProcessingTimeoutPolicy of(Duration timeout) {
        return new ProcessingTimeoutPolicy(timeout, null);
    }

    /**
     * Create a policy with a timeout and an explicit redelivery backoff.
     */
    public static ProcessingTimeoutPolicy of(Duration timeout, BackoffPolicy redeliveryBackoff) {
        return new ProcessingTimeoutPolicy(timeout, redeliveryBackoff);
    }
}
