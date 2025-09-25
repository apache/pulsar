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

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.apache.pulsar.common.naming.TopicName;
import org.jspecify.annotations.Nullable;

@RequiredArgsConstructor
public class TopicLoadingContext {

    private static final String EXAMPLE_LATENCY_OUTPUTS = "1234 ms (queued: 567)";

    private final long startNs = System.nanoTime();
    @Getter
    private final TopicName topicName;
    @Getter
    private final boolean createIfMissing;
    @Getter
    private final CompletableFuture<Optional<Topic>> topicFuture;
    @Getter
    @Setter
    @Nullable private Map<String, String> properties;
    private long polledFromQueueNs = -1L;

    public void polledFromQueue() {
        polledFromQueueNs = System.nanoTime();
    }

    public long latencyMs(long nowInNanos) {
        return TimeUnit.NANOSECONDS.toMillis(nowInNanos - startNs);
    }

    public String latencyString(long nowInNanos) {
        final var builder = new StringBuilder(EXAMPLE_LATENCY_OUTPUTS.length());
        builder.append(latencyMs(nowInNanos));
        builder.append(" ms");
        if (polledFromQueueNs >= 0) {
            builder.append(" (queued: ").append(TimeUnit.NANOSECONDS.toMillis(polledFromQueueNs - startNs)).append(")");
        }
        return builder.toString();
    }
}
