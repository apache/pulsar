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
import java.util.Objects;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Configuration for producer message batching.
 *
 * <p>When batching is enabled, the producer groups multiple messages into a single
 * broker request to improve throughput. A batch is flushed when any of the
 * configured thresholds is reached.
 *
 * <p>Construct via {@link #builder()}, or use {@link #ofDisabled()} to opt out.
 */
@EqualsAndHashCode
@ToString
public final class BatchingPolicy {

    private static final Duration DEFAULT_MAX_PUBLISH_DELAY = Duration.ofMillis(1);
    private static final int DEFAULT_MAX_MESSAGES = 1000;
    private static final MemorySize DEFAULT_MAX_SIZE = MemorySize.ofKilobytes(128);

    private static final BatchingPolicy DISABLED =
            new BatchingPolicy(false, DEFAULT_MAX_PUBLISH_DELAY, DEFAULT_MAX_MESSAGES, DEFAULT_MAX_SIZE);

    private final boolean enabled;
    private final Duration maxPublishDelay;
    private final int maxMessages;
    private final MemorySize maxSize;

    private BatchingPolicy(boolean enabled, Duration maxPublishDelay, int maxMessages, MemorySize maxSize) {
        if (maxPublishDelay == null) {
            maxPublishDelay = DEFAULT_MAX_PUBLISH_DELAY;
        }
        Objects.requireNonNull(maxSize, "maxSize must not be null");
        if (maxMessages < 0) {
            throw new IllegalArgumentException("maxMessages must be >= 0");
        }
        if (maxSize.bytes() < 0) {
            throw new IllegalArgumentException("maxBytes must be >= 0");
        }
        this.enabled = enabled;
        this.maxPublishDelay = maxPublishDelay;
        this.maxMessages = maxMessages;
        this.maxSize = maxSize;
    }

    /**
     * @return whether batching is enabled
     */
    public boolean enabled() {
        return enabled;
    }

    /**
     * @return the maximum time to wait before flushing a batch
     */
    public Duration maxPublishDelay() {
        return maxPublishDelay;
    }

    /**
     * @return the maximum number of messages in a single batch
     */
    public int maxMessages() {
        return maxMessages;
    }

    /**
     * @return the maximum size of a single batch
     */
    public MemorySize maxSize() {
        return maxSize;
    }

    /**
     * Batching disabled.
     *
     * @return a {@link BatchingPolicy} with batching disabled
     */
    public static BatchingPolicy ofDisabled() {
        return DISABLED;
    }

    /**
     * @return a new builder for constructing a {@link BatchingPolicy}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link BatchingPolicy}.
     */
    public static final class Builder {
        private boolean enabled = true;
        private Duration maxPublishDelay = DEFAULT_MAX_PUBLISH_DELAY;
        private int maxMessages = DEFAULT_MAX_MESSAGES;
        private MemorySize maxSize = DEFAULT_MAX_SIZE;

        private Builder() {
        }

        /**
         * Whether batching is enabled. Default is {@code true}.
         *
         * @param enabled whether to batch outgoing messages
         * @return this builder
         */
        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        /**
         * Maximum time the producer waits before flushing a partial batch.
         *
         * @param maxPublishDelay the upper bound on per-message latency added by batching
         * @return this builder
         */
        public Builder maxPublishDelay(Duration maxPublishDelay) {
            this.maxPublishDelay = maxPublishDelay;
            return this;
        }

        /**
         * Maximum number of messages per batch.
         *
         * @param maxMessages the message-count flush threshold
         * @return this builder
         */
        public Builder maxMessages(int maxMessages) {
            this.maxMessages = maxMessages;
            return this;
        }

        /**
         * Maximum payload size per batch.
         *
         * @param maxSize the size flush threshold
         * @return this builder
         */
        public Builder maxSize(MemorySize maxSize) {
            this.maxSize = maxSize;
            return this;
        }

        /**
         * @return a new {@link BatchingPolicy} instance
         */
        public BatchingPolicy build() {
            return new BatchingPolicy(enabled, maxPublishDelay, maxMessages, maxSize);
        }
    }
}
