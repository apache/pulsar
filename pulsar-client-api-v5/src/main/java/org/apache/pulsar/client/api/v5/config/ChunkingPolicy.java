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

import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Configuration for chunking large messages that exceed the broker's max message size.
 *
 * <p>When chunking is enabled, the producer splits a payload larger than the
 * configured chunk size into smaller pieces that are reassembled by the consumer.
 *
 * <p>Use {@link #ofDisabled()} to opt out, or {@link #builder()} to enable with a
 * specific chunk size.
 */
@EqualsAndHashCode
@ToString
public final class ChunkingPolicy {

    private static final ChunkingPolicy DISABLED = new ChunkingPolicy(false, 0);

    private final boolean enabled;
    private final int chunkSize;

    private ChunkingPolicy(boolean enabled, int chunkSize) {
        if (enabled && chunkSize <= 0) {
            throw new IllegalArgumentException("chunkSize must be > 0 when chunking is enabled");
        }
        this.enabled = enabled;
        this.chunkSize = chunkSize;
    }

    /**
     * @return whether chunking is enabled
     */
    public boolean enabled() {
        return enabled;
    }

    /**
     * @return the maximum size of each chunk in bytes (only meaningful when {@link #enabled()})
     */
    public int chunkSize() {
        return chunkSize;
    }

    /**
     * Chunking disabled.
     *
     * @return a {@link ChunkingPolicy} with chunking disabled
     */
    public static ChunkingPolicy ofDisabled() {
        return DISABLED;
    }

    /**
     * @return a new builder for constructing a {@link ChunkingPolicy}
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link ChunkingPolicy}.
     */
    public static final class Builder {
        private boolean enabled = true;
        private int chunkSize;

        private Builder() {
        }

        /**
         * Whether chunking is enabled. Default is {@code true}.
         *
         * @param enabled whether to chunk oversized payloads
         * @return this builder
         */
        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        /**
         * Maximum size of each chunk in bytes. Required when chunking is enabled.
         *
         * @param chunkSize the per-chunk size in bytes
         * @return this builder
         */
        public Builder chunkSize(int chunkSize) {
            this.chunkSize = chunkSize;
            return this;
        }

        /**
         * @return a new {@link ChunkingPolicy} instance
         */
        public ChunkingPolicy build() {
            return new ChunkingPolicy(enabled, chunkSize);
        }
    }
}
