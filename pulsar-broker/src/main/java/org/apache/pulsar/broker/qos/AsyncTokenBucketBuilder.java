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

package org.apache.pulsar.broker.qos;

// CHECKSTYLE.OFF: ClassTypeParameterName
public abstract class AsyncTokenBucketBuilder<SELF extends AsyncTokenBucketBuilder<SELF>> {
    protected MonotonicSnapshotClock clock = AsyncTokenBucket.DEFAULT_SNAPSHOT_CLOCK;
    protected long resolutionNanos = AsyncTokenBucket.defaultResolutionNanos;
    protected boolean consistentConsumedTokens;
    protected boolean consistentAddedTokens;

    protected AsyncTokenBucketBuilder() {
    }

    protected SELF self() {
        return (SELF) this;
    }

    /**
     * Set the clock source for the token bucket. It's recommended to use the {@link DefaultMonotonicSnapshotClock}
     * for most use cases.
     */
    public SELF clock(MonotonicSnapshotClock clock) {
        this.clock = clock;
        return self();
    }

    /**
     * By default, AsyncTokenBucket is eventually consistent. This means that the token balance is updated, when time
     * advances more than the configured resolution. This setting determines the duration of the increment.
     * Setting this value to 0 will make the token balance fully consistent. There's a performance trade-off
     * when setting this value to 0.
     */
    public SELF resolutionNanos(long resolutionNanos) {
        this.resolutionNanos = resolutionNanos;
        return self();
    }

    /**
     * By default, AsyncTokenBucket is eventually consistent. This means that the consumed tokens are subtracted from
     * the total amount of tokens at most once during each "increment", when time advances more than the configured
     * resolution. This setting determines if the consumed tokens are subtracted from tokens balance consistently.
     * For high performance, it is recommended to keep this setting as false.
     */
    public SELF consistentConsumedTokens(boolean consistentConsumedTokens) {
        this.consistentConsumedTokens = consistentConsumedTokens;
        return self();
    }

    /**
     * By default, AsyncTokenBucket is eventually consistent. This means that the added tokens are calculated based
     * on elapsed time at most once during each "increment", when time advances more than the configured
     * resolution. This setting determines if the added tokens are calculated and added to tokens balance consistently.
     * For high performance, it is recommended to keep this setting as false.
     */
    public SELF consistentAddedTokens(boolean consistentAddedTokens) {
        this.consistentAddedTokens = consistentAddedTokens;
        return self();
    }

    public abstract AsyncTokenBucket build();
}
