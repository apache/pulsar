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
    protected MonotonicClock clock = AsyncTokenBucket.DEFAULT_SNAPSHOT_CLOCK;
    protected long addTokensResolutionNanos = AsyncTokenBucket.DEFAULT_ADD_TOKENS_RESOLUTION_NANOS;

    protected AsyncTokenBucketBuilder() {
    }

    protected SELF self() {
        return (SELF) this;
    }

    /**
     * Set the clock source for the token bucket. It's recommended to use the {@link DefaultMonotonicClock}
     * for most use cases.
     */
    public SELF clock(MonotonicClock clock) {
        this.clock = clock;
        return self();
    }

    /**
     * This setting determines the minimum time interval that must pass since the last operation
     * before new tokens are added to the token balance.
     * Setting this value to 0 ensures the token balance is always up-to-date, but it may slightly impact performance.
     */
    public SELF addTokensResolutionNanos(long addTokensResolutionNanos) {
        this.addTokensResolutionNanos = addTokensResolutionNanos;
        return self();
    }

    public abstract AsyncTokenBucket build();
}
