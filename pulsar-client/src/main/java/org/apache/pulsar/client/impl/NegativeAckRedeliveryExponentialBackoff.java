/**
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
package org.apache.pulsar.client.impl;

import com.google.common.base.Preconditions;
import org.apache.pulsar.client.api.NegativeAckRedeliveryBackoff;

/**
 * NegativeAckRedeliveryExponentialBackoff
 */
public class NegativeAckRedeliveryExponentialBackoff implements NegativeAckRedeliveryBackoff {

    private final long minNackTimeMs;
    private final long maxNackTimeMs;
    private final int maxBitShift;

    private NegativeAckRedeliveryExponentialBackoff(long minNackTimeMs, long maxNackTimeMs) {
        this.minNackTimeMs = minNackTimeMs;
        this.maxNackTimeMs = maxNackTimeMs;

        for (int i = 0; ; ) {
            if (this.minNackTimeMs << ++i <= 0) {
                this.maxBitShift = i;
                break;
            }
        }
    }

    public static NegativeAckRedeliveryExponentialBackoff.NegativeAckRedeliveryExponentialBackoffBuilder builder() {
        return new NegativeAckRedeliveryExponentialBackoff.NegativeAckRedeliveryExponentialBackoffBuilder();
    }

    public long getMinNackTimeMs() {
        return this.minNackTimeMs;
    }

    public long getMaxNackTimeMs() {
        return this.maxNackTimeMs;
    }

    @Override
    public long next(int redeliveryCount) {
        if (redeliveryCount <= 0 || minNackTimeMs <= 0) {
            return this.minNackTimeMs;
        }

        if (this.maxBitShift <= redeliveryCount) {
            return this.maxNackTimeMs;
        }

        return Math.min(this.minNackTimeMs << redeliveryCount, this.maxNackTimeMs);
    }

    /**
     * Builder of NegativeAckRedeliveryExponentialBackoff.
     */
    public static class NegativeAckRedeliveryExponentialBackoffBuilder {
        private long minNackTimeMs = 1000 * 10;
        private long maxNackTimeMs = 1000 * 60 * 10;

        public NegativeAckRedeliveryExponentialBackoffBuilder minNackTimeMs(long minNackTimeMs) {
            this.minNackTimeMs = minNackTimeMs;
            return this;
        }

        public NegativeAckRedeliveryExponentialBackoffBuilder maxNackTimeMs(long maxNackTimeMs) {
            this.maxNackTimeMs = maxNackTimeMs;
            return this;
        }

        public NegativeAckRedeliveryExponentialBackoff build() {
            Preconditions.checkArgument(minNackTimeMs >= 0, "min nack time must be >= 0");
            Preconditions.checkArgument(maxNackTimeMs >= minNackTimeMs,
                    "max nack time must be >= minNackTimeMs");
            return new NegativeAckRedeliveryExponentialBackoff(minNackTimeMs, maxNackTimeMs);
        }
    }
}
