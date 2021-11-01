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

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.client.api.NegativeAckRedeliveryBackoff;

/**
 * NegativeAckRedeliveryExponentialBackoff
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class NegativeAckRedeliveryExponentialBackoff implements NegativeAckRedeliveryBackoff {

    private long minNackTimeMs = 1000 * 10;
    private long maxNackTimeMs = 1000 * 60 * 10;

    public static NegativeAckRedeliveryExponentialBackoff.NegativeAckRedeliveryExponentialBackoffBuilder builder() {
        return new NegativeAckRedeliveryExponentialBackoff.NegativeAckRedeliveryExponentialBackoffBuilder();
    }

    @Override
    public long next(int redeliveryCount) {
        if (redeliveryCount <= 0) {
            return minNackTimeMs;
        }
        return Math.min(Math.abs(minNackTimeMs << redeliveryCount), maxNackTimeMs);
    }

    @Override
    public long getMinNackTimeMs() {
        return minNackTimeMs;
    }

    @Override
    public long getMaxNackTimeMs() {
        return maxNackTimeMs;
    }

    /**
     * Builder of NegativeAckRedeliveryExponentialBackoff.
     */
    public static class NegativeAckRedeliveryExponentialBackoffBuilder implements Builder {
        private long minNackTimeMs = 1000 * 10;
        private long maxNackTimeMs = 1000 * 60 * 10;

        @Override
        public Builder minNackTimeMs(long minNackTimeMs) {
            this.minNackTimeMs = minNackTimeMs;
            return this;
        }

        @Override
        public Builder maxNackTimeMs(long maxNackTimeMs) {
            this.maxNackTimeMs = maxNackTimeMs;
            return this;
        }

        @Override
        public NegativeAckRedeliveryExponentialBackoff build() {
            return new NegativeAckRedeliveryExponentialBackoff(minNackTimeMs, maxNackTimeMs);
        }
    }
}
