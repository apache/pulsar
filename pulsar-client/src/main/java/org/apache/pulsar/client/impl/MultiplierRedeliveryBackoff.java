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

import static com.google.common.base.Preconditions.checkArgument;
import org.apache.pulsar.client.api.RedeliveryBackoff;

/**
 * MultiplierRedeliveryBackoff.
 */
public class MultiplierRedeliveryBackoff implements RedeliveryBackoff {

    private final long minDelayMs;
    private final long maxDelayMs;
    private final double multiplier;
    private final int maxMultiplierPow;

    private MultiplierRedeliveryBackoff(long minDelayMs, long maxDelayMs, double multiplier) {
        this.minDelayMs = minDelayMs;
        this.maxDelayMs = maxDelayMs;
        this.multiplier = multiplier;
        maxMultiplierPow = (int) (Math.log((double) maxDelayMs / minDelayMs) / Math.log(multiplier)) + 1;
    }

    public static MultiplierRedeliveryBackoff.MultiplierRedeliveryBackoffBuilder builder() {
        return new MultiplierRedeliveryBackoff.MultiplierRedeliveryBackoffBuilder();
    }

    public long getMinDelayMs() {
        return this.minDelayMs;
    }

    public long getMaxDelayMs() {
        return this.maxDelayMs;
    }

    @Override
    public long next(int redeliveryCount) {
        if (redeliveryCount <= 0 || minDelayMs <= 0) {
            return this.minDelayMs;
        }
        if (redeliveryCount > maxMultiplierPow) {
            return this.maxDelayMs;
        }
        return Math.min((long) (minDelayMs * Math.pow(multiplier, redeliveryCount)), this.maxDelayMs);
    }

    /**
     * Builder of MultiplierRedeliveryBackoff.
     */
    public static class MultiplierRedeliveryBackoffBuilder {
        private long minDelayMs = 1000 * 10;
        private long maxDelayMs = 1000 * 60 * 10;
        private double multiplier = 2.0;

        public MultiplierRedeliveryBackoffBuilder minDelayMs(long minDelayMs) {
            this.minDelayMs = minDelayMs;
            return this;
        }

        public MultiplierRedeliveryBackoffBuilder maxDelayMs(long maxDelayMs) {
            this.maxDelayMs = maxDelayMs;
            return this;
        }

        public MultiplierRedeliveryBackoffBuilder multiplier(double multiplier) {
            this.multiplier = multiplier;
            return this;
        }

        public MultiplierRedeliveryBackoff build() {
            checkArgument(minDelayMs >= 0, "min delay time must be >= 0");
            checkArgument(maxDelayMs >= minDelayMs, "maxDelayMs must be >= minDelayMs");
            checkArgument(multiplier > 1, "multiplier must be > 1");
            return new MultiplierRedeliveryBackoff(minDelayMs, maxDelayMs, multiplier);
        }
    }
}
