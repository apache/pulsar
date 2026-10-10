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
package org.apache.pulsar.client.impl;

import org.apache.pulsar.client.api.RedeliveryBackoff;

import java.util.concurrent.ThreadLocalRandom;
import static com.google.common.base.Preconditions.checkArgument;

public class JitterRedeliveryBackoff implements RedeliveryBackoff {

    private final MultiplierRedeliveryBackoff multiplierBackoff;
    private final double jitterFactor;

    public JitterRedeliveryBackoff(
            MultiplierRedeliveryBackoff multiplierBackoff,
            double jitterFactor
    ) {
        this.multiplierBackoff = multiplierBackoff;
        this.jitterFactor = jitterFactor;
    }

    public static JitterRedeliveryBackoff.JitterRedeliveryBackoffBuilder builder() {
        return new JitterRedeliveryBackoff.JitterRedeliveryBackoffBuilder();
    }

    @Override
    public long next(int redeliveryCount) {
        long nextBackoff = multiplierBackoff.next(redeliveryCount);

        long jitterOffset = Math.round((nextBackoff * jitterFactor));
        long lowBound = Math.max(multiplierBackoff.getMinDelayMs() - nextBackoff, -jitterOffset);
        long highBound = Math.min(multiplierBackoff.getMaxDelayMs() - nextBackoff, jitterOffset);

        ThreadLocalRandom random = ThreadLocalRandom.current();
        long jitter;
        if (highBound == lowBound) {
            if (highBound == 0) jitter = 0;
            else jitter = random.nextLong(highBound);
        } else {
            jitter = random.nextLong(lowBound, highBound);
        }

        return nextBackoff + jitter;
    }

    public static class JitterRedeliveryBackoffBuilder {

        private MultiplierRedeliveryBackoff multiplierBackoff;
        private double jitterFactor = 0.5D;

        public JitterRedeliveryBackoff.JitterRedeliveryBackoffBuilder multiplierBackoff(
                MultiplierRedeliveryBackoff multiplierBackoff
        ) {
            this.multiplierBackoff = multiplierBackoff;
            return this;
        }

        /**
         * Set a jitter factor for exponential backoffs that adds randomness to each backoff. This can
         * be helpful in reducing cascading failure due to retry-storms. This method switches to an
         * exponential backoff strategy with a zero minimum backoff if not already a backoff strategy.
         * Defaults to {@code 0.5} (a jitter of at most 50% of the computed delay).
         *
         * @param jitterFactor the new jitter factor as a {@code double} between {@code 0d} and {@code 1d}
         */
        public JitterRedeliveryBackoff.JitterRedeliveryBackoffBuilder jitter(double jitterFactor) {
            this.jitterFactor = jitterFactor;
            return this;
        }

        public JitterRedeliveryBackoff build() {
            checkArgument(jitterFactor > 0 || jitterFactor < 1, "jitterFactor must be between 0 and 1 (default 0.5)");
            return new JitterRedeliveryBackoff(this.multiplierBackoff, this.jitterFactor);
        }
    }
}
