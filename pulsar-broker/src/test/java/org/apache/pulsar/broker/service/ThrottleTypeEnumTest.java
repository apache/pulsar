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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.util.Arrays;
import org.apache.pulsar.broker.service.ServerCnxThrottleTracker.ThrottleType;
import org.testng.annotations.Test;

/**
 * Guard tests for {@link ThrottleType} ordinal stability.
 *
 * <p>{@link ServerCnxThrottleTracker} indexes its {@code states[]} array by
 * {@link ThrottleType#ordinal()}.  Accidentally removing, renaming, or reordering
 * an enum constant shifts every subsequent ordinal and silently corrupts the
 * per-connection throttle-state tracking.
 *
 * <p>These tests exist to turn that silent corruption into a loud compile- or
 * test-time failure.  If you intentionally add a new constant, bump
 * {@link #EXPECTED_MIN_COUNT} and append the constant at the END of the enum.
 */
@Test(groups = "broker")
public class ThrottleTypeEnumTest {

    /**
     * The number of {@link ThrottleType} constants that existed when adaptive
     * throttling was integrated.  Increase this if you add a new constant;
     * never decrease it.
     */
    private static final int EXPECTED_MIN_COUNT = 8;

    /**
     * Fails if any constant is deleted or renamed so the count drops below
     * {@link #EXPECTED_MIN_COUNT}.  This catches "I thought it was unused"
     * removals that would break the {@code states[]} index mapping.
     */
    @Test
    public void testMinimumConstantCount() {
        int actual = ThrottleType.values().length;
        assertTrue(actual >= EXPECTED_MIN_COUNT,
                "ThrottleType has " + actual + " constants but expected >= " + EXPECTED_MIN_COUNT
                        + ". A constant was deleted or renamed — update states[] in "
                        + "ServerCnxThrottleTracker and adjust EXPECTED_MIN_COUNT here.");
    }

    /**
     * Fails if {@link ThrottleType#AdaptivePublishRate} is absent, confirming that
     * the adaptive throttling wire-up in
     * {@link AbstractTopic} has a valid target.
     */
    @Test
    public void testAdaptivePublishRateIsPresent() {
        boolean found = Arrays.stream(ThrottleType.values())
                .anyMatch(t -> t == ThrottleType.AdaptivePublishRate);
        assertTrue(found,
                "ThrottleType.AdaptivePublishRate is missing. "
                        + "Adaptive throttling in AbstractTopic will fail to compile or "
                        + "produce a NullPointerException at runtime.");
    }

    /**
     * Fails if {@link ThrottleType#AdaptivePublishRate} is not the last constant.
     * New constants must always be appended; reordering shifts ordinals and breaks
     * the index-based state array.
     */
    @Test
    public void testAdaptivePublishRateIsLast() {
        ThrottleType[] values = ThrottleType.values();
        assertEquals(values[values.length - 1], ThrottleType.AdaptivePublishRate,
                "ThrottleType.AdaptivePublishRate must remain the last enum constant. "
                        + "Reordering shifts ordinals and corrupts the states[] index mapping "
                        + "in ServerCnxThrottleTracker.");
    }

    /**
     * Confirms that {@link ThrottleType#AdaptivePublishRate} is declared reentrant,
     * which is required because multiple producers on the same topic share the per-topic
     * {@link AdaptivePublishRateLimiter}.
     */
    @Test
    public void testAdaptivePublishRateIsReentrant() {
        assertTrue(ThrottleType.AdaptivePublishRate.isReentrant(),
                "ThrottleType.AdaptivePublishRate must be reentrant (isReentrant=true). "
                        + "Multiple producers share a single per-topic AdaptivePublishRateLimiter "
                        + "and each independently calls markThrottled(); a non-reentrant type "
                        + "would drop all but the first activation.");
    }
}
