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

import static org.testng.Assert.fail;
import org.testng.annotations.Test;

public class PublishRateLimiterDisableTest {

    // GH issue #10603
    @Test
    void shouldAlwaysAllowAcquire() {
        PublishRateLimiterDisable publishRateLimiter = PublishRateLimiterDisable.DISABLED_RATE_LIMITER;
        ThrottleHandler throttleHandler = (initialPauseTimeNanos, additionalPauseTimeSupplier) -> {
            fail("Should not be throttled");
        };
        publishRateLimiter.incrementPublishCountAndThrottleWhenNeeded(Integer.MAX_VALUE, Long.MAX_VALUE,
                throttleHandler);
    }
}