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
package org.apache.pulsar.broker.service;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.testng.annotations.Test;

public class PrecisPublishLimiterTest {

    @Test
    void shouldResetMsgLimitAfterUpdate() {
        PrecisPublishLimiter precisPublishLimiter = new PrecisPublishLimiter(new PublishRate(), () -> {
        });
        precisPublishLimiter.update(new PublishRate(1, 1));
        assertFalse(precisPublishLimiter.tryAcquire(99, 99));
        precisPublishLimiter.update(new PublishRate(-1, 100));
        assertTrue(precisPublishLimiter.tryAcquire(99, 99));
    }

    @Test
    void shouldResetBytesLimitAfterUpdate() {
        PrecisPublishLimiter precisPublishLimiter = new PrecisPublishLimiter(new PublishRate(), () -> {
        });
        precisPublishLimiter.update(new PublishRate(1, 1));
        assertFalse(precisPublishLimiter.tryAcquire(99, 99));
        precisPublishLimiter.update(new PublishRate(100, -1));
        assertTrue(precisPublishLimiter.tryAcquire(99, 99));
    }

    @Test
    void shouldCloseResources() throws Exception {
        for (int i = 0; i < 20000; i++) {
            PrecisPublishLimiter precisPublishLimiter = new PrecisPublishLimiter(new PublishRate(100, 100), () -> {
            });
            precisPublishLimiter.tryAcquire(99, 99);
            precisPublishLimiter.close();
        }
    }
}