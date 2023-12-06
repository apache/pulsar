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
package org.apache.pulsar.logger;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertFalse;
import org.testng.annotations.Test;

public class VerboseLoggerThrottleTest {

    @Test
    public void testAcquire() throws Exception {
        long periodInSecond = 5;
        int permits = 10;
        VerboseLoggerThrottle throttle = new VerboseLoggerThrottle(periodInSecond, permits);
        for (int i = 0; i < permits; i++) {
            assertTrue(throttle.tryAcquire());
        }
        assertFalse(throttle.tryAcquire());
        Thread.sleep(periodInSecond * 1000);
        for (int i = 0; i < permits; i++) {
            assertTrue(throttle.tryAcquire());
        }
    }
}
