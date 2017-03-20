/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.yahoo.pulsar.client.impl;

import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.Test;

public class BackoffTest {

    @Test
    public void shouldBackoffTest() {
        long currentTimestamp = System.nanoTime();
        Backoff testBackoff = new Backoff(currentTimestamp, TimeUnit.NANOSECONDS, 100, TimeUnit.MICROSECONDS);
        // gives false
        Assert.assertTrue(!testBackoff.shouldBackoff(0L, TimeUnit.NANOSECONDS, 0));
        currentTimestamp = System.nanoTime();
        // gives true
        Assert.assertTrue(testBackoff.shouldBackoff(currentTimestamp, TimeUnit.NANOSECONDS, 100));

    }

}
