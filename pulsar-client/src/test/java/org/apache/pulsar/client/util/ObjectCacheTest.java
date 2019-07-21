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
package org.apache.pulsar.client.util;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.time.Clock;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.testng.annotations.Test;

public class ObjectCacheTest {
    @Test
    public void testCache() {

        AtomicLong currentTime = new AtomicLong(0);

        Clock clock = mock(Clock.class);
        when(clock.millis()).then(invocation -> currentTime.longValue());

        AtomicInteger currentValue = new AtomicInteger(0);

        Supplier<Integer> cache = new ObjectCache<>(() -> currentValue.getAndIncrement(),
                10, TimeUnit.MILLISECONDS, clock);

        cache.get();
        assertEquals(cache.get().intValue(), 0);
        assertEquals(cache.get().intValue(), 0);

        currentTime.set(1);
        // Still the value has not expired
        assertEquals(cache.get().intValue(), 0);

        currentTime.set(10);
        assertEquals(cache.get().intValue(), 1);


        currentTime.set(15);
        assertEquals(cache.get().intValue(), 1);

        currentTime.set(22);
        assertEquals(cache.get().intValue(), 2);
    }
}
