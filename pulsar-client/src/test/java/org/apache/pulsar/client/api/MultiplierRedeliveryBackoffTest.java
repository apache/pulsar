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
package org.apache.pulsar.client.api;

import org.apache.pulsar.client.impl.MultiplierRedeliveryBackoff;
import org.testng.annotations.Test;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;

/**
 * Unit test of {@link MultiplierRedeliveryBackoff}.
 */
public class MultiplierRedeliveryBackoffTest {

    @SuppressWarnings("deprecation")
    @Test
    public void testNext() {

        long minDelayMs = 1000;
        long maxDelayMs = 1000 * 60 * 10;

        RedeliveryBackoff redeliveryBackoff = spy(
                MultiplierRedeliveryBackoff.builder()
                        .minDelayMs(minDelayMs)
                        .maxDelayMs(maxDelayMs)
                        .build());

        assertEquals(redeliveryBackoff.next(-1), minDelayMs);

        assertEquals(redeliveryBackoff.next(0), minDelayMs);

        assertEquals(redeliveryBackoff.next(1), minDelayMs * 2);

        assertEquals(redeliveryBackoff.next(4), minDelayMs * 16);

        assertEquals(redeliveryBackoff.next(100), maxDelayMs);

        minDelayMs = 2000;
        maxDelayMs = 10000 * 60 * 10;
        int multiplier = 5;
        redeliveryBackoff = spy(
                MultiplierRedeliveryBackoff.builder()
                        .minDelayMs(minDelayMs)
                        .maxDelayMs(maxDelayMs)
                        .multiplier(multiplier)
                        .build());

        assertEquals(redeliveryBackoff.next(-1), minDelayMs);

        assertEquals(redeliveryBackoff.next(0), minDelayMs);

        assertEquals(redeliveryBackoff.next(1), minDelayMs * 5);

        assertEquals(redeliveryBackoff.next(2), minDelayMs * 25);

        assertEquals(redeliveryBackoff.next(4), minDelayMs * 625);

        assertEquals(redeliveryBackoff.next(100), maxDelayMs);
    }
}
