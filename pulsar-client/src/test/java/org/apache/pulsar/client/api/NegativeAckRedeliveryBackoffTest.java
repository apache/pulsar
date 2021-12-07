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

import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;

import org.apache.pulsar.client.impl.NegativeAckRedeliveryExponentialBackoff;
import org.testng.annotations.Test;

/**
 * Unit test of {@link NegativeAckRedeliveryBackoff}.
 */
public class NegativeAckRedeliveryBackoffTest {

    @SuppressWarnings("deprecation")
    @Test
    public void testNext() {

        long minNackTime = 1000;
        long maxNackTime = 1000 * 60 * 10;

        NegativeAckRedeliveryBackoff nackBackoff = spy(
                NegativeAckRedeliveryExponentialBackoff.builder()
                        .minNackTimeMs(minNackTime)
                        .maxNackTimeMs(maxNackTime)
                        .build());

        assertEquals(nackBackoff.next(-1), minNackTime);

        assertEquals(nackBackoff.next(0), minNackTime);

        assertEquals(nackBackoff.next(1),minNackTime * 2);

        assertEquals(nackBackoff.next(4), minNackTime * 16);

        assertEquals(nackBackoff.next(100), maxNackTime);
    }
}
