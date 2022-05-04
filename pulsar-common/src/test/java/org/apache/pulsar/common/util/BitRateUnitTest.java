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
package org.apache.pulsar.common.util;

import static org.testng.Assert.assertEquals;
import org.testng.annotations.Test;
public class BitRateUnitTest {

    @Test
    public void testBps() {
        double bps = 1231434.12;
        assertEquals(BitRateUnit.Bit.toBit(bps), bps);
        assertEquals(BitRateUnit.Bit.toByte(bps), bps / 8);
        assertEquals(BitRateUnit.Bit.toKilobit(bps), bps / 1000);
        assertEquals(BitRateUnit.Bit.toMegabit(bps), bps / 1000 / 1000);
        assertEquals(BitRateUnit.Bit.toGigabit(bps), bps / 1000 / 1000 / 1000);
    }

    @Test
    public void testKbps() {
        double kbps = 1231434.12;
        assertEquals(BitRateUnit.Kilobit.toBit(kbps), kbps * 1000);
        assertEquals(BitRateUnit.Kilobit.toByte(kbps), kbps * 1000 / 8);
        assertEquals(BitRateUnit.Kilobit.toKilobit(kbps), kbps);
        assertEquals(BitRateUnit.Kilobit.toMegabit(kbps), kbps / 1000);
        assertEquals(BitRateUnit.Kilobit.toGigabit(kbps), kbps / 1000 / 1000);
    }

    @Test
    public void testMbps() {
        double mbps = 1231434.12;
        assertEquals(BitRateUnit.Megabit.toBit(mbps), mbps * 1000 * 1000);
        assertEquals(BitRateUnit.Megabit.toByte(mbps), mbps * 1000 * 1000 / 8);
        assertEquals(BitRateUnit.Megabit.toKilobit(mbps), mbps * 1000);
        assertEquals(BitRateUnit.Megabit.toMegabit(mbps), mbps);
        assertEquals(BitRateUnit.Megabit.toGigabit(mbps), mbps / 1000);
    }

    @Test
    public void testGbps() {
        double gbps = 1231434.12;
        assertEquals(BitRateUnit.Gigabit.toBit(gbps),gbps * 1000 * 1000 * 1000 );
        assertEquals(BitRateUnit.Gigabit.toByte(gbps), gbps * 1000 * 1000 * 1000 / 8);
        assertEquals(BitRateUnit.Gigabit.toKilobit(gbps), gbps * 1000 * 1000);
        assertEquals(BitRateUnit.Gigabit.toMegabit(gbps), gbps * 1000);
        assertEquals(BitRateUnit.Gigabit.toGigabit(gbps), gbps);
    }

    @Test
    public void testByte() {
        double bytes = 1231434.12;
        assertEquals(BitRateUnit.Byte.toBit(bytes), bytes * 8);
        assertEquals(BitRateUnit.Byte.toByte(bytes), bytes);
        assertEquals(BitRateUnit.Byte.toKilobit(bytes), bytes / 1000 * 8);
        assertEquals(BitRateUnit.Byte.toMegabit(bytes), bytes / 1000 / 1000 * 8);
        assertEquals(BitRateUnit.Byte.toGigabit(bytes), bytes / 1000 / 1000 / 1000 * 8);
    }

}
