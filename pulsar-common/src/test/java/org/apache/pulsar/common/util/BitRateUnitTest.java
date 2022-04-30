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
        assertEquals(BitRateUnit.BPS.toBps(bps), bps);
        assertEquals(BitRateUnit.BPS.toByte(bps), bps / 8);
        assertEquals(BitRateUnit.BPS.toKbps(bps), bps / 1000);
        assertEquals(BitRateUnit.BPS.toMbps(bps), bps / 1000 / 1000);
        assertEquals(BitRateUnit.BPS.toGbps(bps), bps / 1000 / 1000 / 1000);
    }

    @Test
    public void testKbps() {
        double kbps = 1231434.12;
        assertEquals(BitRateUnit.KBPS.toBps(kbps), kbps * 1000);
        assertEquals(BitRateUnit.KBPS.toByte(kbps), kbps * 1000 / 8);
        assertEquals(BitRateUnit.KBPS.toKbps(kbps), kbps);
        assertEquals(BitRateUnit.KBPS.toMbps(kbps), kbps / 1000);
        assertEquals(BitRateUnit.KBPS.toGbps(kbps), kbps / 1000 / 1000);
    }

    @Test
    public void testMbps() {
        double mbps = 1231434.12;
        assertEquals(BitRateUnit.MBPS.toBps(mbps), mbps * 1000 * 1000);
        assertEquals(BitRateUnit.MBPS.toByte(mbps), mbps * 1000 * 1000 / 8);
        assertEquals(BitRateUnit.MBPS.toKbps(mbps), mbps * 1000);
        assertEquals(BitRateUnit.MBPS.toMbps(mbps), mbps);
        assertEquals(BitRateUnit.MBPS.toGbps(mbps), mbps / 1000);
    }

    @Test
    public void testGbps() {
        double gbps = 1231434.12;
        assertEquals(BitRateUnit.GBPS.toBps(gbps),gbps * 1000 * 1000 * 1000 );
        assertEquals(BitRateUnit.GBPS.toByte(gbps), gbps * 1000 * 1000 * 1000 / 8);
        assertEquals(BitRateUnit.GBPS.toKbps(gbps), gbps * 1000 * 1000);
        assertEquals(BitRateUnit.GBPS.toMbps(gbps), gbps * 1000);
        assertEquals(BitRateUnit.GBPS.toGbps(gbps), gbps);
    }

    @Test
    public void testByte() {
        double bytes = 1231434.12;
        assertEquals(BitRateUnit.BYTE.toBps(bytes), bytes * 8);
        assertEquals(BitRateUnit.BYTE.toByte(bytes), bytes);
        assertEquals(BitRateUnit.BYTE.toKbps(bytes), bytes / 1000 * 8);
        assertEquals(BitRateUnit.BYTE.toMbps(bytes), bytes / 1000 / 1000 * 8);
        assertEquals(BitRateUnit.BYTE.toGbps(bytes), bytes / 1000 / 1000 / 1000 * 8);
    }

}
