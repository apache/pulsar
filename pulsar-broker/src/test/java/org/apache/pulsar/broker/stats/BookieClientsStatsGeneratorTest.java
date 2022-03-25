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
package org.apache.pulsar.broker.stats;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Map;

import org.apache.bookkeeper.mledger.proto.PendingBookieOpsStats;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.common.stats.JvmMetrics;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

@Test(groups = "broker")
public class BookieClientsStatsGeneratorTest extends BrokerTestBase {

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.baseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testBookieClientStatsGenerator() throws Exception {
        // should not generate any NPE or other exceptions..
        Map<String, Map<String, PendingBookieOpsStats>> stats = BookieClientStatsGenerator.generate(super.getPulsar());
        assertTrue(stats.isEmpty());
    }

    @Test
    public void testJvmDirectMemoryUsedMetric() {
        PooledByteBufAllocator allocator = new PooledByteBufAllocator( //
                true, // preferDirect
                0, // nHeapArenas,
                1, // nDirectArena
                8192, // pageSize
                11, // maxOrder
                32, // smallCacheSize
                8, // normalCacheSize
                true // Cache all threads
        );
        int allocateMemory = 17777216;
        long directMemory1 = JvmMetrics.getJvmDirectMemoryUsed();
        ByteBuf buf2 = allocator.directBuffer(allocateMemory, allocateMemory);
        long directMemory2 = JvmMetrics.getJvmDirectMemoryUsed();
        assertEquals(directMemory2, directMemory1 + allocateMemory);
        ByteBuf buf3 = allocator.directBuffer(allocateMemory, allocateMemory);
        long directMemory3 = JvmMetrics.getJvmDirectMemoryUsed();
        assertEquals(directMemory3, directMemory2 + allocateMemory);
        buf3.release();
        directMemory3 = JvmMetrics.getJvmDirectMemoryUsed();
        assertEquals(directMemory3, directMemory2);
        buf2.release();
        directMemory2 = JvmMetrics.getJvmDirectMemoryUsed();
        assertEquals(directMemory2, directMemory1);

    }
}
