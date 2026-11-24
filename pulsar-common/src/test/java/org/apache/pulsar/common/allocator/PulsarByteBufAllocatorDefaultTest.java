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
package org.apache.pulsar.common.allocator;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocatorMetric;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.common.allocator.OutOfMemoryPolicy;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;
import org.apache.bookkeeper.common.allocator.impl.ByteBufAllocatorImpl;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class PulsarByteBufAllocatorDefaultTest {

    @Test
    @SuppressWarnings("try")
    public void testDefaultConfig() {
        // Force initialize PulsarByteBufAllocator.DEFAULT before mock the ctor so that it is not polluted.
        assertNotNull(PulsarByteBufAllocator.DEFAULT);

        AtomicBoolean called = new AtomicBoolean();
        try (MockedConstruction<ByteBufAllocatorImpl> ignored = Mockito.mockConstruction(ByteBufAllocatorImpl.class,
                (mock, context) -> {
            called.set(true);
            final List<?> arguments = context.arguments();
            assertTrue(arguments.get(0) instanceof ByteBufAllocator);
            assertEquals(arguments.get(2), PoolingPolicy.PooledDirect);
            assertEquals(arguments.get(4), OutOfMemoryPolicy.FallbackToHeap);
        })) {
            assertFalse(called.get());
            PulsarByteBufAllocator.createByteBufAllocator();
            assertTrue(called.get());
        }
    }

    /**
     * Verify that a {@link PooledByteBufAllocator} created with {@code maxOrder=10} produces the expected chunk size,
     * which is consistent with the {@code -Dio.netty.allocator.maxOrder=10} setting in {@code conf/pulsar_env.sh}.
     *
     * <p>Netty computes chunk size as: {@code pageSize << maxOrder = 8192 << 10 = 8,388,608 bytes (8 MiB)}.
     * This test constructs the allocator directly with {@code maxOrder=10} so no JVM argument is required.
     */
    @Test
    public void testDefaultChunkSizeMatchesMaxOrder10() {
        // Expected chunk size: pageSize (8192 bytes) << maxOrder (10) = 8 MiB
        final int maxOrder = 10;
        final int expectedChunkSize = 8192 << maxOrder;

        // Create a PooledByteBufAllocator with maxOrder=10, same as -Dio.netty.allocator.maxOrder=10
        PooledByteBufAllocator allocator = new PooledByteBufAllocator(
                true,           // preferDirect
                0,                        // nHeapArena
                1,                        // nDirectArena
                8192,                     // pageSize (default)
                maxOrder,                 // maxOrder=10
                64,                       // smallPageSize (default)
                256,                      // normalPageSize (default)
                false,
                0
        );

        PooledByteBufAllocatorMetric metric = allocator.metric();
        // Verify that the chunk size derived from maxOrder=10 equals 8 MiB
        assertEquals(metric.chunkSize(), expectedChunkSize,
                "Chunk size should be 8 MiB (pageSize << maxOrder = 8192 << 10) "
                        + "as configured by -Dio.netty.allocator.maxOrder=10 in pulsar_env.sh");

    }

}