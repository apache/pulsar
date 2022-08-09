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
package org.apache.pulsar.common.allocator;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBufAllocator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.common.allocator.LeakDetectionPolicy;
import org.apache.bookkeeper.common.allocator.OutOfMemoryPolicy;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;
import org.apache.bookkeeper.common.allocator.impl.ByteBufAllocatorImpl;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class PulsarByteBufAllocatorDefaultTest {

    @Test
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
            assertEquals(arguments.get(6), LeakDetectionPolicy.Advanced);
        })) {
            assertFalse(called.get());
            PulsarByteBufAllocator.createByteBufAllocator();
            assertTrue(called.get());
        }
    }

}