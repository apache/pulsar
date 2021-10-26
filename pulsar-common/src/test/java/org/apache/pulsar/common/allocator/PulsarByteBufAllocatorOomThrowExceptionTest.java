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

import io.netty.buffer.ByteBufAllocator;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.allocator.LeakDetectionPolicy;
import org.apache.bookkeeper.common.allocator.OutOfMemoryPolicy;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;
import org.apache.bookkeeper.common.allocator.impl.ByteBufAllocatorBuilderImpl;
import org.apache.bookkeeper.common.allocator.impl.ByteBufAllocatorImpl;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.IObjectFactory;
import org.testng.annotations.ObjectFactory;
import org.testng.annotations.Test;

@PrepareForTest({ByteBufAllocatorImpl.class, ByteBufAllocatorBuilderImpl.class})
@PowerMockIgnore({"javax.management.*", "javax.ws.*", "org.apache.logging.log4j.*"})
@Slf4j
public class PulsarByteBufAllocatorOomThrowExceptionTest {

    @ObjectFactory
    public IObjectFactory getObjectFactory() {
        return new org.powermock.modules.testng.PowerMockObjectFactory();
    }

    @Test
    public void testDefaultConfig() throws Exception {
        try {
            System.setProperty("pulsar.allocator.out_of_memory_policy", "ThrowException");
            final ByteBufAllocatorImpl mockAllocator = PowerMockito.mock(ByteBufAllocatorImpl.class);
            PowerMockito.whenNew(ByteBufAllocatorImpl.class).withAnyArguments().thenReturn(mockAllocator);
            final ByteBufAllocatorImpl byteBufAllocator = (ByteBufAllocatorImpl) PulsarByteBufAllocator.DEFAULT;
            // use the variable, in case the compiler optimization
            log.trace("{}", byteBufAllocator);
            PowerMockito.verifyNew(ByteBufAllocatorImpl.class).withArguments(Mockito.any(ByteBufAllocator.class), Mockito.any(),
                    Mockito.eq(PoolingPolicy.PooledDirect), Mockito.any(), Mockito.eq(OutOfMemoryPolicy.ThrowException),
                    Mockito.any(), Mockito.eq(LeakDetectionPolicy.Advanced));
        } finally {
            System.clearProperty("pulsar.allocator.out_of_memory_policy");
        }
    }

}
