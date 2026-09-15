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

import static org.apache.pulsar.common.allocator.PulsarByteBufAllocator.DEFAULT_ALLOCATOR_NAME;
import static org.apache.pulsar.common.allocator.PulsarByteBufAllocator.ML_CACHE_ALLOCATOR_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import io.netty.buffer.AdaptiveByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufAllocatorMetric;
import io.netty.buffer.PooledByteBufAllocatorMetric;
import io.netty.util.ResourceLeakDetector;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.common.allocator.OutOfMemoryPolicy;
import org.apache.bookkeeper.common.allocator.impl.ByteBufAllocatorImpl;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator.AllocatorRegistry;
import org.apache.pulsar.common.util.ShutdownUtil;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class PulsarByteBufAllocatorTest {
    @DataProvider
    public Object[][] allocatorTypes() {
        return new Object[][] {{"pooled"}, {"unpooled"}, {"adaptive"}};
    }

    @Test(dataProvider = "allocatorTypes")
    public void testNamedAllocatorsHaveIndependentMetrics(String type) {
        Properties properties = new Properties();
        properties.setProperty("pulsar.allocator.type", type);
        AllocatorRegistry registry = new AllocatorRegistry(properties::getProperty);
        ByteBufAllocator first = registry.getOrCreate("first");
        ByteBufAllocator second = registry.getOrCreate("second");
        assertThat(first).isSameAs(registry.getOrCreate("first")).isNotSameAs(second);
        ByteBufAllocatorMetric firstMetric = registry.getAllocatorMetric("first");
        ByteBufAllocatorMetric secondMetric = registry.getAllocatorMetric("second");
        assertThat(firstMetric).isNotSameAs(secondMetric);
        assertThat(secondMetric.usedDirectMemory()).isZero();
        assertThat(secondMetric.usedHeapMemory()).isZero();
        ByteBuf direct = first.directBuffer(128);
        ByteBuf heap = first.heapBuffer(128);
        try {
            assertThat(firstMetric.usedDirectMemory()).isGreaterThanOrEqualTo(128);
            assertThat(firstMetric.usedHeapMemory()).isGreaterThanOrEqualTo(128);
            assertThat(secondMetric.usedDirectMemory()).isZero();
            assertThat(secondMetric.usedHeapMemory()).isZero();
            var stats = new ByteBufAllocatorStats(firstMetric);
            assertThat(stats.totalAllocated).isGreaterThanOrEqualTo(128);
            if (!"pooled".equals(type)) {
                assertThat(stats.totalUsed).isEqualTo(firstMetric.usedDirectMemory());
                assertThat(stats.activeAllocations).isEqualTo(-1);
            }
        } finally {
            direct.release();
            heap.release();
        }
    }

    @Test
    public void testNamedOverridesAndGlobalFallback() {
        Properties properties = new Properties();
        properties.setProperty("pulsar.allocator.type", "unpooled");
        properties.setProperty("pulsar.allocator.default.type", "adaptive");
        properties.setProperty("pulsar.allocator.ml-cache.type", "PoOlEd");
        AllocatorRegistry registry = new AllocatorRegistry(properties::getProperty);
        assertThat(registry.getAllocatorMetric("missing")).isNull();
        assertThat(registry.getOrCreate(DEFAULT_ALLOCATOR_NAME).isDirectBufferPooled()).isTrue();
        assertThat(registry.getOrCreate(ML_CACHE_ALLOCATOR_NAME).isDirectBufferPooled()).isTrue();
        assertThat(registry.getAllocatorMetric(DEFAULT_ALLOCATOR_NAME)).isInstanceOf(AdaptiveByteBufAllocator.class);
        assertThat(registry.getAllocatorMetric(ML_CACHE_ALLOCATOR_NAME))
                .isInstanceOf(PooledByteBufAllocatorMetric.class);
        assertThat(registry.getOrCreate("other").isDirectBufferPooled()).isFalse();
        ByteBufAllocator cache = registry.getOrCreate(ML_CACHE_ALLOCATOR_NAME);
        properties.setProperty("pulsar.allocator.ml-cache.type", "invalid");
        assertThat(registry.getOrCreate(ML_CACHE_ALLOCATOR_NAME)).isSameAs(cache);
        properties.setProperty("pulsar.allocator.broken.type", "invalid");
        assertThatThrownBy(() -> registry.getOrCreate("broken")).isInstanceOf(IllegalArgumentException.class);
        assertThat(registry.getAllocatorMetric("broken")).isNull();
    }

    @Test
    public void testConcurrentRegistration() throws Exception {
        AllocatorRegistry registry = new AllocatorRegistry(key -> null);
        var executor = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<ByteBufAllocator>> results = new ArrayList<>();
            for (int i = 0; i < 8; i++) {
                results.add(executor.submit(() -> {
                    start.await();
                    return registry.getOrCreate("concurrent");
                }));
            }
            start.countDown();
            ByteBufAllocator allocator = results.get(0).get(10, TimeUnit.SECONDS);
            for (Future<ByteBufAllocator> result : results) {
                assertThat(result.get(10, TimeUnit.SECONDS)).isSameAs(allocator);
            }
        } finally {
            start.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    public void testNamedOutOfMemoryPolicy() {
        assertThat(PulsarByteBufAllocator.DEFAULT).isNotNull();
        Properties properties = new Properties();
        properties.setProperty("pulsar.allocator.out_of_memory_policy", "ThrowException");
        properties.setProperty("pulsar.allocator.ml-cache.out_of_memory_policy", "FallbackToHeap");
        try (MockedConstruction<ByteBufAllocatorImpl> mocked = Mockito.mockConstruction(ByteBufAllocatorImpl.class,
                (allocator, context) -> assertThat(context.arguments().get(4))
                        .isEqualTo(context.getCount() == 1 ? OutOfMemoryPolicy.FallbackToHeap
                                : OutOfMemoryPolicy.ThrowException))) {
            AllocatorRegistry registry = new AllocatorRegistry(properties::getProperty);
            registry.getOrCreate(ML_CACHE_ALLOCATOR_NAME);
            registry.getOrCreate(DEFAULT_ALLOCATOR_NAME);
            assertThat(mocked.constructed()).hasSize(2);
        }
    }

    @Test
    public void testNamedExitOnOutOfMemory() {
        assertThat(PulsarByteBufAllocator.DEFAULT).isSameAs(PulsarByteBufAllocator.getOrCreate(DEFAULT_ALLOCATOR_NAME));
        Properties properties = new Properties();
        properties.setProperty("pulsar.allocator.type", "adaptive");
        properties.setProperty("pulsar.allocator.exit_on_oom", "true");
        properties.setProperty("pulsar.allocator.ml-cache.exit_on_oom", "false");
        OutOfMemoryError failure = new OutOfMemoryError("test allocation failure");
        try (MockedStatic<ShutdownUtil> shutdown = Mockito.mockStatic(ShutdownUtil.class);
             MockedConstruction<AdaptiveByteBufAllocator> mocked = Mockito.mockConstruction(
                     AdaptiveByteBufAllocator.class, (allocator, context) -> Mockito.when(
                             allocator.directBuffer(Mockito.anyInt(), Mockito.anyInt())).thenThrow(failure))) {
            AllocatorRegistry registry = new AllocatorRegistry(properties::getProperty);
            ByteBufAllocator cache = registry.getOrCreate(ML_CACHE_ALLOCATOR_NAME);
            assertThatThrownBy(() -> cache.directBuffer(128)).isSameAs(failure);
            shutdown.verifyNoInteractions();
            ByteBufAllocator defaultAllocator = registry.getOrCreate(DEFAULT_ALLOCATOR_NAME);
            assertThatThrownBy(() -> defaultAllocator.directBuffer(128)).isSameAs(failure);
            shutdown.verify(ShutdownUtil::triggerImmediateForcefulShutdown);
            assertThat(mocked.constructed()).hasSize(2);
        }
    }

    @Test
    public void testAllocatorCreationPreservesGlobalLeakDetection() {
        ResourceLeakDetector.Level previous = ResourceLeakDetector.getLevel();
        try {
            ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
            Properties properties = new Properties();
            properties.setProperty("pulsar.allocator.leak_detection", "disabled");
            properties.setProperty("pulsar.allocator.ml-cache.leak_detection", "disabled");
            new AllocatorRegistry(properties::getProperty).getOrCreate(ML_CACHE_ALLOCATOR_NAME);
            assertThat(ResourceLeakDetector.getLevel()).isEqualTo(ResourceLeakDetector.Level.PARANOID);
        } finally {
            ResourceLeakDetector.setLevel(previous);
        }
    }
}
