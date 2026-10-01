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
package org.apache.pulsar.broker.stats;

import static org.apache.pulsar.common.allocator.PulsarByteBufAllocator.DEFAULT_ALLOCATOR_NAME;
import static org.apache.pulsar.common.allocator.PulsarByteBufAllocator.ML_CACHE_ALLOCATOR_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import io.netty.buffer.AdaptiveByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.stats.AllocatorStats;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class AllocatorStatsGeneratorTest {
    @Test
    public void testUnknownAllocatorIsNotCreated() {
        assertThatThrownBy(() -> AllocatorStatsGenerator.generate("unknown-allocator"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("unknown-allocator");
    }

    @Test
    public void testRegisteredCacheAllocatorMetrics() {
        var allocator = PulsarByteBufAllocator.getOrCreate(ML_CACHE_ALLOCATOR_NAME);
        ByteBuf buffer = allocator.directBuffer(128);
        try {
            var metric = PulsarByteBufAllocator.getAllocatorMetric(ML_CACHE_ALLOCATOR_NAME);
            AllocatorStats stats = AllocatorStatsGenerator.generate(ML_CACHE_ALLOCATOR_NAME);
            assertThat(stats.usedDirectMemory).isEqualTo(metric.usedDirectMemory()).isGreaterThanOrEqualTo(128);
            assertThat(metric).isNotSameAs(PulsarByteBufAllocator.getDefaultAllocatorMetric());
        } finally {
            buffer.release();
        }
    }

    @Test
    public void testAdaptiveMetrics() {
        AdaptiveByteBufAllocator allocator = new AdaptiveByteBufAllocator(true);
        ByteBuf direct = allocator.directBuffer(128);
        ByteBuf heap = allocator.heapBuffer(128);
        try (MockedStatic<PulsarByteBufAllocator> mocked = Mockito.mockStatic(PulsarByteBufAllocator.class)) {
            mocked.when(() -> PulsarByteBufAllocator.getAllocatorMetric(DEFAULT_ALLOCATOR_NAME))
                    .thenReturn(allocator.metric());
            AllocatorStats stats = AllocatorStatsGenerator.generate(DEFAULT_ALLOCATOR_NAME);
            assertThat(stats.usedDirectMemory).isEqualTo(allocator.usedDirectMemory()).isGreaterThanOrEqualTo(128);
            assertThat(stats.usedHeapMemory).isEqualTo(allocator.usedHeapMemory()).isGreaterThanOrEqualTo(128);
            assertThat(stats.directArenas).isEmpty();
            assertThat(stats.heapArenas).isEmpty();
        } finally {
            direct.release();
            heap.release();
        }
    }

    @Test
    public void testPooledMetrics() {
        var metric = PooledByteBufAllocator.DEFAULT.metric();
        AllocatorStats stats = AllocatorStatsGenerator.generate(metric);
        assertThat(stats.numDirectArenas).isEqualTo(metric.numDirectArenas());
        assertThat(stats.directArenas).hasSize(metric.numDirectArenas());
        assertThat(stats.heapArenas).hasSize(metric.numHeapArenas());
        assertThat(stats.usedDirectMemory).isEqualTo(metric.usedDirectMemory());
        assertThat(stats.usedHeapMemory).isEqualTo(metric.usedHeapMemory());
    }
}
