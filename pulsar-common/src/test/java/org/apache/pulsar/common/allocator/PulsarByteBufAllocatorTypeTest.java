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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import io.netty.buffer.AdaptiveByteBufAllocator;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufAllocatorMetric;
import io.netty.buffer.ByteBufAllocatorMetricProvider;
import java.util.Properties;
import org.apache.pulsar.common.stats.JvmDefaultGCMetricsLogger;
import org.apache.pulsar.common.stats.JvmMetrics;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class PulsarByteBufAllocatorTypeTest {
    @DataProvider
    public Object[][] allocatorTypes() {
        return new Object[][] {
            {null, null, AllocatorType.POOLED},
            {null, "true", AllocatorType.POOLED},
            {null, "TrUe", AllocatorType.POOLED},
            {null, "false", AllocatorType.UNPOOLED},
            {null, "invalid", AllocatorType.UNPOOLED},
            {"pooled", "false", AllocatorType.POOLED},
            {"UnPoOlEd", "true", AllocatorType.UNPOOLED},
            {"ADAPTIVE", null, AllocatorType.ADAPTIVE},
            {"adaptive", "false", AllocatorType.ADAPTIVE},
            {"AdApTiVe", "true", AllocatorType.ADAPTIVE},
        };
    }

    @Test(dataProvider = "allocatorTypes")
    public void testAllocatorSelection(String type, String pooled, AllocatorType expected) {
        Properties properties = new Properties();
        if (type != null) {
            properties.setProperty(PulsarByteBufAllocator.PULSAR_ALLOCATOR_TYPE, type);
        }
        if (pooled != null) {
            properties.setProperty(PulsarByteBufAllocator.PULSAR_ALLOCATOR_POOLED, pooled);
        }
        assertThat(PulsarByteBufAllocator.resolveAllocatorType(properties::getProperty)).isEqualTo(expected);
        ByteBufAllocator allocator = PulsarByteBufAllocator.createByteBufAllocator(properties::getProperty);
        assertThat(allocator.isDirectBufferPooled()).isEqualTo(expected != AllocatorType.UNPOOLED);
        ByteBuf buffer = allocator.buffer(128);
        try {
            assertThat(buffer.isDirect()).isEqualTo(expected != AllocatorType.UNPOOLED);
            buffer.writeLong(123L);
            assertThat(buffer.readLong()).isEqualTo(123L);
            if (expected == AllocatorType.ADAPTIVE) {
                assertThat(buffer.alloc()).isInstanceOf(AdaptiveByteBufAllocator.class);
                ByteBufAllocatorMetric metric = ((ByteBufAllocatorMetricProvider) buffer.alloc()).metric();
                assertThat(metric.usedDirectMemory()).isGreaterThanOrEqualTo(128);
            }
        } finally {
            buffer.release();
        }
        ByteBuf heapBuffer = allocator.heapBuffer(128);
        try {
            assertThat(heapBuffer.isDirect()).isFalse();
            ByteBufAllocatorMetric metric = ((ByteBufAllocatorMetricProvider) heapBuffer.alloc()).metric();
            assertThat(metric.usedHeapMemory()).isGreaterThanOrEqualTo(128);
        } finally {
            heapBuffer.release();
        }
    }

    @Test
    public void testInvalidTypeDoesNotFallBackToLegacyProperty() {
        for (String type : new String[] {"", "invalid"}) {
            Properties properties = new Properties();
            properties.setProperty(PulsarByteBufAllocator.PULSAR_ALLOCATOR_TYPE, type);
            properties.setProperty(PulsarByteBufAllocator.PULSAR_ALLOCATOR_POOLED, "true");
            assertThatThrownBy(() -> PulsarByteBufAllocator.createByteBufAllocator(properties::getProperty))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("pulsar.allocator.type");
        }
    }

    @Test
    public void testAdaptiveOutOfMemoryPolicies() {
        // Initialize the shared allocator before mocking construction of the test allocator.
        assertThat(PulsarByteBufAllocator.DEFAULT).isNotNull();
        Properties properties = new Properties();
        properties.setProperty(PulsarByteBufAllocator.PULSAR_ALLOCATOR_TYPE, "adaptive");
        OutOfMemoryError failure = new OutOfMemoryError("test direct allocation failure");
        try (MockedConstruction<AdaptiveByteBufAllocator> mocked = Mockito.mockConstruction(
                AdaptiveByteBufAllocator.class, (allocator, context) ->
                        Mockito.when(allocator.directBuffer(Mockito.anyInt(), Mockito.anyInt())).thenThrow(failure))) {
            ByteBufAllocator allocator = PulsarByteBufAllocator.createByteBufAllocator(properties::getProperty);
            assertThat(mocked.constructed()).hasSize(1);
            ByteBuf fallback = allocator.buffer(128);
            try {
                assertThat(fallback.isDirect()).isFalse();
            } finally {
                fallback.release();
            }
            // Explicit direct allocation must not silently fall back to heap.
            assertThatThrownBy(() -> allocator.directBuffer(128)).isSameAs(failure);
            properties.setProperty(PulsarByteBufAllocator.PULSAR_ALLOCATOR_OUT_OF_MEMORY_POLICY, "ThrowException");
            ByteBufAllocator throwingAllocator =
                    PulsarByteBufAllocator.createByteBufAllocator(properties::getProperty);
            assertThatThrownBy(() -> throwingAllocator.buffer(128)).isSameAs(failure);
        }
    }

    @Test
    public void testAdaptiveJvmMetrics() {
        AdaptiveByteBufAllocator allocator = new AdaptiveByteBufAllocator(true);
        ByteBuf buffer = allocator.directBuffer(128);
        try (MockedStatic<PulsarByteBufAllocator> mocked = Mockito.mockStatic(PulsarByteBufAllocator.class)) {
            mocked.when(PulsarByteBufAllocator::getDefaultAllocatorMetric).thenReturn(allocator.metric());
            var metrics = new JvmMetrics(null, "test", new JvmDefaultGCMetricsLogger()).generate().get(0).getMetrics();
            assertThat(metrics.get("test_default_pool_allocated")).isEqualTo(allocator.usedDirectMemory());
            assertThat(metrics.get("test_default_pool_used")).isEqualTo(allocator.usedDirectMemory());
        } finally {
            buffer.release();
        }
    }
}
