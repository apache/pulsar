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

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.allocator.ByteBufAllocatorBuilder;
import org.apache.bookkeeper.common.allocator.LeakDetectionPolicy;
import org.apache.bookkeeper.common.allocator.OutOfMemoryPolicy;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;

/**
 * Holder of a ByteBuf allocator.
 */
@UtilityClass
@Slf4j
public class PulsarByteBufAllocator {

    public static final String PULSAR_ALLOCATOR_POOLED = "pulsar.allocator.pooled";
    public static final String PULSAR_ALLOCATOR_EXIT_ON_OOM = "pulsar.allocator.exit_on_oom";
    public static final String PULSAR_ALLOCATOR_LEAK_DETECTION = "pulsar.allocator.leak_detection";
    public static final String PULSAR_ALLOCATOR_OUT_OF_MEMORY_POLICY = "pulsar.allocator.out_of_memory_policy";

    public static final ByteBufAllocator DEFAULT;

    private static final List<Consumer<OutOfMemoryError>> LISTENERS = new CopyOnWriteArrayList<>();

    public static void registerOOMListener(Consumer<OutOfMemoryError> listener) {
        LISTENERS.add(listener);
    }

    static {
        DEFAULT = createByteBufAllocator();
    }

    @VisibleForTesting
    static ByteBufAllocator createByteBufAllocator() {
        final boolean isPooled = "true".equalsIgnoreCase(System.getProperty(PULSAR_ALLOCATOR_POOLED, "true"));
        final boolean isExitOnOutOfMemory = "true".equalsIgnoreCase(
                System.getProperty(PULSAR_ALLOCATOR_EXIT_ON_OOM, "false"));
        final OutOfMemoryPolicy outOfMemoryPolicy = OutOfMemoryPolicy.valueOf(
                System.getProperty(PULSAR_ALLOCATOR_OUT_OF_MEMORY_POLICY, "FallbackToHeap"));

        final LeakDetectionPolicy leakDetectionPolicy = LeakDetectionPolicy
                .valueOf(System.getProperty(PULSAR_ALLOCATOR_LEAK_DETECTION, "Disabled"));
        if (log.isDebugEnabled()) {
            log.debug("Is Pooled: {} -- Exit on OOM: {}", isPooled, isExitOnOutOfMemory);
        }

        ByteBufAllocatorBuilder builder = ByteBufAllocatorBuilder.create()
                .leakDetectionPolicy(leakDetectionPolicy)
                .pooledAllocator(PooledByteBufAllocator.DEFAULT)
                .outOfMemoryListener(oomException -> {
                    // First notify all listeners
                    LISTENERS.forEach(c -> {
                        try {
                            c.accept(oomException);
                        } catch (Throwable t) {
                            log.warn("Exception during OOM listener: {}", t.getMessage(), t);
                        }
                    });

                    if (isExitOnOutOfMemory) {
                        log.info("Exiting JVM process for OOM error: {}", oomException.getMessage(), oomException);
                        Runtime.getRuntime().halt(1);
                    }
                });

        if (isPooled) {
            builder.poolingPolicy(PoolingPolicy.PooledDirect);
        } else {
            builder.poolingPolicy(PoolingPolicy.UnpooledHeap);
        }
        builder.outOfMemoryPolicy(outOfMemoryPolicy);
        return builder.build();

    }
}
