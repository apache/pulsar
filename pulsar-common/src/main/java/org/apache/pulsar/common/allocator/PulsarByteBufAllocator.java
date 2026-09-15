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

import com.google.common.annotations.VisibleForTesting;
import io.netty.buffer.AdaptiveByteBufAllocator;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufAllocatorMetric;
import io.netty.buffer.ByteBufAllocatorMetricProvider;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.CustomLog;
import lombok.experimental.UtilityClass;
import org.apache.bookkeeper.common.allocator.ByteBufAllocatorBuilder;
import org.apache.bookkeeper.common.allocator.LeakDetectionPolicy;
import org.apache.bookkeeper.common.allocator.OutOfMemoryPolicy;
import org.apache.bookkeeper.common.allocator.PoolingPolicy;
import org.apache.pulsar.common.util.ShutdownUtil;

/**
 * Holder of a ByteBuf allocator.
 *
 * <p>Set {@code pulsar.allocator.type} to {@code pooled}, {@code unpooled}, or {@code adaptive}
 * (case-insensitive). When unset, the deprecated {@code pulsar.allocator.pooled} property is honored,
 * defaulting to pooled allocation. All types retain the configured OOM and leak detection policies.
 */
@CustomLog
@UtilityClass
public class PulsarByteBufAllocator {

    public static final String PULSAR_ALLOCATOR_TYPE = "pulsar.allocator.type";
    /**
     * @deprecated Use {@link #PULSAR_ALLOCATOR_TYPE}. Ignored when the new property is set.
     */
    @Deprecated
    public static final String PULSAR_ALLOCATOR_POOLED = "pulsar.allocator.pooled";
    public static final String PULSAR_ALLOCATOR_EXIT_ON_OOM = "pulsar.allocator.exit_on_oom";
    public static final String PULSAR_ALLOCATOR_LEAK_DETECTION = "pulsar.allocator.leak_detection";
    public static final String PULSAR_ALLOCATOR_OUT_OF_MEMORY_POLICY = "pulsar.allocator.out_of_memory_policy";

    // the highest level of leak detection policy will be used when it is set by any of the following property names
    private static final String[] LEAK_DETECTION_PROPERTY_NAMES = {
            PULSAR_ALLOCATOR_LEAK_DETECTION,
            "io.netty.leakDetection.level", // io.netty.util.ResourceLeakDetector.PROP_LEVEL
            "io.netty.leakDetectionLevel" // io.netty.util.ResourceLeakDetector.PROP_LEVEL_OLD
    };

    public static final ByteBufAllocator DEFAULT;

    private static final List<Consumer<OutOfMemoryError>> LISTENERS = new CopyOnWriteArrayList<>();

    public static void registerOOMListener(Consumer<OutOfMemoryError> listener) {
        LISTENERS.add(listener);
    }

    private static final ByteBufAllocator NETTY_ALLOCATOR =
            createNettyAllocator(resolveAllocatorType(System::getProperty));

    static {
        DEFAULT = createByteBufAllocator(System::getProperty, NETTY_ALLOCATOR);
    }

    /**
     * Returns the metrics of the Netty allocator backing {@link #DEFAULT}.
     * Adaptive metrics report reserved memory; arena and live-buffer usage details are unavailable.
     */
    public static ByteBufAllocatorMetric getDefaultAllocatorMetric() {
        return ((ByteBufAllocatorMetricProvider) NETTY_ALLOCATOR).metric();
    }

    @VisibleForTesting
    static AllocatorType resolveAllocatorType(Function<String, String> propertyResolver) {
        String type = propertyResolver.apply(PULSAR_ALLOCATOR_TYPE);
        if (type != null) {
            return AllocatorType.fromString(type);
        }
        String pooled = propertyResolver.apply(PULSAR_ALLOCATOR_POOLED);
        return pooled == null || "true".equalsIgnoreCase(pooled) ? AllocatorType.POOLED : AllocatorType.UNPOOLED;
    }

    private static ByteBufAllocator createNettyAllocator(AllocatorType type) {
        return switch (type) {
            case POOLED -> PooledByteBufAllocator.DEFAULT;
            case UNPOOLED -> UnpooledByteBufAllocator.DEFAULT;
            case ADAPTIVE -> new AdaptiveByteBufAllocator(true);
        };
    }

    @VisibleForTesting
    static ByteBufAllocator createByteBufAllocator() {
        return createByteBufAllocator(System::getProperty);
    }

    @VisibleForTesting
    static ByteBufAllocator createByteBufAllocator(Function<String, String> propertyResolver) {
        return createByteBufAllocator(propertyResolver, createNettyAllocator(resolveAllocatorType(propertyResolver)));
    }

    private static ByteBufAllocator createByteBufAllocator(Function<String, String> propertyResolver,
                                                          ByteBufAllocator nettyAllocator) {
        final AllocatorType allocatorType = resolveAllocatorType(propertyResolver);
        final boolean isExitOnOutOfMemory = "true".equalsIgnoreCase(
                propertyResolver.apply(PULSAR_ALLOCATOR_EXIT_ON_OOM));
        final OutOfMemoryPolicy outOfMemoryPolicy = OutOfMemoryPolicy.valueOf(
                Objects.requireNonNullElse(propertyResolver.apply(PULSAR_ALLOCATOR_OUT_OF_MEMORY_POLICY),
                        "FallbackToHeap"));

        final LeakDetectionPolicy leakDetectionPolicy = resolveLeakDetectionPolicyWithHighestLevel(propertyResolver);
        log.debug().attr("type", allocatorType).attr("exitOnOOM", isExitOnOutOfMemory).log("Allocator configuration");

        ByteBufAllocatorBuilder builder = ByteBufAllocatorBuilder.create()
                .leakDetectionPolicy(leakDetectionPolicy)
                .pooledAllocator(nettyAllocator)
                .outOfMemoryListener(oomException -> {
                    // First notify all listeners
                    LISTENERS.forEach(c -> {
                        try {
                            c.accept(oomException);
                        } catch (Throwable t) {
                            log.warn().exception(t).log("Exception during OOM listener");
                        }
                    });

                    if (isExitOnOutOfMemory) {
                        log.info().exception(oomException).log("Exiting JVM process for OOM error");
                        ShutdownUtil.triggerImmediateForcefulShutdown();
                    }
                });

        if (allocatorType != AllocatorType.UNPOOLED) {
            builder.poolingPolicy(PoolingPolicy.PooledDirect);
        } else {
            builder.poolingPolicy(PoolingPolicy.UnpooledHeap);
        }
        builder.outOfMemoryPolicy(outOfMemoryPolicy);
        return builder.build();

    }

    /**
     * Resolve the leak detection policy. The value is resolved from the system properties in
     * the order of LEAK_DETECTION_PROPERTY_NAMES.
     * @return parsed leak detection policy
     */
    @VisibleForTesting
    static LeakDetectionPolicy resolveLeakDetectionPolicyWithHighestLevel(Function<String, String> propertyResolver) {
        return Arrays.stream(LEAK_DETECTION_PROPERTY_NAMES)
                .map(propertyResolver)
                .filter(Objects::nonNull)
                .map(LeakDetectionPolicy::parseLevel)
                .max(Comparator.comparingInt(Enum::ordinal))
                .orElse(LeakDetectionPolicy.Disabled);
    }
}
