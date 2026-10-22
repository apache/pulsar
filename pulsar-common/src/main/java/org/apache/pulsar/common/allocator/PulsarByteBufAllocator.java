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
import io.netty.util.ResourceLeakDetector;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
 * Registry of named ByteBuf allocators.
 *
 * <p>Settings under {@code pulsar.allocator.<id>.} override settings under {@code pulsar.allocator.}.
 * Supported settings are {@code type}, {@code exit_on_oom}, and {@code out_of_memory_policy}.
 * The default allocator has the identifier {@code default}. Each identifier owns a separate allocator.
 * Leak detection is configured globally through Netty's {@code io.netty.leakDetection.level} property.
 */
@CustomLog
@UtilityClass
public class PulsarByteBufAllocator {

    public static final String DEFAULT_ALLOCATOR_NAME = "default";
    public static final String ML_CACHE_ALLOCATOR_NAME = "ml-cache";
    private static final String PROPERTY_PREFIX = "pulsar.allocator.";

    public static final String PULSAR_ALLOCATOR_TYPE = "pulsar.allocator.type";
    /**
     * @deprecated Use {@link #PULSAR_ALLOCATOR_TYPE}. Ignored when the new property is set.
     */
    @Deprecated
    public static final String PULSAR_ALLOCATOR_POOLED = "pulsar.allocator.pooled";
    public static final String PULSAR_ALLOCATOR_EXIT_ON_OOM = "pulsar.allocator.exit_on_oom";
    public static final String PULSAR_ALLOCATOR_OUT_OF_MEMORY_POLICY = "pulsar.allocator.out_of_memory_policy";

    public static final ByteBufAllocator DEFAULT;

    private static final List<Consumer<OutOfMemoryError>> LISTENERS = new CopyOnWriteArrayList<>();

    public static void registerOOMListener(Consumer<OutOfMemoryError> listener) {
        LISTENERS.add(listener);
    }

    private static final AllocatorRegistry REGISTRY = new AllocatorRegistry(System::getProperty);

    static {
        DEFAULT = getOrCreate(DEFAULT_ALLOCATOR_NAME);
    }

    /** Returns the registered allocator, creating it atomically on first use. Settings are read once per identifier. */
    public static ByteBufAllocator getOrCreate(String id) {
        return REGISTRY.getOrCreate(id);
    }

    /** Returns the allocator's metrics, or {@code null} if the identifier has not been registered. */
    public static ByteBufAllocatorMetric getAllocatorMetric(String id) {
        return REGISTRY.getAllocatorMetric(id);
    }

    /** Returns the metrics of the allocator backing {@link #DEFAULT}. */
    public static ByteBufAllocatorMetric getDefaultAllocatorMetric() {
        return getAllocatorMetric(DEFAULT_ALLOCATOR_NAME);
    }

    @VisibleForTesting
    static final class AllocatorRegistry {
        private final ConcurrentMap<String, RegisteredAllocator> allocators = new ConcurrentHashMap<>();
        private final Function<String, String> propertyResolver;

        AllocatorRegistry(Function<String, String> propertyResolver) {
            this.propertyResolver = propertyResolver;
        }

        ByteBufAllocator getOrCreate(String id) {
            if (id == null || !id.matches("[a-zA-Z0-9_-]+")) {
                throw new IllegalArgumentException("Invalid allocator identifier: " + id);
            }
            return allocators.computeIfAbsent(id, name -> {
                Function<String, String> resolver = key -> {
                    String override = propertyResolver.apply(PROPERTY_PREFIX + name
                            + "." + key.substring(PROPERTY_PREFIX.length()));
                    return override != null ? override : propertyResolver.apply(key);
                };
                ByteBufAllocator nettyAllocator = createNettyAllocator(resolveAllocatorType(resolver, name), name);
                return new RegisteredAllocator(createByteBufAllocator(name, resolver, nettyAllocator),
                        ((ByteBufAllocatorMetricProvider) nettyAllocator).metric());
            }).allocator();
        }

        ByteBufAllocatorMetric getAllocatorMetric(String id) {
            RegisteredAllocator allocator = allocators.get(id);
            return allocator == null ? null : allocator.metric();
        }
    }

    private record RegisteredAllocator(ByteBufAllocator allocator, ByteBufAllocatorMetric metric) {
    }

    @VisibleForTesting
    static AllocatorType resolveAllocatorType(Function<String, String> propertyResolver) {
        return resolveAllocatorType(propertyResolver, DEFAULT_ALLOCATOR_NAME);
    }

    private static AllocatorType resolveAllocatorType(Function<String, String> propertyResolver, String id) {
        String type = propertyResolver.apply(PULSAR_ALLOCATOR_TYPE);
        if (type != null) {
            return AllocatorType.fromString(type);
        }
        String pooled = propertyResolver.apply(PULSAR_ALLOCATOR_POOLED);
        if (pooled != null) {
            return "true".equalsIgnoreCase(pooled) ? AllocatorType.POOLED : AllocatorType.UNPOOLED;
        }
        if (DEFAULT_ALLOCATOR_NAME.equals(id)) {
            return AllocatorType.POOLED;
        }
        // Cache copies are commonly small, similarly sized entries. Adaptive reuses freed size-class slots
        // within smaller chunks, limiting fragmentation from entries with different cache lifetimes.
        return ML_CACHE_ALLOCATOR_NAME.equals(id) ? AllocatorType.ADAPTIVE : AllocatorType.POOLED;
    }

    private static ByteBufAllocator createNettyAllocator(AllocatorType type, String id) {
        return switch (type) {
            case POOLED -> DEFAULT_ALLOCATOR_NAME.equals(id)
                    ? PooledByteBufAllocator.DEFAULT : new PooledByteBufAllocator(true);
            case UNPOOLED -> DEFAULT_ALLOCATOR_NAME.equals(id)
                    ? UnpooledByteBufAllocator.DEFAULT : new UnpooledByteBufAllocator(false);
            case ADAPTIVE -> new AdaptiveByteBufAllocator();
        };
    }

    @VisibleForTesting
    static ByteBufAllocator createByteBufAllocator() {
        return new AllocatorRegistry(System::getProperty).getOrCreate(DEFAULT_ALLOCATOR_NAME);
    }

    @VisibleForTesting
    static ByteBufAllocator createByteBufAllocator(Function<String, String> propertyResolver) {
        return new AllocatorRegistry(propertyResolver).getOrCreate(DEFAULT_ALLOCATOR_NAME);
    }

    private static ByteBufAllocator createByteBufAllocator(String id, Function<String, String> propertyResolver,
                                                          ByteBufAllocator nettyAllocator) {
        final AllocatorType allocatorType = resolveAllocatorType(propertyResolver, id);
        final boolean isExitOnOutOfMemory = "true".equalsIgnoreCase(
                propertyResolver.apply(PULSAR_ALLOCATOR_EXIT_ON_OOM));
        final OutOfMemoryPolicy outOfMemoryPolicy = OutOfMemoryPolicy.valueOf(
                Objects.requireNonNullElse(propertyResolver.apply(PULSAR_ALLOCATOR_OUT_OF_MEMORY_POLICY),
                        "FallbackToHeap"));

        // BookKeeper's builder sets Netty's global level. Preserve the current level instead of configuring it per ID.
        final LeakDetectionPolicy leakDetectionPolicy =
                LeakDetectionPolicy.parseLevel(ResourceLeakDetector.getLevel().name());
        log.debug().attr("allocator", id).attr("type", allocatorType).attr("exitOnOOM", isExitOnOutOfMemory)
                .log("Allocator configuration");

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
            builder.poolingPolicy(PoolingPolicy.UnpooledHeap).unpooledAllocator(nettyAllocator);
        }
        builder.outOfMemoryPolicy(outOfMemoryPolicy);
        return builder.build();
    }
}
