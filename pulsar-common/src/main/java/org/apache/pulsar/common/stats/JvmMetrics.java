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
package org.apache.pulsar.common.stats;

import static org.apache.pulsar.common.util.Runnables.catchingAndLoggingThrowables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.netty.buffer.PoolArenaMetric;
import io.netty.buffer.PoolChunkListMetric;
import io.netty.buffer.PoolChunkMetric;
import io.netty.buffer.PooledByteBufAllocator;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for providing JVM metrics.
 */
public class JvmMetrics {

    private static final Logger log = LoggerFactory.getLogger(JvmMetrics.class);
    private static Field directMemoryUsage = null;
    private final JvmGCMetricsLogger gcLogger;

    private final String componentName;
    private static final Map<String, Class<? extends JvmGCMetricsLogger>> gcLoggerMap = new HashMap<>();
    static {
        try {
            directMemoryUsage = io.netty.util.internal.PlatformDependent.class
                .getDeclaredField("DIRECT_MEMORY_COUNTER");
            directMemoryUsage.setAccessible(true);
        } catch (Exception e) {
            log.warn("Failed to access netty DIRECT_MEMORY_COUNTER field {}", e.getMessage());
        }
        // GC type and implementation mapping
        gcLoggerMap.put("-XX:+UseG1GC", JvmG1GCMetricsLogger.class);
    }

    public static JvmMetrics create(ScheduledExecutorService executor, String componentName,
            String jvmGCMetricsLoggerClassName) {
        String gcLoggerImplClassName = StringUtils.isNotBlank(jvmGCMetricsLoggerClassName) ? jvmGCMetricsLoggerClassName
                : detectGCType();
        JvmGCMetricsLogger gcLoggerImpl = null;
        if (StringUtils.isNotBlank(gcLoggerImplClassName)) {
            try {
                gcLoggerImpl = (JvmGCMetricsLogger) Class.forName(gcLoggerImplClassName)
                        .getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                log.error("Failed to initialize jvmGCMetricsLogger {} due to {}", jvmGCMetricsLoggerClassName,
                        e.getMessage(), e);
            }
        }
        return new JvmMetrics(executor, componentName,
                gcLoggerImpl != null ? gcLoggerImpl : new JvmDefaultGCMetricsLogger());
    }

    private static String detectGCType() {
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
        Set<String> arguments = Sets.newHashSet(runtimeMxBean.getInputArguments());
        for (Entry<String, Class<? extends JvmGCMetricsLogger>> gc : gcLoggerMap.entrySet()) {
            if (arguments.contains(gc.getKey())) {
                return gc.getValue().getName();
            }
        }
        return null;
    }

    public JvmMetrics(ScheduledExecutorService executor, String componentName, JvmGCMetricsLogger gcLogger) {
        this.gcLogger = gcLogger;
        if (executor != null) {
            executor.scheduleAtFixedRate(catchingAndLoggingThrowables(gcLogger::refresh), 0, 1, TimeUnit.MINUTES);
        }
        this.componentName = componentName;
    }

    public List<Metrics> generate() {

        Metrics m = createMetrics();

        Runtime r = Runtime.getRuntime();

        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();

        m.put("jvm_start_time", runtimeMXBean.getStartTime());
        m.put("jvm_heap_used", r.totalMemory() - r.freeMemory());
        m.put("jvm_max_memory", r.maxMemory());
        m.put("jvm_total_memory", r.totalMemory());

        m.put("jvm_direct_memory_used", getJvmDirectMemoryUsed());
        m.put("jvm_max_direct_memory", io.netty.util.internal.PlatformDependent.maxDirectMemory());
        m.put("jvm_thread_cnt", getThreadCount());

        this.gcLogger.logMetrics(m);

        long totalAllocated = 0;
        long totalUsed = 0;

        for (PoolArenaMetric arena : PooledByteBufAllocator.DEFAULT.metric().directArenas()) {
            this.gcLogger.logMetrics(m);
            for (PoolChunkListMetric list : arena.chunkLists()) {
                for (PoolChunkMetric chunk : list) {
                    int size = chunk.chunkSize();
                    int used = size - chunk.freeBytes();

                    totalAllocated += size;
                    totalUsed += used;
                }
            }
        }

        m.put(this.componentName + "_default_pool_allocated", totalAllocated);
        m.put(this.componentName + "_default_pool_used", totalUsed);

        this.gcLogger.logMetrics(m);

        return Lists.newArrayList(m);
    }

    public static long getJvmDirectMemoryUsed() {
        if (directMemoryUsage != null) {
            try {
                return ((AtomicLong) directMemoryUsage.get(null)).get();
            } catch (Exception e) {
                if (log.isDebugEnabled()) {
                    log.debug("Failed to get netty-direct-memory used count {}", e.getMessage());
                }
            }
        }

        List<BufferPoolMXBean> pools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
        for (BufferPoolMXBean pool : pools) {
            if (pool.getName().equals("direct")) {
                return pool.getMemoryUsed();
            }
        }

        // Couldnt get direct memory usage
        return -1;
    }

    private long getThreadCount() {
        // get top level thread group to track active thread count
        ThreadGroup parentThreadGroup = Thread.currentThread().getThreadGroup();

        while (parentThreadGroup.getParent() != null) {
            parentThreadGroup = parentThreadGroup.getParent();
        }

        return parentThreadGroup.activeCount();
    }

    private Metrics createMetrics() {
        return createMetrics(Collections.singletonMap("metric", "jvm_metrics"));
    }

    private Metrics createMetrics(Map<String, String> dimensionMap) {
        // create with current version
        return Metrics.create(dimensionMap);
    }

}
