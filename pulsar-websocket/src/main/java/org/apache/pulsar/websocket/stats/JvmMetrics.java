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
package org.apache.pulsar.websocket.stats;

import static org.apache.pulsar.common.stats.Metrics.create;
import static org.apache.pulsar.common.util.Runnables.catchingAndLoggingThrowables;

import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.websocket.WebSocketService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import io.netty.util.internal.PlatformDependent;

public class JvmMetrics {

    private volatile long accumulatedYoungGcCount = 0;
    private volatile long currentYoungGcCount = 0;
    private volatile long accumulatedYoungGcTime = 0;
    private volatile long currentYoungGcTime = 0;

    private volatile long accumulatedOldGcCount = 0;
    private volatile long currentOldGcCount = 0;
    private volatile long accumulatedOldGcTime = 0;
    private volatile long currentOldGcTime = 0;

    private static final Logger log = LoggerFactory.getLogger(JvmMetrics.class);

    public JvmMetrics(WebSocketService service) {
        service.getExecutor()
                .scheduleAtFixedRate(catchingAndLoggingThrowables(this::updateGcStats), 0, 1, TimeUnit.MINUTES);
    }

    public Metrics generate() {
        Map<String, String> dimensionMap = Maps.newHashMap();
        dimensionMap.put("system", "jvm");
        Metrics m = create(dimensionMap);

        Runtime r = Runtime.getRuntime();

        m.put("jvm_heap_used", r.totalMemory() - r.freeMemory());
        m.put("jvm_max_memory", r.maxMemory());
        m.put("jvm_total_memory", r.totalMemory());

        m.put("jvm_max_direct_memory", PlatformDependent.maxDirectMemory());
        m.put("jvm_thread_cnt", getThreadCount());

        m.put("jvm_gc_young_pause", currentYoungGcTime);
        m.put("jvm_gc_young_count", currentYoungGcCount);
        m.put("jvm_gc_old_pause", currentOldGcTime);
        m.put("jvm_gc_old_count", currentOldGcCount);

        return m;
    }

    private static ObjectName youngGenName = null;
    private static ObjectName oldGenName = null;

    static {
        try {
            youngGenName = new ObjectName("java.lang:type=GarbageCollector,name=G1 Young Generation");
            oldGenName = new ObjectName("java.lang:type=GarbageCollector,name=G1 Old Generation");
        } catch (MalformedObjectNameException e) {
            // Ok, no G1GC used
        }
    }

    private void updateGcStats() {
        MBeanServer s = ManagementFactory.getPlatformMBeanServer();

        try {
            long newValueYoungGcCount = (Long) s.getAttribute(youngGenName, "CollectionCount");
            long newValueYoungGcTime = (Long) s.getAttribute(youngGenName, "CollectionTime");

            currentYoungGcCount = newValueYoungGcCount - accumulatedYoungGcCount;
            currentYoungGcTime = newValueYoungGcTime - accumulatedYoungGcTime;

            accumulatedYoungGcCount = newValueYoungGcCount;
            accumulatedYoungGcTime = newValueYoungGcTime;

            long newValueOldGcCount = (Long) s.getAttribute(oldGenName, "CollectionCount");
            long newValueOldGcTime = (Long) s.getAttribute(oldGenName, "CollectionTime");

            currentOldGcCount = newValueOldGcCount - accumulatedOldGcCount;
            currentOldGcTime = newValueOldGcTime - accumulatedOldGcTime;

            accumulatedOldGcCount = newValueOldGcCount;
            accumulatedOldGcTime = newValueOldGcTime;
        } catch (Exception e) {
            log.error("Failed to collect GC stats: {}", e.getMessage());
        }
    }

    private long getThreadCount() {
        // get top level thread group to track active thread count
        ThreadGroup parentThreadGroup = Thread.currentThread().getThreadGroup();

        while (parentThreadGroup.getParent() != null) {
            parentThreadGroup = parentThreadGroup.getParent();
        }

        return parentThreadGroup.activeCount();
    }

}
