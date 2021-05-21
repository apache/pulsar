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

import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logger for the JVM G1 GC metrics.
 */
public class JvmG1GCMetricsLogger implements JvmGCMetricsLogger {

    private volatile long accumulatedYoungGcCount = 0;
    private volatile long currentYoungGcCount = 0;
    private volatile long accumulatedYoungGcTime = 0;
    private volatile long currentYoungGcTime = 0;

    private volatile long accumulatedOldGcCount = 0;
    private volatile long currentOldGcCount = 0;
    private volatile long accumulatedOldGcTime = 0;
    private volatile long currentOldGcTime = 0;

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

    @Override
    public void logMetrics(Metrics metrics) {
        metrics.put("jvm_gc_young_pause", currentYoungGcTime);
        metrics.put("jvm_gc_young_count", currentYoungGcCount);
        metrics.put("jvm_gc_old_pause", currentOldGcTime);
        metrics.put("jvm_gc_old_count", currentOldGcCount);
    }

    @Override
    public void refresh() {
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

    private static final Logger log = LoggerFactory.getLogger(JvmG1GCMetricsLogger.class);
}
