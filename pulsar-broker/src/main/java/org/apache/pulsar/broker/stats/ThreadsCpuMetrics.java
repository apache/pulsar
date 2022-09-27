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
package org.apache.pulsar.broker.stats;

import io.prometheus.client.Counter;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

public final class ThreadsCpuMetrics {
    private static boolean supportCpuTime = true;

    private static final Counter THREAD_TOTAL_CPU_USAGE = Counter.build()
            .name("pulsar_broker_thread_total_cpu_usage")
            .help("-")
            .labelNames("thread_name")
            .unit("ns")
            .register();
    private static final Counter THREAD_SYSTEM_CPU_USAGE = Counter.build()
            .name("pulsar_broker_thread_system_cpu_usage")
            .help("-")
            .labelNames("thread_name")
            .unit("ns")
            .register();
    private static final Counter THREAD_USER_CPU_USAGE = Counter.build()
            .name("pulsar_broker_thread_user_cpu_usage")
            .help("-")
            .labelNames("thread_name")
            .unit("ns")
            .register();

    private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();

    private final long[] threadIds;

    public static void initialize() {
        if (!THREAD_MX_BEAN.isThreadCpuTimeSupported()) {
            supportCpuTime = false;
            return;
        }

        if (!THREAD_MX_BEAN.isThreadCpuTimeEnabled()) {
            THREAD_MX_BEAN.setThreadCpuTimeEnabled(true);
        }
    }

    public ThreadsCpuMetrics() {
        this.threadIds = THREAD_MX_BEAN.getAllThreadIds();
        THREAD_TOTAL_CPU_USAGE.clear();
        THREAD_USER_CPU_USAGE.clear();
        THREAD_SYSTEM_CPU_USAGE.clear();
    }

    /**
     * Must call the method before PrometheusMetricsGeneratorUtils#generateSystemMetrics.
     */
    public void generate() {
        if (!supportCpuTime) {
            return;
        }

        // No stack trace
        ThreadInfo[] threadInfos = THREAD_MX_BEAN.getThreadInfo(threadIds);
        for (ThreadInfo info : threadInfos) {
            long threadId = info.getThreadId();
            long userNs = THREAD_MX_BEAN.getThreadUserTime(threadId);
            long totalNs = THREAD_MX_BEAN.getThreadCpuTime(threadId);
            long systemNs = totalNs - userNs;
            String threadName = info.getThreadName();
            int idx = threadName.indexOf("{");
            if (idx > 0) {
                threadName = threadName.substring(0, idx);
            }

            THREAD_TOTAL_CPU_USAGE.labels(threadName).inc(totalNs);
            THREAD_SYSTEM_CPU_USAGE.labels(threadName).inc(systemNs);
            THREAD_USER_CPU_USAGE.labels(threadName).inc(userNs);
        }
    }
}
