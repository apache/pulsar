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
package org.apache.pulsar.broker.stats.prometheus;

import io.netty.buffer.ByteBuf;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.common.util.SimpleTextOutputStream;
import org.apache.pulsar.common.util.ThreadMonitor;
import org.apache.pulsar.common.util.ThreadPoolMonitor;

/**
 * Generate metrics in a text format suitable to be consumed by Prometheus.
 * Format specification can be found at {@link https://prometheus.io/docs/instrumenting/exposition_formats/}
 */
public class PrometheusMetricsGeneratorUtils {

    public static void generate(String cluster, OutputStream out,
                                List<PrometheusRawMetricsProvider> metricsProviders)
            throws IOException {
        ByteBuf buf = PulsarByteBufAllocator.DEFAULT.heapBuffer();
        try {
            SimpleTextOutputStream stream = new SimpleTextOutputStream(buf);
            generateSystemMetrics(stream, cluster);
            if (metricsProviders != null) {
                for (PrometheusRawMetricsProvider metricsProvider : metricsProviders) {
                    metricsProvider.generate(stream);
                }
            }
            out.write(buf.array(), buf.arrayOffset(), buf.readableBytes());
        } finally {
            buf.release();
        }
    }

    public static void generateSystemMetrics(SimpleTextOutputStream stream, String cluster) {
        Enumeration<Collector.MetricFamilySamples> metricFamilySamples =
                CollectorRegistry.defaultRegistry.metricFamilySamples();
        while (metricFamilySamples.hasMoreElements()) {
            Collector.MetricFamilySamples metricFamily = metricFamilySamples.nextElement();

            // Write type of metric
            stream.write("# TYPE ").write(metricFamily.name).write(getTypeNameSuffix(metricFamily.type)).write(' ')
                    .write(getTypeStr(metricFamily.type)).write('\n');

            for (int i = 0; i < metricFamily.samples.size(); i++) {
                Collector.MetricFamilySamples.Sample sample = metricFamily.samples.get(i);
                stream.write(sample.name);
                stream.write("{");
                if (!sample.labelNames.contains("cluster")) {
                    stream.write("cluster=\"").write(cluster).write('"');
                    // If label is empty, should not append ','.
                    if (!CollectionUtils.isEmpty(sample.labelNames)) {
                        stream.write(",");
                    }
                }
                for (int j = 0; j < sample.labelNames.size(); j++) {
                    String labelValue = sample.labelValues.get(j);
                    if (labelValue != null) {
                        labelValue = labelValue.replace("\"", "\\\"");
                    }
                    if (j > 0) {
                        stream.write(",");
                    }
                    stream.write(sample.labelNames.get(j));
                    stream.write("=\"");
                    stream.write(labelValue);
                    stream.write('"');
                }

                stream.write("} ");
                stream.write(Collector.doubleToGoString(sample.value));
                stream.write('\n');
            }
        }
    }

    public static final String THREAD_BLOCK_COUNTS = "thread_blocked_count";
    public static final String THREAD_BLOCK_TIME_MS = "thread_blocked_time_ms";
    public static final String THREAD_WAIT_COUNTS = "thread_waited_count";
    public static final String THREAD_WAIT_TIME_MS = "thread_waited_time_ms";

    public static List<Metrics> generateThreadPoolMonitorMetrics(String cluster) {
        List<Metrics> metrics = new ArrayList<>();

        Map<Long, ThreadMonitor.ThreadMonitorState> threadStat = new HashMap<>(ThreadMonitor.THREAD_ID_TO_STATE.size());
        threadStat.putAll(ThreadMonitor.THREAD_ID_TO_STATE);

        threadStat.forEach((tid, threadMonitorState) -> {
            Map<String, String> dimensionMap = new HashMap<>();
            dimensionMap.put("cluster", cluster);
            dimensionMap.put("threadName", threadMonitorState.getName());
            dimensionMap.put("tid", String.valueOf(tid));
            dimensionMap.put("runningTask", Boolean.toString(threadMonitorState.isRunningTask()));
            Metrics m = Metrics.create(dimensionMap);
            m.put(ThreadMonitor.THREAD_ACTIVE_TIMESTAMP_GAUGE_NAME, threadMonitorState.getLastActiveTimestamp());

            metrics.add(m);
        });

        Map<String, String> dimensionMap = new HashMap<>();
        dimensionMap.put("cluster", cluster);

        Metrics m = Metrics.create(dimensionMap);
        m.put(ThreadPoolMonitor.THREAD_POOL_MONITOR_ENABLED_GAUGE_NAME,
                ThreadPoolMonitor.isEnabled() ? 1 : 0);
        m.put(ThreadPoolMonitor.THREAD_POOL_MONITOR_CHECK_INTERVAL_MS_GAUGE_NAME,
                ThreadPoolMonitor.checkIntervalMs());
        m.put(ThreadPoolMonitor.THREAD_POOL_MONITOR_REGISTERED_POOL_NUMBER_GAUGE_NAME,
                ThreadPoolMonitor.registeredThreadPool());
        m.put(ThreadPoolMonitor.THREAD_POOL_MONITOR_SUBMITTED_POOL_NUMBER_GAUGE_NAME,
                ThreadPoolMonitor.submittedThreadPool());

        metrics.add(m);

        ThreadMXBean threadMXBean = ThreadPoolMonitor.THREAD_MX_BEAN;
        if (threadMXBean.isThreadContentionMonitoringSupported()) {
            long[] allThreadIds = threadMXBean.getAllThreadIds();
            ThreadInfo[] threadInfo = threadMXBean.getThreadInfo(allThreadIds, 0);
            for (ThreadInfo info : threadInfo) {
                if (info != null) {
                    Map<String, String> dimensions = new HashMap<>();
                    dimensions.put("cluster", cluster);
                    dimensions.put("threadName", info.getThreadName());
                    dimensions.put("tid", String.valueOf(info.getThreadId()));

                    Metrics threadMetric = Metrics.create(dimensions);
                    threadMetric.put(THREAD_BLOCK_COUNTS, info.getBlockedCount());
                    threadMetric.put(THREAD_BLOCK_TIME_MS, info.getBlockedTime());
                    threadMetric.put(THREAD_WAIT_COUNTS, info.getWaitedCount());
                    threadMetric.put(THREAD_WAIT_TIME_MS, info.getWaitedTime());

                    metrics.add(threadMetric);
                }
            }
        }

        return metrics;
    }


    static String getTypeNameSuffix(Collector.Type type) {
        if (type.equals(Collector.Type.INFO)) {
            return "_info";
        }
        return "";
    }

    static String getTypeStr(Collector.Type type) {
        switch (type) {
            case COUNTER:
                return "counter";
            case GAUGE:
            case INFO:
                return "gauge";
            case SUMMARY:
                return "summary";
            case HISTOGRAM:
                return "histogram";
            case UNKNOWN:
            default:
                return "unknown";
        }
    }

}

