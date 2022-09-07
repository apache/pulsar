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
package org.apache.pulsar.broker.stats.prometheus;

import static org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsGeneratorUtils.generateSystemMetrics;
import static org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsGeneratorUtils.getTypeStr;
import static org.apache.pulsar.common.stats.JvmMetrics.getJvmDirectMemoryUsed;
import io.netty.buffer.ByteBuf;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.Gauge.Child;
import io.prometheus.client.hotspot.DefaultExports;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.bookkeeper.stats.NullStatsProvider;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.pulsar.PulsarVersion;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.stats.metrics.ManagedCursorMetrics;
import org.apache.pulsar.broker.stats.metrics.ManagedLedgerCacheMetrics;
import org.apache.pulsar.broker.stats.metrics.ManagedLedgerMetrics;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.common.util.SimpleTextOutputStream;

/**
 * Generate metrics aggregated at the namespace level and optionally at a topic level and formats them out
 * in a text format suitable to be consumed by Prometheus.
 * Format specification can be found at <a
 * href="https://prometheus.io/docs/instrumenting/exposition_formats/">Exposition Formats</a>
 */
public class PrometheusMetricsGenerator {

    static {
        DefaultExports.initialize();

        Gauge.build("jvm_memory_direct_bytes_used", "-").create().setChild(new Child() {
            @Override
            public double get() {
                return getJvmDirectMemoryUsed();
            }
        }).register(CollectorRegistry.defaultRegistry);

        Gauge.build("jvm_memory_direct_bytes_max", "-").create().setChild(new Child() {
            @Override
            public double get() {
                return io.netty.util.internal.PlatformDependent.maxDirectMemory();
            }
        }).register(CollectorRegistry.defaultRegistry);

        // metric to export pulsar version info
        Gauge.build("pulsar_version_info", "-")
                .labelNames("version", "commit").create()
                .setChild(new Child() {
                    @Override
                    public double get() {
                        return 1.0;
                    }
                }, PulsarVersion.getVersion(), PulsarVersion.getGitSha())
                .register(CollectorRegistry.defaultRegistry);
    }

    public static void generate(PulsarService pulsar, boolean includeTopicMetrics, boolean includeConsumerMetrics,
                                boolean includeProducerMetrics, OutputStream out) throws IOException {
        generate(pulsar, includeTopicMetrics, includeConsumerMetrics, includeProducerMetrics, false, out, null);
    }

    public static void generate(PulsarService pulsar, boolean includeTopicMetrics, boolean includeConsumerMetrics,
                                boolean includeProducerMetrics, boolean splitTopicAndPartitionIndexLabel,
                                OutputStream out) throws IOException {
        generate(pulsar, includeTopicMetrics, includeConsumerMetrics, includeProducerMetrics,
                splitTopicAndPartitionIndexLabel, out, null);
    }

    public static void generate(PulsarService pulsar, boolean includeTopicMetrics, boolean includeConsumerMetrics,
                                boolean includeProducerMetrics, boolean splitTopicAndPartitionIndexLabel,
                                OutputStream out,
                                List<PrometheusRawMetricsProvider> metricsProviders)
            throws IOException {
        ByteBuf buf = PulsarByteBufAllocator.DEFAULT.heapBuffer();
        //Used in namespace/topic and transaction aggregators as share metric names
        PrometheusMetricStreams metricStreams = new PrometheusMetricStreams();
        try {
            SimpleTextOutputStream stream = new SimpleTextOutputStream(buf);

            generateSystemMetrics(stream, pulsar.getConfiguration().getClusterName());

            NamespaceStatsAggregator.generate(pulsar, includeTopicMetrics, includeConsumerMetrics,
                    includeProducerMetrics, splitTopicAndPartitionIndexLabel, metricStreams);

            if (pulsar.getWorkerServiceOpt().isPresent()) {
                pulsar.getWorkerService().generateFunctionsStats(stream);
            }

            if (pulsar.getConfiguration().isTransactionCoordinatorEnabled()) {
                TransactionAggregator.generate(pulsar, metricStreams, includeTopicMetrics);
            }

            metricStreams.flushAllToStream(stream);

            generateBrokerBasicMetrics(pulsar, stream);

            generateManagedLedgerBookieClientMetrics(pulsar, stream);

            if (metricsProviders != null) {
                for (PrometheusRawMetricsProvider metricsProvider : metricsProviders) {
                    metricsProvider.generate(stream);
                }
            }
            out.write(buf.array(), buf.arrayOffset(), buf.readableBytes());
        } finally {
            //release all the metrics buffers
            metricStreams.releaseAll();
            buf.release();
        }
    }

    private static void generateBrokerBasicMetrics(PulsarService pulsar, SimpleTextOutputStream stream) {
        String clusterName = pulsar.getConfiguration().getClusterName();
        // generate managedLedgerCache metrics
        parseMetricsToPrometheusMetrics(new ManagedLedgerCacheMetrics(pulsar).generate(),
                clusterName, Collector.Type.GAUGE, stream);

        if (pulsar.getConfiguration().isExposeManagedLedgerMetricsInPrometheus()) {
            // generate managedLedger metrics
            parseMetricsToPrometheusMetrics(new ManagedLedgerMetrics(pulsar).generate(),
                    clusterName, Collector.Type.GAUGE, stream);
        }

        if (pulsar.getConfiguration().isExposeManagedCursorMetricsInPrometheus()) {
            // generate managedCursor metrics
            parseMetricsToPrometheusMetrics(new ManagedCursorMetrics(pulsar).generate(),
                    clusterName, Collector.Type.GAUGE, stream);
        }

        parseMetricsToPrometheusMetrics(Collections.singletonList(pulsar.getBrokerService()
                        .getPulsarStats().getBrokerOperabilityMetrics().generateConnectionMetrics()),
                clusterName, Collector.Type.GAUGE, stream);

        // generate loadBalance metrics
        parseMetricsToPrometheusMetrics(pulsar.getLoadManager().get().getLoadBalancingMetrics(),
                clusterName, Collector.Type.GAUGE, stream);
    }

    private static void parseMetricsToPrometheusMetrics(Collection<Metrics> metrics, String cluster,
                                                        Collector.Type metricType, SimpleTextOutputStream stream) {
        Set<String> names = new HashSet<>();
        for (Metrics metrics1 : metrics) {
            for (Map.Entry<String, Object> entry : metrics1.getMetrics().entrySet()) {
                String value = null;
                if (entry.getKey().contains(".")) {
                    try {
                        String key = entry.getKey();
                        int dotIndex = key.indexOf(".");
                        int nameIndex = key.substring(0, dotIndex).lastIndexOf("_");
                        if (nameIndex == -1) {
                            continue;
                        }

                        String name = key.substring(0, nameIndex);
                        value = key.substring(nameIndex + 1);
                        if (!names.contains(name)) {
                            stream.write("# TYPE ").write(name.replace("brk_", "pulsar_")).write(' ')
                                    .write(getTypeStr(metricType)).write("\n");
                            names.add(name);
                        }
                        stream.write(name.replace("brk_", "pulsar_"))
                                .write("{cluster=\"").write(cluster).write('"');
                    } catch (Exception e) {
                        continue;
                    }
                } else {


                    String name = entry.getKey();
                    if (!names.contains(name)) {
                        stream.write("# TYPE ").write(entry.getKey().replace("brk_", "pulsar_")).write(' ')
                                .write(getTypeStr(metricType)).write('\n');
                        names.add(name);
                    }
                    stream.write(name.replace("brk_", "pulsar_"))
                            .write("{cluster=\"").write(cluster).write('"');
                }

                for (Map.Entry<String, String> metric : metrics1.getDimensions().entrySet()) {
                    if (metric.getKey().isEmpty() || "cluster".equals(metric.getKey())) {
                        continue;
                    }
                    stream.write(", ").write(metric.getKey()).write("=\"").write(metric.getValue()).write('"');
                    if (value != null && !value.isEmpty()) {
                        stream.write(", ").write("quantile=\"").write(value).write('"');
                    }
                }
                stream.write("} ").write(String.valueOf(entry.getValue())).write("\n");
            }
        }
    }

    private static void generateManagedLedgerBookieClientMetrics(PulsarService pulsar, SimpleTextOutputStream stream) {
        StatsProvider statsProvider = pulsar.getManagedLedgerClientFactory().getStatsProvider();
        if (statsProvider instanceof NullStatsProvider) {
            return;
        }

        try {
            Writer writer = new StringWriter();
            statsProvider.writeAllMetrics(writer);
            stream.write(writer.toString());
        } catch (IOException e) {
            // nop
        }
    }

}
