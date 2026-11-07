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
import io.prometheus.metrics.config.EscapingScheme;
import io.prometheus.metrics.expositionformats.PrometheusTextFormatWriter;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.CustomLog;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.util.SimpleTextOutputStream;

/**
 * Generate metrics in a text format suitable to be consumed by Prometheus.
 * Format specification can be found at <a href="https://prometheus.io/docs/instrumenting/exposition_formats/">exposition formats</a>
 */
@CustomLog
public class PrometheusMetricsGeneratorUtils {

    private static final PrometheusTextFormatWriter TEXT_WRITER = PrometheusTextFormatWriter.builder()
            .setIncludeCreatedTimestamps(false).build();
    /** Families already reported as registered in both registries; each one is logged once per process. */
    private static final Set<String> REPORTED_DUPLICATES = ConcurrentHashMap.newKeySet();

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

    /**
     * The only migration boundary: keep the legacy exposition until its last collector is ported,
     * then remove the legacy writer call and its simpleclient dependency. Both registries are read
     * during migration and written one after the other, so a family registered in both would be
     * declared twice; that is a registration bug and is logged, not merged.
     */
    public static void generateSystemMetrics(SimpleTextOutputStream stream, String cluster) {
        generateSystemMetrics(stream, cluster, CollectorRegistry.defaultRegistry, PrometheusRegistry.defaultRegistry);
    }

    static void generateSystemMetrics(SimpleTextOutputStream stream, String cluster,
                                      CollectorRegistry legacyRegistry, PrometheusRegistry registry) {
        Set<String> legacyNames = LegacyPrometheusMetricsWriter.write(stream, cluster, legacyRegistry);
        MetricSnapshots snapshots = registry.scrape();
        if (snapshots.size() == 0) {
            return;
        }
        if (!legacyNames.isEmpty()) {
            for (MetricSnapshot snapshot : snapshots) {
                String name = snapshot.getMetadata().getPrometheusName();
                if (legacyNames.contains(name) && REPORTED_DUPLICATES.add(name)) {
                    log.warn().attr("metric", name)
                            .log("Metric family is registered in both Prometheus client registries;"
                                    + " /metrics declares it twice. Register it in one registry only");
                }
            }
        }
        try {
            TEXT_WRITER.write(new PulsarPrometheusTextOutput(stream, cluster), snapshots,
                    EscapingScheme.UNDERSCORE_ESCAPING);
        } catch (IOException e) {
            // The destination is an in-memory stream; keep the existing unchecked scrape API.
            throw new UncheckedIOException(e);
        }
    }

    /** Legacy text type names; the broker's dropwizard-backed metrics still render through this. */
    static String getTypeStr(Collector.Type type) {
        return switch (type) {
            case COUNTER -> "counter";
            case GAUGE, INFO -> "gauge";
            case SUMMARY -> "summary";
            case HISTOGRAM -> "histogram";
            default -> "unknown";
        };
    }

}
