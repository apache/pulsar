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
package org.apache.pulsar.metrics.prometheus.bookkeeper;

// CHECKSTYLE.OFF: IllegalImport

import com.google.common.annotations.VisibleForTesting;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.internal.PlatformDependent;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.Gauge.Child;
import io.prometheus.client.hotspot.GarbageCollectorExports;
import io.prometheus.client.hotspot.MemoryPoolsExports;
import io.prometheus.client.hotspot.StandardExports;
import io.prometheus.client.hotspot.ThreadExports;
import java.io.IOException;
import java.io.Writer;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.stats.ThreadRegistry;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.ee8.servlet.ServletContextHandler;
import org.eclipse.jetty.ee8.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
// CHECKSTYLE.ON: IllegalImport

/**
 * A <i>Prometheus</i> based {@link StatsProvider} implementation.
 */
public class PrometheusMetricsProvider implements StatsProvider {

    private ScheduledExecutorService executor;

    public static final String PROMETHEUS_STATS_HTTP_ENABLE = "prometheusStatsHttpEnable";
    public static final boolean DEFAULT_PROMETHEUS_STATS_HTTP_ENABLE = true;

    public static final String PROMETHEUS_STATS_HTTP_ADDRESS = "prometheusStatsHttpAddress";
    public static final String DEFAULT_PROMETHEUS_STATS_HTTP_ADDR = "0.0.0.0";

    public static final String PROMETHEUS_STATS_HTTP_PORT = "prometheusStatsHttpPort";
    public static final int DEFAULT_PROMETHEUS_STATS_HTTP_PORT = 8000;

    public static final String PROMETHEUS_STATS_LATENCY_ROLLOVER_SECONDS = "prometheusStatsLatencyRolloverSeconds";
    public static final int DEFAULT_PROMETHEUS_STATS_LATENCY_ROLLOVER_SECONDS = 60;

    final CollectorRegistry registry;

    Server server;

    /*
     * These acts a registry of the metrics defined in this provider
     */
    final ConcurrentMap<ScopeContext, LongAdderCounter> counters = new ConcurrentHashMap<>();
    final ConcurrentMap<ScopeContext, SimpleGauge<? extends Number>> gauges = new ConcurrentHashMap<>();
    final ConcurrentMap<ScopeContext, DataSketchesOpStatsLogger> opStats = new ConcurrentHashMap<>();
    final ConcurrentMap<ScopeContext, ThreadScopedDataSketchesStatsLogger> threadScopedOpStats =
            new ConcurrentHashMap<>();
    final ConcurrentMap<ScopeContext, ThreadScopedLongAdderCounter> threadScopedCounters =
            new ConcurrentHashMap<>();

    public PrometheusMetricsProvider() {
        this(CollectorRegistry.defaultRegistry);
    }

    public PrometheusMetricsProvider(CollectorRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void start(Configuration conf) {
        boolean httpEnabled = conf.getBoolean(PROMETHEUS_STATS_HTTP_ENABLE, DEFAULT_PROMETHEUS_STATS_HTTP_ENABLE);
        boolean bkHttpServerEnabled = conf.getBoolean("httpServerEnabled", false);
        boolean exposeDefaultJVMMetrics = conf.getBoolean("exposeDefaultJVMMetrics", true);
        // only start its own http server when prometheus http is enabled and bk http server is not enabled.
        if (httpEnabled && !bkHttpServerEnabled) {
            String httpAddr = conf.getString(PROMETHEUS_STATS_HTTP_ADDRESS, DEFAULT_PROMETHEUS_STATS_HTTP_ADDR);
            int httpPort = conf.getInt(PROMETHEUS_STATS_HTTP_PORT, DEFAULT_PROMETHEUS_STATS_HTTP_PORT);
            InetSocketAddress httpEndpoint = InetSocketAddress.createUnresolved(httpAddr, httpPort);
            this.server = new Server(httpEndpoint);
            ServletContextHandler context = new ServletContextHandler();
            context.setContextPath("/");
            server.setHandler(context);

            context.addServlet(new ServletHolder(new PrometheusServlet(this)), "/metrics");

            try {
                server.start();
                log.info("Started Prometheus stats endpoint at {}", httpEndpoint);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        if (exposeDefaultJVMMetrics) {
            // Include standard JVM stats
            registerMetrics(new StandardExports());
            registerMetrics(new MemoryPoolsExports());
            registerMetrics(new GarbageCollectorExports());
            registerMetrics(new ThreadExports());

        // Add direct memory allocated through unsafe
            registerMetrics(Gauge.build("jvm_memory_direct_bytes_used", "-").create().setChild(new Child() {
                @Override
                public double get() {
                    return getDirectMemoryUsage.get();
                }
            }));

            registerMetrics(Gauge.build("jvm_memory_direct_bytes_max", "-").create().setChild(new Child() {
                @Override
                public double get() {
                    return PlatformDependent.estimateMaxDirectMemory();
                }
            }));
        }

        executor = Executors.newSingleThreadScheduledExecutor(new DefaultThreadFactory("metrics"));

        int latencyRolloverSeconds = conf.getInt(PROMETHEUS_STATS_LATENCY_ROLLOVER_SECONDS,
                DEFAULT_PROMETHEUS_STATS_LATENCY_ROLLOVER_SECONDS);

        executor.scheduleAtFixedRate(() -> {
            rotateLatencyCollection();
        }, 1, latencyRolloverSeconds, TimeUnit.SECONDS);

    }

    @Override
    public void stop() {
        if (server != null) {
            try {
                server.stop();
            } catch (Exception e) {
                log.warn("Failed to shutdown Jetty server", e);
            } finally {
                ThreadRegistry.clear();
            }
        }
    }

    @Override
    public StatsLogger getStatsLogger(String scope) {
        return new PrometheusStatsLogger(PrometheusMetricsProvider.this, scope, Collections.emptyMap());
    }

    @Override
    public void writeAllMetrics(Writer writer) throws IOException {
        PrometheusTextFormat prometheusTextFormat = new PrometheusTextFormat();
        PrometheusTextFormat.writeMetricsCollectedByPrometheusClient(writer, registry);

        gauges.forEach((sc, gauge) -> prometheusTextFormat.writeGauge(writer, sc.getScope(), gauge));
        counters.forEach((sc, counter) -> prometheusTextFormat.writeCounter(writer, sc.getScope(), counter));
        opStats.forEach((sc, opStatLogger) ->
                prometheusTextFormat.writeOpStat(writer, sc.getScope(), opStatLogger));
    }

    @Override
    public String getStatsName(String... statsComponents) {
        String completeName;
        if (statsComponents.length == 0) {
            return "";
        } else if (statsComponents[0].isEmpty()) {
            completeName = StringUtils.join(statsComponents, '_', 1, statsComponents.length);
        } else {
            completeName = StringUtils.join(statsComponents, '_');
        }
        return Collector.sanitizeMetricName(completeName);
    }

    @VisibleForTesting
    void rotateLatencyCollection() {
        opStats.forEach((name, metric) -> {
            metric.rotateLatencyCollection();
        });
    }

    private void registerMetrics(Collector collector) {
        try {
            collector.register(registry);
        } catch (Exception e) {
            // Ignore if these were already registered
            if (log.isDebugEnabled()) {
                log.debug("Failed to register Prometheus collector exports", e);
            }
        }
    }

    private static final Logger log = LoggerFactory.getLogger(PrometheusMetricsProvider.class);

    /*
     * Try to get Netty counter of used direct memory. This will be correct, unlike the JVM values.
     */
    private static final AtomicLong directMemoryUsage;
    private static final Optional<BufferPoolMXBean> poolMxBeanOp;
    private static final Supplier<Double> getDirectMemoryUsage;

    static {
        if (PlatformDependent.useDirectBufferNoCleaner()) {
            poolMxBeanOp = Optional.empty();
            AtomicLong tmpDirectMemoryUsage = null;
            try {
                Field field = PlatformDependent.class.getDeclaredField("DIRECT_MEMORY_COUNTER");
                field.setAccessible(true);
                tmpDirectMemoryUsage = (AtomicLong) field.get(null);
            } catch (Throwable t) {
                log.warn("Failed to access netty DIRECT_MEMORY_COUNTER field {}", t.getMessage());
            }
            directMemoryUsage = tmpDirectMemoryUsage;
            getDirectMemoryUsage = () -> directMemoryUsage != null ? directMemoryUsage.get() : Double.NaN;
        } else {
            directMemoryUsage = null;
            List<BufferPoolMXBean> platformMXBeans = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
            poolMxBeanOp = platformMXBeans.stream()
                    .filter(bufferPoolMXBean -> bufferPoolMXBean.getName().equals("direct")).findAny();
            getDirectMemoryUsage = () -> poolMxBeanOp.isPresent() ? poolMxBeanOp.get().getMemoryUsed() : Double.NaN;
        }
    }
}
