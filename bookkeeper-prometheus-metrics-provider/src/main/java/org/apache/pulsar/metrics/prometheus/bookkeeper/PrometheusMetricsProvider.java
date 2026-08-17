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
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.internal.PlatformDependent;
import io.prometheus.metrics.core.metrics.GaugeWithCallback;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.PrometheusNaming;
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
import lombok.CustomLog;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.bookkeeper.stats.ThreadRegistry;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.jetty.ee8.servlet.ServletContextHandler;
import org.eclipse.jetty.ee8.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
// CHECKSTYLE.ON: IllegalImport

/**
 * A <i>Prometheus</i> based {@link StatsProvider} implementation.
 */
@CustomLog
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

    final PrometheusRegistry registry;

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
        this(PrometheusRegistry.defaultRegistry);
    }

    public PrometheusMetricsProvider(PrometheusRegistry registry) {
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
                log.info().attr("endpoint", httpEndpoint).log("Started Prometheus stats endpoint");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        if (exposeDefaultJVMMetrics) {
            // Include standard JVM stats. Note that the metric names produced by the Prometheus Java client 1.x
            // JvmMetrics differ from the ones the legacy simpleclient hotspot exports produced, for example
            // jvm_memory_bytes_used is now jvm_memory_used_bytes.
            registerJvmMetrics();

            // Netty tracks direct memory allocated through unsafe, which is more accurate than the JVM's own
            // accounting, so these two are exported in addition to the standard JVM metrics.
            registerGaugeQuietly("jvm_memory_direct_bytes_used",
                    "Direct memory currently allocated by Netty",
                    () -> getDirectMemoryUsage.get());

            registerGaugeQuietly("jvm_memory_direct_bytes_max",
                    "Maximum direct memory available to the JVM",
                    () -> (double) PlatformDependent.estimateMaxDirectMemory());
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
                log.warn().exception(e).log("Failed to shutdown Jetty server");
            } finally {
                ThreadRegistry.clear();
            }
        }
        if (executor != null) {
            MoreExecutors.shutdownAndAwaitTermination(executor, 5, TimeUnit.SECONDS);
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
        return PrometheusNaming.sanitizeMetricName(completeName);
    }

    @VisibleForTesting
    void rotateLatencyCollection() {
        opStats.forEach((name, metric) -> {
            metric.rotateLatencyCollection();
        });
    }

    private void registerJvmMetrics() {
        try {
            JvmMetrics.builder().register(registry);
        } catch (Exception e) {
            // Ignore if these were already registered, which happens when more than one provider instance shares
            // the default registry.
            log.debug().exception(e).log("Failed to register JVM metrics");
        }
    }

    private void registerGaugeQuietly(String name, String help, Supplier<Double> valueSupplier) {
        try {
            GaugeWithCallback.builder()
                    .name(name)
                    .help(help)
                    .callback(callback -> callback.call(valueSupplier.get()))
                    .register(registry);
        } catch (Exception e) {
            // Ignore if these were already registered
            log.debug().exception(e).attr("metric", name).log("Failed to register Prometheus gauge");
        }
    }

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
                log.warn().exceptionMessage(t)
                        .log("Failed to access netty DIRECT_MEMORY_COUNTER field");
            }
            directMemoryUsage = tmpDirectMemoryUsage;
            getDirectMemoryUsage = () -> directMemoryUsage != null ? directMemoryUsage.get() : Double.NaN;
        } else {
            directMemoryUsage = null;
            List<BufferPoolMXBean> platformMXBeans = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
            poolMxBeanOp = platformMXBeans.stream()
                    .filter(bufferPoolMXBean -> bufferPoolMXBean.getName().equals("direct")).findAny();
            getDirectMemoryUsage = () -> poolMxBeanOp.isPresent()
                    ? (double) poolMxBeanOp.get().getMemoryUsed() : Double.NaN;
        }
    }
}
