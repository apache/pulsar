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
package org.apache.pulsar.metrics.prometheus.zookeeper;

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;
import io.prometheus.metrics.expositionformats.PrometheusTextFormatWriter;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.DataPointSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.InfoSnapshot;
import io.prometheus.metrics.model.snapshots.Label;
import io.prometheus.metrics.model.snapshots.Labels;
import io.prometheus.metrics.model.snapshots.MetricMetadata;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.Quantile;
import io.prometheus.metrics.model.snapshots.SummarySnapshot;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.CustomLog;
import org.apache.zookeeper.metrics.Counter;
import org.apache.zookeeper.metrics.CounterSet;
import org.apache.zookeeper.metrics.Gauge;
import org.apache.zookeeper.metrics.GaugeSet;
import org.apache.zookeeper.metrics.MetricsContext;
import org.apache.zookeeper.metrics.MetricsProvider;
import org.apache.zookeeper.metrics.MetricsProviderLifeCycleException;
import org.apache.zookeeper.metrics.Summary;
import org.apache.zookeeper.metrics.SummarySet;
import org.apache.zookeeper.server.RateLogger;
import org.eclipse.jetty.ee8.nested.ServletConstraint;
import org.eclipse.jetty.ee8.security.ConstraintMapping;
import org.eclipse.jetty.ee8.security.ConstraintSecurityHandler;
import org.eclipse.jetty.ee8.servlet.ServletContextHandler;
import org.eclipse.jetty.ee8.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import org.slf4j.LoggerFactory;

/**
 * A Metrics Provider implementation based on https://prometheus.io.
 *
 * <p>The metrics are backed by the Prometheus Java client 1.x ({@code io.prometheus:prometheus-metrics-*}).
 * By default they are registered in {@link PrometheusRegistry#defaultRegistry} so that all the metrics
 * of the JVM are available as a single view, which matches the behavior of the former simpleclient based
 * implementation. Collectors that have not been migrated from the legacy simpleclient registry yet,
 * such as the log4j2 Prometheus appender configured in {@code conf/log4j2.yaml}, are exposed from
 * {@code CollectorRegistry.defaultRegistry} as well until #27045 is complete.
 */
@CustomLog
public class PrometheusMetricsProvider implements MetricsProvider {
    private static final String LABEL = "key";
    private static final String[] LABELS = {LABEL};

    private static final PrometheusTextFormatWriter TEXT_FORMAT_WRITER = PrometheusTextFormatWriter.create();

    /**
     * simpleclient 0.16.0 ages summary quantiles over 10 minutes split into 5 buckets, while
     * Prometheus Java client 1.x defaults to 5 minutes. Keep the old window so that quantiles
     * of bursty ZooKeeper operations do not expire earlier under the same metric names.
     */
    static final long SUMMARY_MAX_AGE_SECONDS = 600;
    static final int SUMMARY_AGE_BUCKETS = 5;

    /**
     * Number of worker threads for reporting Prometheus summary metrics.
     * Default value is 1.
     * If the number is less than 1, the main thread will be used.
     */
    static final String NUM_WORKER_THREADS = "numWorkerThreads";

    /**
     * The max queue size for Prometheus summary metrics reporting task.
     * Default value is 1000000.
     */
    static final String MAX_QUEUE_SIZE = "maxQueueSize";

    /**
     * The timeout in ms for Prometheus worker threads shutdown.
     * Default value is 1000ms.
     */
    static final String WORKER_SHUTDOWN_TIMEOUT_MS = "workerShutdownTimeoutMs";

    /**
     * We are using the 'defaultRegistry'.
     * <p>
     * When you are running ZooKeeper (server or client) together with other
     * libraries every metrics will be expected as a single view.
     * </p>
     */
    private final PrometheusRegistry collectorRegistry;
    private final RateLogger rateLogger = new RateLogger(
            LoggerFactory.getLogger(PrometheusMetricsProvider.class), 60 * 1000);
    private String host = "0.0.0.0";
    private int port = 7000;
    private boolean exportJvmInfo = true;
    private Server server;
    private final MetricsServletImpl servlet = new MetricsServletImpl();
    private final Context rootContext = new Context();
    private int numWorkerThreads = 1;
    private int maxQueueSize = 1000000;
    private long workerShutdownTimeoutMs = 1000;
    private Optional<ExecutorService> executorOptional = Optional.empty();

    public PrometheusMetricsProvider() {
        this(PrometheusRegistry.defaultRegistry);
    }

    /**
     * Constructor that allows isolating a provider instance in its own registry. ZooKeeper creates
     * the provider through the no-arg constructor; the overload mirrors the BookKeeper stats
     * provider and keeps tests independent of the process-wide default registry.
     */
    PrometheusMetricsProvider(PrometheusRegistry collectorRegistry) {
        this.collectorRegistry = collectorRegistry;
    }

    @Override
    public void configure(Properties configuration) throws MetricsProviderLifeCycleException {
        log.info().attr("configuration", configuration).log("Initializing metrics");
        this.host = configuration.getProperty("httpHost", "0.0.0.0");
        this.port = Integer.parseInt(configuration.getProperty("httpPort", "7000"));
        this.exportJvmInfo = Boolean.parseBoolean(configuration.getProperty("exportJvmInfo", "true"));
        this.numWorkerThreads = Integer.parseInt(
                configuration.getProperty(NUM_WORKER_THREADS, "1"));
        this.maxQueueSize = Integer.parseInt(
                configuration.getProperty(MAX_QUEUE_SIZE, "1000000"));
        this.workerShutdownTimeoutMs = Long.parseLong(
                configuration.getProperty(WORKER_SHUTDOWN_TIMEOUT_MS, "1000"));
    }

    @Override
    public void start() throws MetricsProviderLifeCycleException {
        this.executorOptional = createExecutor();
        try {
            log.info().attr("host", host).attr("port", port)
                    .attr("exportJvmInfo", exportJvmInfo)
                    .log("Starting /metrics HTTP endpoint");
            if (exportJvmInfo) {
                // JvmMetrics only registers its collectors the first time it is called for a registry,
                // which matches the one-time initialization that simpleclient's DefaultExports performed.
                JvmMetrics.builder().register(collectorRegistry);
            }
            server = new Server(new InetSocketAddress(host, port));
            ServletContextHandler context = new ServletContextHandler();
            context.setContextPath("/");
            constrainTraceMethod(context);
            server.setHandler(context);
            context.addServlet(new ServletHolder(servlet), "/metrics");
            server.start();
        } catch (Exception err) {
            log.error().exception(err).log("Cannot start /metrics server");
            if (server != null) {
                try {
                    server.stop();
                } catch (Exception suppressed) {
                    err.addSuppressed(suppressed);
                } finally {
                    server = null;
                }
            }
            throw new MetricsProviderLifeCycleException(err);
        }
    }

    // for tests
    MetricsServletImpl getServlet() {
        return servlet;
    }

    @Override
    public MetricsContext getRootContext() {
        return rootContext;
    }

    @Override
    public void stop() {
        shutdownExecutor();
        if (server != null) {
            try {
                server.stop();
            } catch (Exception err) {
                log.error().exception(err).log("Cannot safely stop Jetty server");
            } finally {
                server = null;
            }
        }
    }

    /**
     * Dump all values to the 4lw interface and to the Admin server.
     * <p>
     * This method is not expected to be used to serve metrics to Prometheus. We
     * are using the text exposition format writer provided by Prometheus for that,
     * leaving the real representation to the Prometheus Java client.
     * </p>
     *
     * @param sink the receiver of data (4lw interface, Admin server or tests)
     */
    @Override
    public void dump(BiConsumer<String, Object> sink) {
        sampleGauges();
        for (MetricSnapshot snapshot : collectorRegistry.scrape()) {
            for (DataPointSnapshot dataPoint : snapshot.getDataPoints()) {
                if (dataPoint instanceof CounterSnapshot.CounterDataPointSnapshot counter) {
                    sink.accept(buildKeyForDump(getCounterName(snapshot.getMetadata()), counter.getLabels()),
                            counter.getValue());
                } else if (dataPoint instanceof GaugeSnapshot.GaugeDataPointSnapshot gauge) {
                    sink.accept(buildKeyForDump(
                            snapshot.getMetadata().getPrometheusName(), gauge.getLabels()), gauge.getValue());
                } else if (dataPoint instanceof SummarySnapshot.SummaryDataPointSnapshot summary) {
                    String name = snapshot.getMetadata().getPrometheusName();
                    for (Quantile quantile : summary.getQuantiles()) {
                        sink.accept(buildKeyForDump(name, summary.getLabels(),
                                "quantile", Double.toString(quantile.getQuantile())), quantile.getValue());
                    }
                    sink.accept(buildKeyForDump(name + "_count", summary.getLabels()),
                            (double) summary.getCount());
                    sink.accept(buildKeyForDump(name + "_sum", summary.getLabels()), summary.getSum());
                } else if (dataPoint instanceof InfoSnapshot.InfoDataPointSnapshot info) {
                    // Info metrics such as jvm_runtime_info carry the runtime labels and a constant
                    // value of 1, matching the gauge that simpleclient's DefaultExports exposed.
                    sink.accept(buildKeyForDump(getInfoName(snapshot.getMetadata()), info.getLabels()), 1.0);
                }
            }
        }
        // Keep exposing collectors that are still registered in the legacy simpleclient registry,
        // most notably the log4j2 Prometheus appender configured in conf/log4j2.yaml.
        Enumeration<Collector.MetricFamilySamples> legacyFamilies = legacyMetricFamilySamples();
        while (legacyFamilies.hasMoreElements()) {
            for (Collector.MetricFamilySamples.Sample sample : legacyFamilies.nextElement().samples) {
                sink.accept(buildKeyForDump(sample), sample.value);
            }
        }
    }

    /**
     * Returns the sample name of an info metric. The 1.x client strips the {@code _info} suffix from
     * the metric name and appends it again in the exposition format, so it has to be added explicitly
     * when building the key for {@link #dump}.
     */
    private static String getInfoName(MetricMetadata metadata) {
        return metadata.getPrometheusName() + "_info";
    }

    /**
     * Collectors that have not been migrated from simpleclient 0.x yet (see #27045) still register
     * in {@link CollectorRegistry#defaultRegistry}. JVM and process collectors are filtered out:
     * this provider registers {@link JvmMetrics} on the 1.x registry, so exposing the old samples
     * here would produce two sets of JVM metrics under different names.
     */
    private static Enumeration<Collector.MetricFamilySamples> legacyMetricFamilySamples() {
        List<Collector.MetricFamilySamples> families = new ArrayList<>();
        Enumeration<Collector.MetricFamilySamples> enumeration =
                CollectorRegistry.defaultRegistry.metricFamilySamples();
        while (enumeration.hasMoreElements()) {
            Collector.MetricFamilySamples filtered = enumeration.nextElement().filter(
                    name -> !name.startsWith("jvm_") && !name.startsWith("process_"));
            if (filtered != null) {
                families.add(filtered);
            }
        }
        return Collections.enumeration(families);
    }

    /**
     * Appends the samples of the legacy simpleclient registry in the 0.0.4 text format. The 1.x
     * writer has already flushed its output to the same stream at this point.
     */
    private static void writeLegacyMetrics(OutputStream out) throws IOException {
        Writer writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
        TextFormat.write004(writer, legacyMetricFamilySamples());
        writer.flush();
    }

    /**
     * Returns the sample name of a counter, which is the exposition base name with the {@code _total} suffix.
     * The 1.x client strips the suffix from the metric name and appends it again in the exposition format, so
     * the suffix has to be added explicitly when building the key for {@link #dump}.
     */
    private static String getCounterName(MetricMetadata metadata) {
        String name = metadata.getExpositionBasePrometheusName();
        return name.endsWith("_total") ? name : name + "_total";
    }

    private static String buildKeyForDump(String name, Labels labels) {
        return buildKeyForDump(name, labels, null, null);
    }

    private static String buildKeyForDump(Collector.MetricFamilySamples.Sample sample) {
        StringBuilder keyBuilder = new StringBuilder(sample.name);
        if (!sample.labelNames.isEmpty()) {
            keyBuilder.append('{');
            for (int i = 0; i < sample.labelNames.size(); ++i) {
                if (i > 0) {
                    keyBuilder.append(',');
                }
                keyBuilder.append(sample.labelNames.get(i))
                        .append("=\"")
                        .append(sample.labelValues.get(i))
                        .append('"');
            }
            keyBuilder.append('}');
        }
        return keyBuilder.toString();
    }

    private static String buildKeyForDump(String name, Labels labels,
                                          String additionalLabelName, String additionalLabelValue) {
        StringBuilder keyBuilder = new StringBuilder(name);
        if (!labels.isEmpty() || additionalLabelName != null) {
            keyBuilder.append('{');
            boolean isFirst = true;
            for (Label label : labels) {
                if (!isFirst) {
                    keyBuilder.append(',');
                }
                isFirst = false;
                keyBuilder.append(label.getName());
                keyBuilder.append("=\"");
                keyBuilder.append(label.getValue());
                keyBuilder.append('"');
            }
            if (additionalLabelName != null) {
                if (!isFirst) {
                    keyBuilder.append(',');
                }
                keyBuilder.append(additionalLabelName);
                keyBuilder.append("=\"");
                keyBuilder.append(additionalLabelValue);
                keyBuilder.append('"');
            }
            keyBuilder.append('}');
        }
        return keyBuilder.toString();
    }

    /**
     * Update Gauges. In ZooKeeper Metrics API Gauges are callbacks served by
     * internal components and the value is not held by Prometheus structures.
     */
    private void sampleGauges() {
        rootContext.gauges.values()
                .forEach(PrometheusGaugeWrapper::sample);

        rootContext.gaugeSets.values()
                .forEach(PrometheusLabelledGaugeWrapper::sample);
    }

    @Override
    public void resetAllValues() {
        // not supported on Prometheus
    }

    /**
     * Add constraint to a given context to disallow TRACE method.
     * @param ctxHandler the context to modify
     */
    private void constrainTraceMethod(ServletContextHandler ctxHandler) {
        ServletConstraint c = new ServletConstraint();
        c.setAuthenticate(true);

        ConstraintMapping cmt = new ConstraintMapping();
        cmt.setConstraint(c);
        cmt.setMethod("TRACE");
        cmt.setPathSpec("/*");

        ConstraintSecurityHandler securityHandler = new ConstraintSecurityHandler();
        securityHandler.setConstraintMappings(new ConstraintMapping[] {cmt});

        ctxHandler.setSecurityHandler(securityHandler);
    }

    private class Context implements MetricsContext {

        private final ConcurrentMap<String, PrometheusGaugeWrapper> gauges = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, PrometheusLabelledGaugeWrapper> gaugeSets = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, PrometheusCounter> counters = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, PrometheusLabelledCounter> counterSets = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, PrometheusSummary> basicSummaries = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, PrometheusSummary> summaries = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, PrometheusLabelledSummary> basicSummarySets = new ConcurrentHashMap<>();
        private final ConcurrentMap<String, PrometheusLabelledSummary> summarySets = new ConcurrentHashMap<>();

        @Override
        public MetricsContext getContext(String name) {
            // no hierarchy yet
            return this;
        }

        @Override
        public Counter getCounter(String name) {
            return counters.computeIfAbsent(name, PrometheusCounter::new);
        }

        @Override
        public CounterSet getCounterSet(final String name) {
            Objects.requireNonNull(name, "Cannot register a CounterSet with null name");
            return counterSets.computeIfAbsent(name, PrometheusLabelledCounter::new);
        }

        /**
         * Gauges may go up and down, in ZooKeeper they are a way to export
         * internal values with a callback.
         *
         * @param name  the name of the gauge
         * @param gauge the callback
         */
        @Override
        public void registerGauge(String name, Gauge gauge) {
            Objects.requireNonNull(name);
            gauges.compute(name, (id, prev) ->
                    new PrometheusGaugeWrapper(id, gauge, prev != null ? prev.inner : null));
        }

        @Override
        public void unregisterGauge(String name) {
            PrometheusGaugeWrapper existing = gauges.remove(name);
            if (existing != null) {
                existing.unregister();
            }
        }

        @Override
        public void registerGaugeSet(final String name, final GaugeSet gaugeSet) {
            Objects.requireNonNull(name, "Cannot register a GaugeSet with null name");
            Objects.requireNonNull(gaugeSet, "Cannot register a null GaugeSet for " + name);

            gaugeSets.compute(name, (id, prev) ->
                new PrometheusLabelledGaugeWrapper(name, gaugeSet, prev != null ? prev.inner : null));
        }

        @Override
        public void unregisterGaugeSet(final String name) {
            Objects.requireNonNull(name, "Cannot unregister GaugeSet with null name");

            final PrometheusLabelledGaugeWrapper existing = gaugeSets.remove(name);
            if (existing != null) {
                existing.unregister();
            }
        }

        @Override
        public Summary getSummary(String name, DetailLevel detailLevel) {
            if (detailLevel == DetailLevel.BASIC) {
                return basicSummaries.computeIfAbsent(name, (n) -> {
                    if (summaries.containsKey(n)) {
                        throw new IllegalArgumentException("Already registered a non basic summary as " + n);
                    }
                    return new PrometheusSummary(name, detailLevel);
                });
            } else {
                return summaries.computeIfAbsent(name, (n) -> {
                    if (basicSummaries.containsKey(n)) {
                        throw new IllegalArgumentException("Already registered a basic summary as " + n);
                    }
                    return new PrometheusSummary(name, detailLevel);
                });
            }
        }

        @Override
        public SummarySet getSummarySet(String name, DetailLevel detailLevel) {
            if (detailLevel == DetailLevel.BASIC) {
                return basicSummarySets.computeIfAbsent(name, (n) -> {
                    if (summarySets.containsKey(n)) {
                        throw new IllegalArgumentException("Already registered a non basic summary set as " + n);
                    }
                    return new PrometheusLabelledSummary(name, detailLevel);
                });
            } else {
                return summarySets.computeIfAbsent(name, (n) -> {
                    if (basicSummarySets.containsKey(n)) {
                        throw new IllegalArgumentException("Already registered a basic summary set as " + n);
                    }
                    return new PrometheusLabelledSummary(name, detailLevel);
                });
            }
        }

    }

    private class PrometheusCounter implements Counter {

        private final io.prometheus.metrics.core.metrics.Counter inner;
        private final String name;

        public PrometheusCounter(String name) {
            this.name = name;
            this.inner = io.prometheus.metrics.core.metrics.Counter
                    .builder()
                    .name(name)
                    .help(name)
                    .register(collectorRegistry);
        }

        @Override
        public void add(long delta) {
            try {
                inner.inc(delta);
            } catch (IllegalArgumentException err) {
                log.error().attr("delta", delta).attr("metric", name)
                        .exception(err).log("invalid delta for metric");
            }
        }

        @Override
        public long get() {
            // this method is used only for tests
            // Prometheus returns a "double"
            // it is safe to fine to a long
            // we are never setting non-integer values
            return (long) inner.get();
        }

    }

    private class PrometheusLabelledCounter implements CounterSet {
        private final String name;
        private final io.prometheus.metrics.core.metrics.Counter inner;

        public PrometheusLabelledCounter(final String name) {
            this.name = name;
            this.inner = io.prometheus.metrics.core.metrics.Counter
                    .builder()
                    .name(name)
                    .help(name)
                    .labelNames(LABELS)
                    .register(collectorRegistry);
        }

        @Override
        public void add(final String key, final long delta) {
            if (key == null) {
                log.error().attr("delta", delta).attr("metric", name)
                        .log("Cannot add to a CounterSet with a null key");
                return;
            }
            try {
                inner.labelValues(key).inc(delta);
            } catch (final IllegalArgumentException e) {
                log.error().attr("delta", delta).attr("metric", name)
                        .attr("key", key).exception(e).log("invalid delta for metric");
            }
        }
    }

    private class PrometheusGaugeWrapper {

        private final io.prometheus.metrics.core.metrics.Gauge inner;
        private final Gauge gauge;
        private final String name;

        public PrometheusGaugeWrapper(String name, Gauge gauge,
                                      io.prometheus.metrics.core.metrics.Gauge prev) {
            this.name = name;
            this.gauge = gauge;
            this.inner = prev != null ? prev
                    : io.prometheus.metrics.core.metrics.Gauge
                    .builder()
                    .name(name)
                    .help(name)
                    .register(collectorRegistry);
        }

        /**
         * Call the callback and update Prometheus Gauge. This method is called
         * when the server is polling for a value.
         */
        private void sample() {
            Number value = gauge.get();
            this.inner.set(value != null ? value.doubleValue() : 0);
        }

        private void unregister() {
            collectorRegistry.unregister(inner);
        }
    }

    /**
     * Prometheus implementation of GaugeSet interface. It wraps the GaugeSet object and
     * sets the values of a labelled Prometheus Gauge when the server is polling.
     */
    private class PrometheusLabelledGaugeWrapper {
        private final String name;
        private final GaugeSet gaugeSet;
        private final io.prometheus.metrics.core.metrics.Gauge inner;

        private PrometheusLabelledGaugeWrapper(final String name,
                                               final GaugeSet gaugeSet,
                                               final io.prometheus.metrics.core.metrics.Gauge prev) {
            this.name = name;
            this.gaugeSet = gaugeSet;
            this.inner = prev != null ? prev
                    : io.prometheus.metrics.core.metrics.Gauge
                    .builder()
                    .name(name)
                    .help(name)
                    .labelNames(LABELS)
                    .register(collectorRegistry);
        }

        /**
         * Call the callback provided by the GaugeSet and update Prometheus Gauge. This method
         * is called when the server is polling for a value.
         */
        private void sample() {
            gaugeSet.values().forEach((key, value) -> {
                if (key == null) {
                    throw new IllegalArgumentException(
                            "Cannot use a null key for the GaugeSet " + name);
                }
                this.inner.labelValues(key).set(value != null ? value.doubleValue() : 0);
            });
        }

        private void unregister() {
            collectorRegistry.unregister(inner);
        }
    }

    private class PrometheusSummary implements Summary {

        private final io.prometheus.metrics.core.metrics.Summary inner;
        private final String name;

        public PrometheusSummary(String name, MetricsContext.DetailLevel level) {
            this.name = name;
            var builder = io.prometheus.metrics.core.metrics.Summary.builder()
                    .name(name)
                    .help(name)
                    .maxAgeSeconds(SUMMARY_MAX_AGE_SECONDS)
                    .numberOfAgeBuckets(SUMMARY_AGE_BUCKETS)
                    .quantile(0.5, 0.05); // Add 50th percentile (= median) with 5% tolerated error
            if (level == MetricsContext.DetailLevel.ADVANCED) {
                builder.quantile(0.9, 0.01) // Add 90th percentile with 1% tolerated error
                        .quantile(0.99, 0.001); // Add 99th percentile with 0.1% tolerated error
            }
            this.inner = builder.register(collectorRegistry);
        }

        @Override
        public void add(long delta) {
            reportMetrics(() -> observe(delta));
        }

        private void observe(final long delta) {
            try {
                inner.observe(delta);
            } catch (final IllegalArgumentException err) {
                log.error().attr("delta", delta).attr("metric", name)
                        .exception(err).log("invalid delta for metric");
            }
        }
    }

    private class PrometheusLabelledSummary implements SummarySet {

        private final io.prometheus.metrics.core.metrics.Summary inner;
        private final String name;

        public PrometheusLabelledSummary(String name, MetricsContext.DetailLevel level) {
            this.name = name;
            var builder = io.prometheus.metrics.core.metrics.Summary.builder()
                    .name(name)
                    .help(name)
                    .labelNames(LABELS)
                    .maxAgeSeconds(SUMMARY_MAX_AGE_SECONDS)
                    .numberOfAgeBuckets(SUMMARY_AGE_BUCKETS)
                    .quantile(0.5, 0.05); // Add 50th percentile (= median) with 5% tolerated error
            if (level == MetricsContext.DetailLevel.ADVANCED) {
                builder.quantile(0.9, 0.01) // Add 90th percentile with 1% tolerated error
                        .quantile(0.99, 0.001); // Add 99th percentile with 0.1% tolerated error
            }
            this.inner = builder.register(collectorRegistry);
        }

        @Override
        public void add(String key, long value) {
            reportMetrics(() -> observe(key, value));
        }

        private void observe(final String key, final long value) {
            if (key == null) {
                log.error().attr("value", value).attr("metric", name)
                        .log("Cannot add to a SummarySet with a null key");
                return;
            }
            try {
                inner.labelValues(key).observe(value);
            } catch (final IllegalArgumentException err) {
                log.error().attr("value", value).attr("metric", name)
                        .attr("key", key).exception(err).log("invalid value for metric");
            }
        }

    }

    class MetricsServletImpl extends HttpServlet {
        private static final long serialVersionUID = 1L;

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            // little trick: update the Gauges before serving data
            // from the Prometheus registry
            sampleGauges();
            resp.setStatus(HttpServletResponse.SC_OK);
            resp.setContentType(PrometheusTextFormatWriter.CONTENT_TYPE);
            OutputStream out = resp.getOutputStream();
            TEXT_FORMAT_WRITER.write(out, collectorRegistry.scrape());
            // The log4j2 Prometheus appender is still a simpleclient 0.x collector (see #27045).
            writeLegacyMetrics(out);
        }
    }

    private Optional<ExecutorService> createExecutor() {
        if (numWorkerThreads < 1) {
            log.info().attr("numWorkerThreads", numWorkerThreads)
                    .log("Executor service was not created as numWorkerThreads is less than 1");
            return Optional.empty();
        }

        final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(maxQueueSize);
        final ThreadPoolExecutor executor = new ThreadPoolExecutor(numWorkerThreads,
                numWorkerThreads,
                0L,
                TimeUnit.MILLISECONDS,
                queue, new PrometheusWorkerThreadFactory());
        log.info().attr("numWorkerThreads", numWorkerThreads)
                .attr("maxQueueSize", maxQueueSize)
                .log("Executor service was created");
        return Optional.of(executor);
    }

    private void shutdownExecutor() {
        if (executorOptional.isPresent()) {
            log.info().attr("timeoutMs", workerShutdownTimeoutMs)
                    .log("Shutdown executor service");
            final ExecutorService executor = executorOptional.get();
            executor.shutdown();
            try {
                if (!executor.awaitTermination(workerShutdownTimeoutMs, TimeUnit.MILLISECONDS)) {
                    log.error().attr("timeoutMs", workerShutdownTimeoutMs)
                            .log("Not all Prometheus worker threads terminated properly");
                    executor.shutdownNow();
                }
            } catch (final Exception e) {
                log.error().exception(e)
                        .log("Error occurred while terminating Prometheus worker threads");
                executor.shutdownNow();
            }
        }
    }

    private static class PrometheusWorkerThreadFactory implements ThreadFactory {
        private static final AtomicInteger workerCounter = new AtomicInteger(1);

        @Override
        public Thread newThread(final Runnable runnable) {
            final String threadName = "PrometheusMetricsProviderWorker-" + workerCounter.getAndIncrement();
            final Thread thread = new Thread(runnable, threadName);
            thread.setDaemon(true);
            return thread;
        }
    }

    private void reportMetrics(final Runnable task) {
        if (executorOptional.isPresent()) {
            try {
                executorOptional.get().submit(task);
            } catch (final RejectedExecutionException e) {
                rateLogger.rateLimitLog("Prometheus metrics reporting task queue size exceeded the max",
                        String.valueOf(maxQueueSize));
            }
        } else {
            task.run();
        }
    }
}
