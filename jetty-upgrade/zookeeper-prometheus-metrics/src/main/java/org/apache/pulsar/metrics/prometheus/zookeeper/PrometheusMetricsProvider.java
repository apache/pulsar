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
import io.prometheus.client.exporter.MetricsServlet;
import io.prometheus.client.hotspot.DefaultExports;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Enumeration;
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
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Metrics Provider implementation based on https://prometheus.io.
 *
 * @since 3.6.0
 */
public class PrometheusMetricsProvider implements MetricsProvider {

    private static final Logger LOG = LoggerFactory.getLogger(PrometheusMetricsProvider.class);
    private static final String LABEL = "key";
    private static final String[] LABELS = {LABEL};

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
    private final CollectorRegistry collectorRegistry = CollectorRegistry.defaultRegistry;
    private final RateLogger rateLogger = new RateLogger(LOG, 60 * 1000);
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

    @Override
    public void configure(Properties configuration) throws MetricsProviderLifeCycleException {
        LOG.info("Initializing metrics, configuration: {}", configuration);
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
            LOG.info("Starting /metrics HTTP endpoint at host: {}, port: {}, exportJvmInfo: {}",
                    host, port, exportJvmInfo);
            if (exportJvmInfo) {
                DefaultExports.initialize();
            }
            server = new Server(new InetSocketAddress(host, port));
            ServletContextHandler context = new ServletContextHandler();
            context.setContextPath("/");
            constrainTraceMethod(context);
            server.setHandler(context);
            context.addServlet(new ServletHolder(servlet), "/metrics");
            server.start();
        } catch (Exception err) {
            LOG.error("Cannot start /metrics server", err);
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
                LOG.error("Cannot safely stop Jetty server", err);
            } finally {
                server = null;
            }
        }
    }

    /**
     * Dump all values to the 4lw interface and to the Admin server.
     * <p>
     * This method is not expected to be used to serve metrics to Prometheus. We
     * are using the MetricsServlet provided by Prometheus for that, leaving the
     * real representation to the Prometheus Java client.
     * </p>
     *
     * @param sink the receiver of data (4lw interface, Admin server or tests)
     */
    @Override
    public void dump(BiConsumer<String, Object> sink) {
        sampleGauges();
        Enumeration<Collector.MetricFamilySamples> samplesFamilies = collectorRegistry.metricFamilySamples();
        while (samplesFamilies.hasMoreElements()) {
            Collector.MetricFamilySamples samples = samplesFamilies.nextElement();
            samples.samples.forEach(sample -> {
                String key = buildKeyForDump(sample);
                sink.accept(key, sample.value);
            });
        }
    }

    private static String buildKeyForDump(Collector.MetricFamilySamples.Sample sample) {
        StringBuilder keyBuilder = new StringBuilder();
        keyBuilder.append(sample.name);
        if (sample.labelNames.size() > 0) {
            keyBuilder.append('{');
            for (int i = 0; i < sample.labelNames.size(); ++i) {
                if (i > 0) {
                    keyBuilder.append(',');
                }
                keyBuilder.append(sample.labelNames.get(i));
                keyBuilder.append("=\"");
                keyBuilder.append(sample.labelValues.get(i));
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

        private final io.prometheus.client.Counter inner;
        private final String name;

        public PrometheusCounter(String name) {
            this.name = name;
            this.inner = io.prometheus.client.Counter
                    .build(name, name)
                    .register(collectorRegistry);
        }

        @Override
        public void add(long delta) {
            try {
                inner.inc(delta);
            } catch (IllegalArgumentException err) {
                LOG.error("invalid delta {} for metric {}", delta, name, err);
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
        private final io.prometheus.client.Counter inner;

        public PrometheusLabelledCounter(final String name) {
            this.name = name;
            this.inner = io.prometheus.client.Counter
                    .build(name, name)
                    .labelNames(LABELS)
                    .register(collectorRegistry);
        }

        @Override
        public void add(final String key, final long delta) {
            try {
                inner.labels(key).inc(delta);
            } catch (final IllegalArgumentException e) {
                LOG.error("invalid delta {} for metric {} with key {}", delta, name, key, e);
            }
        }
    }

    private class PrometheusGaugeWrapper {

        private final io.prometheus.client.Gauge inner;
        private final Gauge gauge;
        private final String name;

        public PrometheusGaugeWrapper(String name, Gauge gauge, io.prometheus.client.Gauge prev) {
            this.name = name;
            this.gauge = gauge;
            this.inner = prev != null ? prev
                    : io.prometheus.client.Gauge
                    .build(name, name)
                    .register(collectorRegistry);
        }

        /**
         * Call the callack and update Prometheus Gauge. This method is called
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
     * uses the callback API to update the Prometheus Gauge.
     */
    private class PrometheusLabelledGaugeWrapper {
        private final GaugeSet gaugeSet;
        private final io.prometheus.client.Gauge inner;

        private PrometheusLabelledGaugeWrapper(final String name,
                                               final GaugeSet gaugeSet,
                                               final io.prometheus.client.Gauge prev) {
            this.gaugeSet = gaugeSet;
            this.inner = prev != null ? prev :
                    io.prometheus.client.Gauge
                            .build(name, name)
                            .labelNames(LABELS)
                            .register(collectorRegistry);
        }

        /**
         * Call the callback provided by the GaugeSet and update Prometheus Gauge.
         * This method is called when the server is polling for a value.
         */
        private void sample() {
            gaugeSet.values().forEach((key, value) ->
                this.inner.labels(key).set(value != null ? value.doubleValue() : 0));
        }

        private void unregister() {
            collectorRegistry.unregister(inner);
        }
    }

    private class PrometheusSummary implements Summary {

        private final io.prometheus.client.Summary inner;
        private final String name;

        public PrometheusSummary(String name, MetricsContext.DetailLevel level) {
            this.name = name;
            if (level == MetricsContext.DetailLevel.ADVANCED) {
                this.inner = io.prometheus.client.Summary
                        .build(name, name)
                        .quantile(0.5, 0.05) // Add 50th percentile (= median) with 5% tolerated error
                        .quantile(0.9, 0.01) // Add 90th percentile with 1% tolerated error
                        .quantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
                        .register(collectorRegistry);
            } else {
                this.inner = io.prometheus.client.Summary
                        .build(name, name)
                        .quantile(0.5, 0.05) // Add 50th percentile (= median) with 5% tolerated error
                        .register(collectorRegistry);
            }
        }

        @Override
        public void add(long delta) {
            reportMetrics(() -> observe(delta));
        }

        private void observe(final long delta) {
            try {
                inner.observe(delta);
            } catch (final IllegalArgumentException err) {
                LOG.error("invalid delta {} for metric {}", delta, name, err);
            }
        }
    }

    private class PrometheusLabelledSummary implements SummarySet {

        private final io.prometheus.client.Summary inner;
        private final String name;

        public PrometheusLabelledSummary(String name, MetricsContext.DetailLevel level) {
            this.name = name;
            if (level == MetricsContext.DetailLevel.ADVANCED) {
                this.inner = io.prometheus.client.Summary
                        .build(name, name)
                        .labelNames(LABELS)
                        .quantile(0.5, 0.05) // Add 50th percentile (= median) with 5% tolerated error
                        .quantile(0.9, 0.01) // Add 90th percentile with 1% tolerated error
                        .quantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
                        .register(collectorRegistry);
            } else {
                this.inner = io.prometheus.client.Summary
                        .build(name, name)
                        .labelNames(LABELS)
                        .quantile(0.5, 0.05) // Add 50th percentile (= median) with 5% tolerated error
                        .register(collectorRegistry);
            }
        }

        @Override
        public void add(String key, long value) {
            reportMetrics(() -> observe(key, value));
        }

        private void observe(final String key, final long value) {
            try {
                inner.labels(key).observe(value);
            } catch (final IllegalArgumentException err) {
                LOG.error("invalid value {} for metric {} with key {}", value, name, key, err);
            }
        }

    }

    class MetricsServletImpl extends MetricsServlet {

        @Override
        protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            // little trick: update the Gauges before serving data
            // from Prometheus CollectorRegistry
            sampleGauges();
            // serve data using Prometheus built in client.
            super.doGet(req, resp);
        }
    }

    private Optional<ExecutorService> createExecutor() {
        if (numWorkerThreads < 1) {
            LOG.info("Executor service was not created as numWorkerThreads {} is less than 1", numWorkerThreads);
            return Optional.empty();
        }

        final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>(maxQueueSize);
        final ThreadPoolExecutor executor = new ThreadPoolExecutor(numWorkerThreads,
                numWorkerThreads,
                0L,
                TimeUnit.MILLISECONDS,
                queue, new PrometheusWorkerThreadFactory());
        LOG.info("Executor service was created with numWorkerThreads {} and maxQueueSize {}",
                numWorkerThreads,
                maxQueueSize);
        return Optional.of(executor);
    }

    private void shutdownExecutor() {
        if (executorOptional.isPresent()) {
            LOG.info("Shutdown executor service with timeout {}", workerShutdownTimeoutMs);
            final ExecutorService executor = executorOptional.get();
            executor.shutdown();
            try {
                if (!executor.awaitTermination(workerShutdownTimeoutMs, TimeUnit.MILLISECONDS)) {
                    LOG.error("Not all the Prometheus worker threads terminated properly after {} timeout",
                            workerShutdownTimeoutMs);
                    executor.shutdownNow();
                }
            } catch (final Exception e) {
                LOG.error("Error occurred while terminating Prometheus worker threads", e);
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
