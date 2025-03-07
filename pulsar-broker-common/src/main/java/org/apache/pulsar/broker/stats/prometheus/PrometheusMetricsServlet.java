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

import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.EOFException;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.servlet.AsyncContext;
import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrometheusMetricsServlet extends HttpServlet {
    public static final String DEFAULT_METRICS_PATH = "/metrics";
    /**
     * Metrics endpoint uses version 1.0.0 of the Prometheus/OpenMetrics text format.
     * <p>
     * This content-type value ensures compatibility with Prometheus 3.x and later versions.
     * For details, refer to the Prometheus 3.x migration guide:
     * https://prometheus.io/docs/prometheus/latest/migration/#scrape-protocols
     * <p>
     * The difference between version 1.0.0 and 0.0.4 is mainly about counters.
     * The prometheus client_java library >=0.10.0 creates OpenMetrics compatible counters
     * which are not compatible with the 0.0.4 format which is currently in use in BookKeeper.
     * For implementation details of Prometheus client_java, see:
     * https://github.com/prometheus/client_java/blob/parent-0.16.0/
     * simpleclient/src/main/java/io/prometheus/client/Counter.java#L76-L80
     * <p>
     * The library will always append "_total" to the counter name unless the counter name already contains "_total"
     * suffix and will have a separate "_created" counter to ensure OpenMetrics compatibility.
     * This change was introduced in https://github.com/prometheus/client_java/pull/615 in version 0.10.0.
     * Release notes: https://github.com/prometheus/client_java/releases/tag/parent-0.10.0
     * <p>
     * In Pulsar, the library was updated to 0.15.0 with https://github.com/apache/pulsar/pull/13785 in Pulsar 2.11.0.
     */
    public static final String PROMETHEUS_TEXT_FORMAT_V1_0_0 = "text/plain; version=1.0.0; charset=utf-8";
    private static final long serialVersionUID = 1L;
    static final int HTTP_STATUS_OK_200 = 200;
    static final int HTTP_STATUS_INTERNAL_SERVER_ERROR_500 = 500;
    protected final long metricsServletTimeoutMs;
    protected final String cluster;
    protected List<PrometheusRawMetricsProvider> metricsProviders;

    protected ExecutorService executor = null;
    protected final int executorMaxThreads;

    public PrometheusMetricsServlet(long metricsServletTimeoutMs, String cluster) {
        this(metricsServletTimeoutMs, cluster, 1);
    }

    public PrometheusMetricsServlet(long metricsServletTimeoutMs, String cluster, int executorMaxThreads) {
        this.metricsServletTimeoutMs = metricsServletTimeoutMs;
        this.cluster = cluster;
        this.executorMaxThreads = executorMaxThreads;
    }

    @Override
    public void init() throws ServletException {
        if (executorMaxThreads > 0) {
            executor =
                    Executors.newScheduledThreadPool(executorMaxThreads, new DefaultThreadFactory("prometheus-stats"));
        }
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) {
        AsyncContext context = request.startAsync();
        // set hard timeout to 2 * timeout
        if (metricsServletTimeoutMs > 0) {
            context.setTimeout(metricsServletTimeoutMs * 2);
        }
        long startNanos = System.nanoTime();
        AtomicBoolean taskStarted = new AtomicBoolean(false);
        Future<?> future = executor.submit(() -> {
            taskStarted.set(true);
            long elapsedNanos = System.nanoTime() - startNanos;
            // check if the request has been timed out, implement a soft timeout
            // so that response writing can continue to up to 2 * timeout
            if (metricsServletTimeoutMs > 0 && elapsedNanos > TimeUnit.MILLISECONDS.toNanos(metricsServletTimeoutMs)) {
                log.warn("Prometheus metrics request was too long in queue ({}ms). Skipping sending metrics.",
                        TimeUnit.NANOSECONDS.toMillis(elapsedNanos));
                if (!response.isCommitted()) {
                    response.setStatus(HTTP_STATUS_INTERNAL_SERVER_ERROR_500);
                }
                context.complete();
                return;
            }
            handleAsyncMetricsRequest(context);
        });
        context.addListener(new AsyncListener() {
            @Override
            public void onComplete(AsyncEvent asyncEvent) throws IOException {
                if (!taskStarted.get()) {
                    future.cancel(false);
                }
            }

            @Override
            public void onTimeout(AsyncEvent asyncEvent) throws IOException {
                if (!taskStarted.get()) {
                    future.cancel(false);
                }
                log.warn("Prometheus metrics request timed out");
                HttpServletResponse res = (HttpServletResponse) context.getResponse();
                if (!res.isCommitted()) {
                    res.setStatus(HTTP_STATUS_INTERNAL_SERVER_ERROR_500);
                }
                context.complete();
            }

            @Override
            public void onError(AsyncEvent asyncEvent) throws IOException {
                if (!taskStarted.get()) {
                    future.cancel(false);
                }
            }

            @Override
            public void onStartAsync(AsyncEvent asyncEvent) throws IOException {

            }
        });

    }

    private void handleAsyncMetricsRequest(AsyncContext context) {
        long start = System.currentTimeMillis();
        HttpServletResponse res = (HttpServletResponse) context.getResponse();
        try {
            generateMetricsSynchronously(res);
        } catch (Exception e) {
            long end = System.currentTimeMillis();
            long time = end - start;
            if (e instanceof EOFException) {
                // NO STACKTRACE
                log.error("Failed to send metrics, "
                        + "likely the client or this server closed "
                        + "the connection due to a timeout ({} ms elapsed): {}", time, e + "");
            } else {
                log.error("Failed to generate prometheus stats, {} ms elapsed", time, e);
            }
            if (!res.isCommitted()) {
                res.setStatus(HTTP_STATUS_INTERNAL_SERVER_ERROR_500);
            }
        } finally {
            long end = System.currentTimeMillis();
            long time = end - start;
            try {
                context.complete();
            } catch (IllegalStateException e) {
                // this happens when metricsServletTimeoutMs expires
                // java.lang.IllegalStateException: AsyncContext completed and/or Request lifecycle recycled
                log.error("Failed to generate prometheus stats, "
                        + "this is likely due to metricsServletTimeoutMs: {} ms elapsed: {}", time, e + "");
            }
        }
    }

    private void generateMetricsSynchronously(HttpServletResponse res) throws IOException {
        res.setStatus(HTTP_STATUS_OK_200);
        res.setContentType(PROMETHEUS_TEXT_FORMAT_V1_0_0);
        PrometheusMetricsGeneratorUtils.generate(cluster, res.getOutputStream(), metricsProviders);
    }

    @Override
    public void destroy() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    public void addRawMetricsProvider(PrometheusRawMetricsProvider metricsProvider) {
        if (metricsProviders == null) {
            metricsProviders = new LinkedList<>();
        }
        metricsProviders.add(metricsProvider);
    }

    private static final Logger log = LoggerFactory.getLogger(PrometheusMetricsServlet.class);
}
