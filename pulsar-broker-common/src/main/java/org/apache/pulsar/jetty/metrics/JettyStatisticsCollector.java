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

package org.apache.pulsar.jetty.metrics;

import io.prometheus.metrics.model.registry.MultiCollector;
import io.prometheus.metrics.model.snapshots.CounterSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;
import io.prometheus.metrics.model.snapshots.MetricFamilyDescriptor;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.util.List;
import org.eclipse.jetty.server.handler.StatisticsHandler;

/**
 * Collect Prometheus metrics from jetty's org.eclipse.jetty.server.handler.StatisticsHandler.
 *
 * Port of the client_java 0.16 JettyStatisticsCollector, which targeted Jetty 9.x, adapted to Jetty 12.x and the
 * Prometheus Java client 1.x. The metric names are unchanged.
 */
public class JettyStatisticsCollector implements MultiCollector {
    // Counters are declared without the "_total" suffix; the exposition format appends it.
    private static final MetricFamilyDescriptor REQUESTS = counter("jetty_requests", "Number of requests");
    private static final MetricFamilyDescriptor REQUESTS_ACTIVE =
            gauge("jetty_requests_active", "Number of requests currently active");
    private static final MetricFamilyDescriptor REQUESTS_ACTIVE_MAX =
            gauge("jetty_requests_active_max", "Maximum number of requests that have been active at once");
    private static final MetricFamilyDescriptor REQUEST_TIME_MAX_SECONDS =
            gauge("jetty_request_time_max_seconds", "Maximum time spent handling requests");
    private static final MetricFamilyDescriptor REQUEST_TIME_SECONDS =
            counter("jetty_request_time_seconds", "Total time spent in all request handling");
    private static final MetricFamilyDescriptor DISPATCHED = counter("jetty_dispatched", "Number of dispatches");
    private static final MetricFamilyDescriptor DISPATCHED_ACTIVE =
            gauge("jetty_dispatched_active", "Number of dispatches currently active");
    private static final MetricFamilyDescriptor DISPATCHED_ACTIVE_MAX =
            gauge("jetty_dispatched_active_max", "Maximum number of active dispatches being handled");
    private static final MetricFamilyDescriptor DISPATCHED_TIME_MAX =
            gauge("jetty_dispatched_time_max", "Maximum time spent in dispatch handling");
    private static final MetricFamilyDescriptor DISPATCHED_TIME_SECONDS =
            counter("jetty_dispatched_time_seconds", "Total time spent in dispatch handling");
    private static final MetricFamilyDescriptor RESPONSES = MetricFamilyDescriptor.counter("jetty_responses")
            .help("Number of requests with response status")
            .labelNames("code")
            .build();
    private static final MetricFamilyDescriptor STATS_SECONDS =
            gauge("jetty_stats_seconds", "Time in seconds stats have been collected for");
    private static final MetricFamilyDescriptor RESPONSES_BYTES =
            counter("jetty_responses_bytes", "Total number of bytes across all responses");
    private static final List<MetricFamilyDescriptor> DESCRIPTORS = List.of(
            REQUESTS, REQUESTS_ACTIVE, REQUESTS_ACTIVE_MAX, REQUEST_TIME_MAX_SECONDS, REQUEST_TIME_SECONDS,
            DISPATCHED, DISPATCHED_ACTIVE, DISPATCHED_ACTIVE_MAX, DISPATCHED_TIME_MAX, DISPATCHED_TIME_SECONDS,
            RESPONSES, STATS_SECONDS, RESPONSES_BYTES);

    private final StatisticsHandler statisticsHandler;

    public JettyStatisticsCollector(StatisticsHandler statisticsHandler) {
        this.statisticsHandler = statisticsHandler;
    }

    @Override
    @SuppressWarnings("deprecation")
    public MetricSnapshots collect() {
        return MetricSnapshots.builder()
                .metricSnapshot(counter(REQUESTS, unsigned(statisticsHandler.getRequests())))
                .metricSnapshot(gauge(REQUESTS_ACTIVE, statisticsHandler.getRequestsActive()))
                .metricSnapshot(gauge(REQUESTS_ACTIVE_MAX, statisticsHandler.getRequestsActiveMax()))
                .metricSnapshot(gauge(REQUEST_TIME_MAX_SECONDS,
                        statisticsHandler.getRequestTimeMax() / 1000_000_000.0))
                .metricSnapshot(counter(REQUEST_TIME_SECONDS,
                        statisticsHandler.getRequestTimeTotal() / 1000_000_000.0))
                .metricSnapshot(counter(DISPATCHED, unsigned(statisticsHandler.getHandleTotal())))
                .metricSnapshot(gauge(DISPATCHED_ACTIVE, statisticsHandler.getHandleActive()))
                .metricSnapshot(gauge(DISPATCHED_ACTIVE_MAX, statisticsHandler.getHandleActiveMax()))
                .metricSnapshot(gauge(DISPATCHED_TIME_MAX,
                        statisticsHandler.getHandleTimeMax() / 1000_000_000.0))
                .metricSnapshot(counter(DISPATCHED_TIME_SECONDS,
                        statisticsHandler.getHandleTimeTotal() / 1000_000_000.0))
                .metricSnapshot(statusCounter())
                .metricSnapshot(gauge(STATS_SECONDS,
                        statisticsHandler.getStatisticsDuration().toNanos() / 1000_000_000.0))
                .metricSnapshot(counter(RESPONSES_BYTES,
                        statisticsHandler.getBytesRead() + statisticsHandler.getBytesWritten()))
                .build();
    }

    /** Fixed metadata lets the registry reject a second collector instance at registration time. */
    @Override
    public List<MetricFamilyDescriptor> getMetricFamilyDescriptors() {
        return DESCRIPTORS;
    }

    private static MetricFamilyDescriptor counter(String name, String help) {
        return MetricFamilyDescriptor.counter(name).help(help).build();
    }

    private static MetricFamilyDescriptor gauge(String name, String help) {
        return MetricFamilyDescriptor.gauge(name).help(help).build();
    }

    private static MetricSnapshot gauge(MetricFamilyDescriptor family, double value) {
        return new GaugeSnapshot(family.getMetadata(),
                List.of(new GaugeSnapshot.GaugeDataPointSnapshot(value, Labels.EMPTY, null)));
    }

    private static MetricSnapshot counter(MetricFamilyDescriptor family, double value) {
        return new CounterSnapshot(family.getMetadata(), List.of(counterDataPoint(value, Labels.EMPTY)));
    }

    private MetricSnapshot statusCounter() {
        return new CounterSnapshot(RESPONSES.getMetadata(), List.of(
                statusDataPoint("1xx", statisticsHandler.getResponses1xx()),
                statusDataPoint("2xx", statisticsHandler.getResponses2xx()),
                statusDataPoint("3xx", statisticsHandler.getResponses3xx()),
                statusDataPoint("4xx", statisticsHandler.getResponses4xx()),
                statusDataPoint("5xx", statisticsHandler.getResponses5xx())));
    }

    private static CounterSnapshot.CounterDataPointSnapshot statusDataPoint(String status, int value) {
        return counterDataPoint(unsigned(value), Labels.of("code", status));
    }

    /**
     * Jetty keeps its request and response totals in an {@code int} that wraps negative after 2^31 events. A 1.x
     * counter snapshot rejects a negative value and that would fail the whole scrape, so the total is read as
     * unsigned instead.
     */
    private static long unsigned(int value) {
        return Integer.toUnsignedLong(value);
    }

    /**
     * The {@code long} totals (bytes, nanoseconds) can also wrap on a very long run. A wrapped value is useless
     * either way, so it is clamped to zero, which Prometheus reads as a counter reset, rather than failing the
     * scrape.
     */
    private static CounterSnapshot.CounterDataPointSnapshot counterDataPoint(double value, Labels labels) {
        return new CounterSnapshot.CounterDataPointSnapshot(Math.max(0.0, value), labels, null, 0L);
    }
}
