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

import io.prometheus.client.Collector;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.eclipse.jetty.server.handler.StatisticsHandler;

/**
 * Collect Prometheus metrics from jetty's org.eclipse.jetty.server.handler.StatisticsHandler.
 *
 * This is ported from prometheus client_java 0.16 version of JettyStatisticsCollector which is using Jetty 9.x.
 * This supports Jetty 12.x.
 */
public class JettyStatisticsCollector extends Collector {
    private final StatisticsHandler statisticsHandler;
    private static final List<String> EMPTY_LIST = new ArrayList<String>();

    public JettyStatisticsCollector(StatisticsHandler statisticsHandler) {
        this.statisticsHandler = statisticsHandler;
    }

    @Override
    public List<MetricFamilySamples> collect() {
        return Arrays.asList(
                buildCounter("jetty_requests_total", "Number of requests", statisticsHandler.getRequests()),
                buildGauge("jetty_requests_active", "Number of requests currently active",
                        statisticsHandler.getRequestsActive()),
                buildGauge("jetty_requests_active_max", "Maximum number of requests that have been active at once",
                        statisticsHandler.getRequestsActiveMax()),
                buildGauge("jetty_request_time_max_seconds", "Maximum time spent handling requests",
                        statisticsHandler.getRequestTimeMax() / 1000_000_000.0),
                buildCounter("jetty_request_time_seconds_total", "Total time spent in all request handling",
                        statisticsHandler.getRequestTimeTotal() / 1000_000_000.0),
                buildCounter("jetty_dispatched_total", "Number of dispatches", statisticsHandler.getHandleTotal()),
                buildGauge("jetty_dispatched_active", "Number of dispatches currently active",
                        statisticsHandler.getHandleActive()),
                buildGauge("jetty_dispatched_active_max", "Maximum number of active dispatches being handled",
                        statisticsHandler.getHandleActiveMax()),
                buildGauge("jetty_dispatched_time_max", "Maximum time spent in dispatch handling",
                        statisticsHandler.getHandleTimeMax() / 1000_000_000.0),
                buildCounter("jetty_dispatched_time_seconds_total", "Total time spent in dispatch handling",
                        statisticsHandler.getHandleTimeTotal() / 1000_000_000.0),
                buildStatusCounter(),
                buildGauge("jetty_stats_seconds", "Time in seconds stats have been collected for",
                        statisticsHandler.getStatisticsDuration().toNanos() / 1000_000_000.0),
                buildCounter("jetty_responses_bytes_total", "Total number of bytes across all responses",
                        statisticsHandler.getBytesRead() + statisticsHandler.getBytesWritten())
        );
    }

    private static MetricFamilySamples buildGauge(String name, String help, double value) {
        return new MetricFamilySamples(
                name,
                Type.GAUGE,
                help,
                Collections.singletonList(new MetricFamilySamples.Sample(name, EMPTY_LIST, EMPTY_LIST, value)));
    }

    private static MetricFamilySamples buildCounter(String name, String help, double value) {
        return new MetricFamilySamples(
                name,
                Type.COUNTER,
                help,
                Collections.singletonList(new MetricFamilySamples.Sample(name, EMPTY_LIST, EMPTY_LIST, value)));
    }

    private MetricFamilySamples buildStatusCounter() {
        String name = "jetty_responses_total";
        return new MetricFamilySamples(
                name,
                Type.COUNTER,
                "Number of requests with response status",
                Arrays.asList(
                        buildStatusSample(name, "1xx", statisticsHandler.getResponses1xx()),
                        buildStatusSample(name, "2xx", statisticsHandler.getResponses2xx()),
                        buildStatusSample(name, "3xx", statisticsHandler.getResponses3xx()),
                        buildStatusSample(name, "4xx", statisticsHandler.getResponses4xx()),
                        buildStatusSample(name, "5xx", statisticsHandler.getResponses5xx())
                )
        );
    }

    private static MetricFamilySamples.Sample buildStatusSample(String name, String status, double value) {
        return new MetricFamilySamples.Sample(
                name,
                Collections.singletonList("code"),
                Collections.singletonList(status),
                value);
    }
}