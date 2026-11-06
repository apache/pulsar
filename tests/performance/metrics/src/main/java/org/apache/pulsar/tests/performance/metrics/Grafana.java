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
package org.apache.pulsar.tests.performance.metrics;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.pulsar.tests.performance.report.ReportsUrl;

/** Links into the metrics stack's Grafana, and the panels that a run renders into its report. */
public final class Grafana {
    /**
     * A dashboard that Grafana's setup installs, with the values of its variables other than the run's cluster.
     *
     * @param title the dashboard's title, and what the variables choose
     * @param path the dashboard's UID and slug, such as {@code EetmjdhnA/pulsar-messaging}
     */
    public record Dashboard(String title, String path, Map<String, String> variables) {
    }

    /**
     * A panel of a dashboard.
     *
     * @param name the panel's file name without {@code .png}
     * @param title what the panel shows
     * @param panelId the panel's ID in the dashboard
     */
    public record Panel(String name, String title, Dashboard dashboard, int panelId) {
    }

    static final Dashboard MESSAGING = new Dashboard("Pulsar / Messaging", "EetmjdhnA/pulsar-messaging", Map.of());
    static final Dashboard BROKER_JVM = new Dashboard("Pulsar / JVM of the brokers", "ystagDCsB/pulsar-jvm",
            Map.of("job", "broker", "instance", "All"));
    static final Dashboard BOOKKEEPER = new Dashboard("Pulsar / BookKeeper", "qAjftkhlA/pulsar-bookkeeper",
            Map.of("job", "bookie", "instance", "All"));
    /** The panels that a run renders into its report. */
    public static final List<Panel> REPORT_PANELS = List.of(
            new Panel("publish-rate", "Publish rate", MESSAGING, 16),
            new Panel("delivery-rate", "Delivery rate", MESSAGING, 2),
            new Panel("backlog", "Backlog", MESSAGING, 4),
            new Panel("storage-write-latency", "Storage write latency", MESSAGING, 3),
            new Panel("broker-heap", "Broker heap memory", BROKER_JVM, 1),
            new Panel("broker-gc-time", "Broker GC time", BROKER_JVM, 3),
            new Panel("broker-cpu", "Broker CPU", BROKER_JVM, 4),
            new Panel("bookie-add-entry-latency", "Bookie add entry latency (99th percentile)", BOOKKEEPER, 5));

    private Grafana() {
    }

    /**
     * Grafana's base URL, ending in {@code /}: the configured one, else Grafana's port on the address that the stack
     * is published on, as for the reports server.
     */
    public static String baseUrl(String configuredUrl, String bindAddress) {
        return ReportsUrl.baseUrl(configuredUrl, bindAddress, MetricsStack.GRAFANA_PORT);
    }

    /** The Pulsar / Messaging dashboard of a run: its cluster label, over the time that it was scraped. */
    public static String runDashboard(String baseUrl, String cluster, Instant from, Instant to) {
        return dashboardUrl(baseUrl, MESSAGING, cluster, from, to);
    }

    /** The dashboards of the report's panels, each once, in the order of the panels. */
    public static List<Dashboard> reportDashboards() {
        return REPORT_PANELS.stream().map(Panel::dashboard).distinct().toList();
    }

    /** A dashboard of a run: its cluster label and the dashboard's variables, over a time range. */
    public static String dashboardUrl(String baseUrl, Dashboard dashboard, String cluster, Instant from, Instant to) {
        return baseUrl + "d/" + dashboard.path() + "?" + query(dashboard, cluster, from, to);
    }

    /** A panel of a run, on its own, as Grafana shows it when the panel is viewed. */
    public static String panelUrl(String baseUrl, Panel panel, String cluster, Instant from, Instant to) {
        return dashboardUrl(baseUrl, panel.dashboard(), cluster, from, to) + "&viewPanel=" + panel.panelId();
    }

    // The query of a dashboard's URL for a run, with its variables in a fixed order
    static String query(Dashboard dashboard, String cluster, Instant from, Instant to) {
        StringBuilder query = new StringBuilder("orgId=1&var-cluster=").append(encode(cluster));
        new TreeMap<>(dashboard.variables()).forEach((name, value) ->
                query.append("&var-").append(name).append('=').append(encode(value)));
        return query.append("&from=").append(from.toEpochMilli()).append("&to=").append(to.toEpochMilli())
                .toString();
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
