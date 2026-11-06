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
package org.apache.pulsar.tests.performance.launcher;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.apache.pulsar.tests.performance.metrics.Grafana;
import org.apache.pulsar.tests.performance.metrics.GrafanaApi;
import org.apache.pulsar.tests.performance.metrics.MetricsStack;
import org.apache.pulsar.tests.performance.metrics.RunScrape;
import org.apache.pulsar.tests.performance.report.ReportsUrl;
import org.apache.pulsar.tests.performance.report.RunReport;
import org.testcontainers.containers.GenericContainer;

/**
 * The collection of a run's metrics into VictoriaMetrics, see {@code docs/metrics.md}: the running metrics stack's, or
 * else a stack that the run starts for itself and stops at its end. Its metrics have the run as their {@code cluster}
 * label, the run directory's path in the reports root, which the dashboards choose runs by. At the end, the run's
 * events become annotations in Grafana, and panels of Grafana's dashboards are rendered into the run directory's
 * {@code grafana-panels}; {@code metrics.json} in the run directory has the label, the time that the run was scraped,
 * the events, the panels and a link to the run in Grafana.
 */
final class MetricsCollection implements AutoCloseable {
    // The port of the bookies' and ZooKeeper's metrics endpoints, their prometheusStatsHttpPort and
    // metricsProvider.httpPort; the configuration store serves none
    static final int STATS_PORT = 8000;
    static final String PANELS_DIRECTORY = "grafana-panels";
    private static final int PANEL_WIDTH = 1000;
    private static final int PANEL_HEIGHT = 400;
    // Grafana's image renderer renders a panel in about 2 seconds, and several at a time
    private static final int CONCURRENT_RENDERS = 4;
    // VictoriaMetrics' -search.latencyOffset, after which the queries see the last scrape
    private static final long QUERY_LATENCY_MILLIS = 3000;

    /**
     * An event of the run, which becomes an annotation in Grafana.
     *
     * @param name the event's tag, such as {@code warmup-finished}
     * @param description what happened, such as {@code Warmup finished: the measurement starts}
     */
    record Event(String name, String description, Instant time) {
    }
    private final MetricsStack startedStack;
    private final RunScrape scrape;
    // Each job's instances, such as broker: [pulsar-broker-0]
    private final Map<String, List<String>> instances;
    private final String cluster;
    private final String runId;
    private final int intervalSeconds;
    private final Instant start;
    private boolean scraping;

    private MetricsCollection(MetricsStack startedStack, RunScrape scrape, Map<String, List<String>> instances,
                              String cluster, String runId, int intervalSeconds) {
        this.startedStack = startedStack;
        this.scrape = scrape;
        this.instances = instances;
        this.cluster = cluster;
        this.runId = runId;
        this.intervalSeconds = intervalSeconds;
        this.start = Instant.now();
        this.scraping = true;
    }

    /**
     * Starts scraping the brokers of a started cluster.
     *
     * @param composeFile the metrics stack's compose file, to start VictoriaMetrics when it doesn't run
     * @param cluster the run's {@code cluster} label
     * @param status prints the collection's status on the console
     * @return the collection, or null when the metrics can't be collected, which the run goes on without
     */
    static MetricsCollection start(PulsarCluster pulsarCluster, Path composeFile, String bindAddress, String cluster,
                                   String runId, MetricsSettings settings, Consumer<String> status) {
        if (composeFile == null) {
            status.accept("Metrics: not collected, since the launcher doesn't know the metrics stack's compose file;"
                    + " run it with the Gradle tasks");
            return null;
        }
        MetricsStack startedStack = null;
        try {
            if (!MetricsStack.isRunning()) {
                status.accept("Metrics: starting the metrics stack for this run, since it doesn't run");
                startedStack = MetricsStack.start(composeFile, bindAddress, false);
            }
            List<RunScrape.Component> components = List.of(
                    new RunScrape.Component("broker", PulsarContainer.BROKER_HTTP_PORT,
                            targets(pulsarCluster.getBrokers())),
                    new RunScrape.Component("bookie", STATS_PORT, targets(pulsarCluster.getBookies())),
                    new RunScrape.Component("zookeeper", STATS_PORT, targets(List.of(pulsarCluster.getZooKeeper()))));
            RunScrape scrape = RunScrape.start(pulsarCluster.getClusterName(), components,
                    Map.of("cluster", cluster, "run_id", runId), settings.intervalSeconds());
            status.accept(String.format("Metrics: VictoriaMetrics scrapes the brokers, the bookies and ZooKeeper every"
                    + " %d s, as the cluster %s", settings.intervalSeconds(), cluster));
            Map<String, List<String>> instances = new LinkedHashMap<>();
            for (RunScrape.Component component : components) {
                instances.put(component.job(), component.targets().stream().map(target ->
                        RunScrape.instanceName(pulsarCluster.getClusterName(), target.name())).toList());
            }
            return new MetricsCollection(startedStack, scrape, instances, cluster, runId, settings.intervalSeconds());
        } catch (Exception e) {
            status.accept("Metrics: not collected, since starting them failed: " + e.getMessage());
            if (startedStack != null) {
                try {
                    startedStack.close();
                } catch (Exception closeFailure) {
                    e.addSuppressed(closeFailure);
                }
            }
            return null;
        }
    }

    // Docker names a container with a leading slash
    private static List<RunScrape.Target> targets(Collection<? extends GenericContainer<?>> containers) {
        return containers.stream().map(container -> new RunScrape.Target(container.getContainerId(),
                container.getContainerName().replaceFirst("^/", ""))).toList();
    }

    /**
     * The run's {@code cluster} label: the run directory's path in the reports root, or its name when it isn't in
     * the reports root.
     */
    static String clusterLabel(Path reportsRoot, Path runDirectory) {
        Path root = reportsRoot.toAbsolutePath().normalize();
        Path run = runDirectory.toAbsolutePath().normalize();
        Path label = run.startsWith(root) && !run.equals(root) ? root.relativize(run) : run.getFileName();
        return label.toString().replace('\\', '/');
    }

    /**
     * Stops scraping after one more scrape, so that the metrics cover the end of the run, adds the run's events to
     * Grafana as annotations, renders the report's panels, and writes {@code metrics.json} into the run directory. A
     * failure to annotate or render is only reported, since the metrics are collected.
     *
     * @param events the run's events
     * @return the link to the run's dashboard in Grafana
     */
    String finish(Path runDirectory, List<Event> events, String grafanaUrl, String bindAddress,
                  Consumer<String> status) throws IOException, InterruptedException {
        Thread.sleep(intervalSeconds * 1000L);
        stopScraping();
        Instant end = Instant.now();
        GrafanaApi grafana = new GrafanaApi(MetricsStack.localUrl(bindAddress, MetricsStack.GRAFANA_PORT));
        try {
            for (Event event : events) {
                grafana.annotate(event.time(), List.of(cluster, event.name()),
                        "Run " + cluster + ": " + event.description());
            }
        } catch (IOException e) {
            status.accept("Metrics: couldn't add the run's events to Grafana: " + e.getMessage());
        }
        Thread.sleep(QUERY_LATENCY_MILLIS);
        List<Grafana.Panel> panels = renderPanels(grafana, runDirectory.resolve(PANELS_DIRECTORY), end, status);
        String baseUrl = Grafana.baseUrl(grafanaUrl, bindAddress);
        String dashboard = Grafana.runDashboard(baseUrl, cluster, start, end);
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode metrics = mapper.createObjectNode();
        metrics.put("cluster", cluster);
        metrics.put("runId", runId);
        metrics.put("intervalSeconds", intervalSeconds);
        metrics.put("startEpochMs", start.toEpochMilli());
        metrics.put("endEpochMs", end.toEpochMilli());
        metrics.put("grafanaDashboard", dashboard);
        putConnections(metrics, bindAddress, baseUrl);
        ArrayNode eventsNode = metrics.putArray("events");
        for (Event event : events) {
            eventsNode.addObject().put("name", event.name()).put("description", event.description())
                    .put("epochMs", event.time().toEpochMilli());
        }
        // The dashboards of the panels over the run, with the configured base URL, which opens them where Grafana is
        // reached
        ArrayNode dashboardsNode = metrics.putArray("dashboards");
        for (Grafana.Dashboard reportDashboard : Grafana.reportDashboards()) {
            dashboardsNode.addObject().put("title", reportDashboard.title())
                    .put("url", Grafana.dashboardUrl(baseUrl, reportDashboard, cluster, start, end));
        }
        ArrayNode panelsNode = metrics.putArray("panels");
        for (Grafana.Panel panel : panels) {
            panelsNode.addObject().put("title", panel.title())
                    .put("file", PANELS_DIRECTORY + "/" + panel.name() + ".png")
                    .put("url", Grafana.panelUrl(baseUrl, panel, cluster, start, end))
                    // The dashboard that the panel is on, over the run, to see the panel among the others
                    .put("dashboard", panel.dashboard().title())
                    .put("dashboardUrl", Grafana.dashboardUrl(baseUrl, panel.dashboard(), cluster, start, end));
        }
        mapper.writerWithDefaultPrettyPrinter().writeValue(runDirectory.resolve(RunReport.METRICS_FILE).toFile(),
                metrics);
        return dashboard;
    }

    /**
     * What an agent or a script needs to query the run's metrics directly: VictoriaMetrics' Prometheus API and the
     * run's label selector, the jobs and their instances, and Grafana's URLs, API credentials and data source.
     */
    private void putConnections(ObjectNode metrics, String bindAddress, String grafanaBaseUrl) {
        metrics.put("selector", "{cluster=\"" + cluster.replace("\\", "\\\\").replace("\"", "\\\"") + "\"}");
        ObjectNode jobs = metrics.putObject("jobs");
        instances.forEach((job, names) -> {
            ArrayNode instancesNode = jobs.putArray(job);
            names.forEach(instancesNode::add);
        });
        ObjectNode labels = metrics.putObject("labels");
        labels.put("cluster", "the run, as in selector");
        labels.put("exported_cluster", "the Pulsar cluster's own name");
        labels.put("job", "the component: " + String.join(", ", instances.keySet()));
        labels.put("kubernetes_pod_name", "the container, as in jobs");
        labels.put("run_id", "the run's ID");
        String victoriaMetrics = ReportsUrl.baseUrl(null, bindAddress, MetricsStack.VICTORIAMETRICS_PORT);
        metrics.putObject("victoriaMetrics")
                .put("url", victoriaMetrics)
                .put("prometheusApi", victoriaMetrics + "api/v1/")
                .put("localUrl", MetricsStack.localUrl(bindAddress, MetricsStack.VICTORIAMETRICS_PORT))
                .put("ui", victoriaMetrics + "vmui/");
        metrics.putObject("grafana")
                .put("url", grafanaBaseUrl)
                .put("localUrl", MetricsStack.localUrl(bindAddress, MetricsStack.GRAFANA_PORT))
                .put("user", MetricsStack.GRAFANA_USER)
                .put("password", MetricsStack.GRAFANA_PASSWORD)
                .put("dataSourceUid", MetricsStack.DATA_SOURCE_UID)
                .put("annotationTag", GrafanaApi.PULSAR_PERFORMANCE_TAG);
    }

    // Renders the report's panels over the time that the run was scraped, several at a time; returns those rendered
    private List<Grafana.Panel> renderPanels(GrafanaApi grafana, Path directory, Instant end,
                                                Consumer<String> status) throws IOException {
        Files.createDirectories(directory);
        List<Grafana.Panel> rendered = new CopyOnWriteArrayList<>();
        try (ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_RENDERS)) {
            for (Grafana.Panel panel : Grafana.REPORT_PANELS) {
                executor.execute(() -> {
                    try {
                        Files.write(directory.resolve(panel.name() + ".png"),
                                grafana.render(panel, cluster, start, end, PANEL_WIDTH, PANEL_HEIGHT));
                        rendered.add(panel);
                    } catch (IOException e) {
                        status.accept("Metrics: couldn't render the panel " + panel.title() + ": " + e.getMessage());
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
        }
        // In the report's order
        return Grafana.REPORT_PANELS.stream().filter(rendered::contains).toList();
    }

    /** Whether the run started the metrics stack for itself, and stops it at its end. */
    boolean startedStack() {
        return startedStack != null;
    }

    private void stopScraping() throws IOException, InterruptedException {
        if (scraping) {
            scraping = false;
            scrape.close();
        }
    }

    /** Stops scraping, also when the run failed, and stops the VictoriaMetrics that the run started. */
    @Override
    public void close() throws IOException, InterruptedException {
        try {
            stopScraping();
        } finally {
            if (startedStack != null) {
                startedStack.close();
            }
        }
    }
}
