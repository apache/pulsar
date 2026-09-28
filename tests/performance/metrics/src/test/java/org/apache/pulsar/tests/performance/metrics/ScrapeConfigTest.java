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

import static org.assertj.core.api.Assertions.assertThat;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class ScrapeConfigTest {
    private final ObjectMapper yaml = new ObjectMapper(new YAMLFactory());

    @Test
    public void scrapesEachComponentAsTheDashboardsJobWithTheRunsLabels() throws Exception {
        String config = ScrapeConfig.yaml("iot-42", List.of(
                new ScrapeConfig.Job("broker", List.of(new ScrapeConfig.Target("iot-42-pulsar-broker-0:8080",
                        "pulsar-broker-0"))),
                new ScrapeConfig.Job("zookeeper", List.of(new ScrapeConfig.Target("iot-42-zookeeper:8000",
                        "zookeeper")))), Map.of("cluster", "2026-09-27/master/iot-telemetry/09-27-12-00-00"), 5);

        // A file of VictoriaMetrics' scrape_config_files is a list of scrape configurations
        JsonNode jobs = yaml.readTree(config);
        assertThat(jobs.isArray()).isTrue();
        JsonNode broker = jobs.get(0);
        assertThat(broker.path("job_name").asText()).isEqualTo("iot-42-broker");
        assertThat(broker.path("scrape_interval").asText()).isEqualTo("5s");
        assertThat(broker.path("metrics_path").asText()).isEqualTo("/metrics");
        JsonNode target = broker.path("static_configs").get(0);
        assertThat(target.path("targets").get(0).asText()).isEqualTo("iot-42-pulsar-broker-0:8080");
        assertThat(target.path("labels").path("cluster").asText())
                .isEqualTo("2026-09-27/master/iot-telemetry/09-27-12-00-00");
        assertThat(target.path("labels").path("kubernetes_pod_name").asText()).isEqualTo("pulsar-broker-0");
        assertThat(broker.path("relabel_configs").get(0).path("target_label").asText()).isEqualTo("job");
        assertThat(broker.path("relabel_configs").get(0).path("replacement").asText()).isEqualTo("broker");
        assertThat(jobs.get(1).path("job_name").asText()).isEqualTo("iot-42-zookeeper");
        assertThat(jobs.get(1).path("relabel_configs").get(0).path("replacement").asText()).isEqualTo("zookeeper");
    }

    @Test
    public void namesAContainerOnTheDashboardsWithoutItsCluster() {
        assertThat(RunScrape.instanceName("iot-42", "iot-42-pulsar-bookie-1")).isEqualTo("pulsar-bookie-1");
        assertThat(RunScrape.instanceName("iot-42", "zookeeper")).isEqualTo("zookeeper");
    }

    @Test
    public void rendersAPanelOfTheRunWithTheDashboardsVariables() {
        Grafana.Panel panel = new Grafana.Panel("broker-heap", "Broker heap", Grafana.BROKER_JVM, 1);
        String cluster = "2026-09-27/master/run/09-27-12-00-00";
        Instant from = Instant.ofEpochMilli(1000);
        Instant to = Instant.ofEpochMilli(2000);

        assertThat(GrafanaApi.renderUrl("http://127.0.0.1:3000/", panel, cluster, from, to, 1000, 400))
                .startsWith("http://127.0.0.1:3000/render/d-solo/ystagDCsB/pulsar-jvm?orgId=1"
                        + "&var-cluster=2026-09-27%2Fmaster%2Frun%2F09-27-12-00-00&var-instance=All&var-job=broker"
                        + "&from=1000&to=2000&panelId=1&width=1000&height=400&theme=light&tz=");
        // The panel in Grafana, at the configured base URL
        assertThat(Grafana.panelUrl("http://perf-host:3000/", panel, cluster, from, to))
                .isEqualTo("http://perf-host:3000/d/ystagDCsB/pulsar-jvm?orgId=1"
                        + "&var-cluster=2026-09-27%2Fmaster%2Frun%2F09-27-12-00-00&var-instance=All&var-job=broker"
                        + "&from=1000&to=2000&viewPanel=1");
        // Each file name once, and each dashboard once
        assertThat(Grafana.REPORT_PANELS).extracting(Grafana.Panel::name).doesNotHaveDuplicates();
        assertThat(Grafana.reportDashboards()).extracting(Grafana.Dashboard::title)
                .containsExactly("Pulsar / Messaging", "Pulsar / JVM of the brokers", "Pulsar / BookKeeper");
    }

    @Test
    public void linksARunsMessagingDashboardOverItsTime() {
        assertThat(Grafana.runDashboard("http://127.0.0.1:3000/", "2026-09-27/master/run/09-27-12-00-00",
                Instant.ofEpochMilli(1000), Instant.ofEpochMilli(2000)))
                .isEqualTo("http://127.0.0.1:3000/d/EetmjdhnA/pulsar-messaging?orgId=1"
                        + "&var-cluster=2026-09-27%2Fmaster%2Frun%2F09-27-12-00-00&from=1000&to=2000");
        assertThat(Grafana.baseUrl("http://perf-host:3000", "0.0.0.0")).isEqualTo("http://perf-host:3000/");
        assertThat(Grafana.baseUrl(null, "10.1.2.3")).isEqualTo("http://10.1.2.3:3000/");
    }
}
