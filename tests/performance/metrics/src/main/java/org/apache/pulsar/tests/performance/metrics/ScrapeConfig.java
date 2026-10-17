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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A run's scrape configuration for VictoriaMetrics: the metrics endpoints of its components, such as its brokers, with
 * the labels that the dashboards of the Apache Pulsar Helm chart filter on. Each component's job is named after the
 * run, so that the jobs of runs don't clash, and relabeled with the component's job, such as {@code broker}, as the
 * dashboards expect; each container is the dashboards' {@code kubernetes_pod_name}. The run's labels, such as
 * {@code cluster}, replace the ones of the same name in the scraped metrics, which keep theirs as
 * {@code exported_<name>}.
 */
final class ScrapeConfig {
    static final String INSTANCE_LABEL = "kubernetes_pod_name";
    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory()
            .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER));

    /** A container to scrape: its address on the metrics network, and its name on the dashboards. */
    record Target(String address, String instance) {
    }

    /** A component's containers, and the job that the dashboards know it by, such as {@code broker}. */
    record Job(String job, List<Target> targets) {
    }

    private ScrapeConfig() {
    }

    /**
     * The scrape configuration of a run, for VictoriaMetrics' {@code scrape_config_files}.
     *
     * @param run the run's name, unique among the runs
     * @param jobs the run's components
     * @param labels the labels of every metric of the run
     * @param intervalSeconds the scrape interval
     */
    static String yaml(String run, List<Job> jobs, Map<String, String> labels, int intervalSeconds) {
        try {
            // A file of scrape_config_files is a list of scrape configurations
            return YAML.writeValueAsString(jobs.stream().map(job -> scrapeConfig(run + "-" + job.job(), job,
                    labels, intervalSeconds)).toList());
        } catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Map<String, Object> scrapeConfig(String jobName, Job component, Map<String, String> labels,
                                                    int intervalSeconds) {
        List<Map<String, Object>> staticConfigs = component.targets().stream().map(target -> {
            Map<String, String> targetLabels = new LinkedHashMap<>(labels);
            targetLabels.put(INSTANCE_LABEL, target.instance());
            Map<String, Object> staticConfig = new LinkedHashMap<>();
            staticConfig.put("targets", List.of(target.address()));
            staticConfig.put("labels", targetLabels);
            return staticConfig;
        }).toList();
        Map<String, Object> job = new LinkedHashMap<>();
        job.put("job_name", jobName);
        job.put("scrape_interval", intervalSeconds + "s");
        job.put("scrape_timeout", intervalSeconds + "s");
        job.put("metrics_path", "/metrics");
        job.put("static_configs", staticConfigs);
        job.put("relabel_configs", List.of(Map.of("target_label", "job", "replacement", component.job())));
        return job;
    }
}
