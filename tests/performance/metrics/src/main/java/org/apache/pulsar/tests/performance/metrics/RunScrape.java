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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * A run's containers, such as its brokers, bookies and ZooKeeper, scraped by the running VictoriaMetrics while the run
 * runs: the containers join the metrics network under their container names, which are unique among the runs, and
 * the run's scrape configuration goes into VictoriaMetrics' {@code /etc/victoriametrics/runs}, which VictoriaMetrics
 * checks every second. Closing it removes the scrape configuration; the containers leave the network when they are
 * removed. It uses the {@code docker} CLI.
 */
public final class RunScrape implements AutoCloseable {
    static final String RUNS_DIRECTORY = "/etc/victoriametrics/runs";

    /** A container of the run: its ID, and its name, which it joins the metrics network by. */
    public record Target(String containerId, String name) {
    }

    /**
     * A component of the run.
     *
     * @param job the component's job on the dashboards, such as {@code broker}, {@code bookie} or {@code zookeeper}
     * @param port the port of its metrics endpoint, {@code /metrics}
     * @param targets its containers
     */
    public record Component(String job, int port, List<Target> targets) {
    }

    private final String victoriaMetrics;
    private final String file;

    private RunScrape(String victoriaMetrics, String file) {
        this.victoriaMetrics = victoriaMetrics;
        this.file = file;
    }

    /**
     * Starts scraping a run's components.
     *
     * @param name the run's name among the runs that VictoriaMetrics scrapes, its cluster's name, which its containers'
     *             names start with
     * @param components the run's components
     * @param labels the labels of every metric of the run, such as {@code cluster}
     * @param intervalSeconds the scrape interval
     */
    public static RunScrape start(String name, List<Component> components, Map<String, String> labels,
                                  int intervalSeconds) throws IOException, InterruptedException {
        String victoriaMetrics = MetricsStack.victoriaMetricsContainerId();
        if (victoriaMetrics == null) {
            throw new IOException("VictoriaMetrics isn't running");
        }
        List<ScrapeConfig.Job> jobs = new ArrayList<>();
        for (Component component : components) {
            List<ScrapeConfig.Target> targets = new ArrayList<>();
            for (Target target : component.targets()) {
                MetricsStack.docker("network", "connect", "--alias", target.name(), MetricsStack.NETWORK,
                        target.containerId()).check("connect " + target.name() + " to the metrics network");
                targets.add(new ScrapeConfig.Target(target.name() + ":" + component.port(),
                        instanceName(name, target.name())));
            }
            jobs.add(new ScrapeConfig.Job(component.job(), targets));
        }
        String file = RUNS_DIRECTORY + "/" + name + ".yaml";
        String yaml = ScrapeConfig.yaml(name, jobs, labels, intervalSeconds);
        // Written beside the file and renamed, so that VictoriaMetrics never reads a part of it; docker cp doesn't
        // reach the tmpfs mount of the directory
        MetricsStack.docker("exec", victoriaMetrics, "sh", "-c", "echo " + Base64.getEncoder().encodeToString(
                yaml.getBytes(StandardCharsets.UTF_8)) + " | base64 -d > " + file + ".tmp && mv " + file + ".tmp "
                + file).check("add the run's scrape configuration to VictoriaMetrics");
        return new RunScrape(victoriaMetrics, file);
    }

    // A container's name on the dashboards, such as pulsar-broker-0, without the cluster's name that its name starts
    // with
    public static String instanceName(String clusterName, String containerName) {
        return containerName.startsWith(clusterName + "-") ? containerName.substring(clusterName.length() + 1)
                : containerName;
    }

    /** Stops scraping the run's containers. */
    @Override
    public void close() throws IOException, InterruptedException {
        MetricsStack.docker("exec", victoriaMetrics, "rm", "-f", file)
                .check("remove the run's scrape configuration from VictoriaMetrics");
    }
}
