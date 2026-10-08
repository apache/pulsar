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

import java.nio.file.Path;
import java.util.concurrent.Callable;
import org.apache.pulsar.tests.performance.report.ReportsUrl;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

/**
 * Starts and stops the metrics stack with Docker Compose. {@code up} runs it in the background, so that Grafana shows
 * the metrics of the runs made meanwhile and before: the runs find VictoriaMetrics running and have it scrape their
 * clusters. {@code down} stops it.
 */
@Command(name = "metrics", mixinStandardHelpOptions = true,
        description = "Start and stop the performance tests' metrics stack, VictoriaMetrics and Grafana",
        subcommands = {MetricsCommand.Up.class, MetricsCommand.Down.class})
public final class MetricsCommand {
    private MetricsCommand() {
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new MetricsCommand()).execute(args));
    }

    /** The options of every subcommand. */
    static final class StackOptions {
        @Option(names = "--compose-file", required = true, defaultValue = "${sys:performance.metrics.composeFile}",
                description = "The stack's compose.yaml; the Gradle tasks pass it")
        Path composeFile;

        @Option(names = "--bind-address", defaultValue = "${sys:performance.metrics.bindAddress:-127.0.0.1}",
                description = "The address that VictoriaMetrics and Grafana are published on; default: "
                        + "${DEFAULT-VALUE}, reachable only from this host")
        String bindAddress;

        @Option(names = "--grafana-url", defaultValue = "${sys:performance.metrics.grafanaUrl}",
                description = "The URL that Grafana is reached at, which it prints; default: Grafana's port on the "
                        + "bind address, or with 0.0.0.0 on the address of the host's first network interface with an "
                        + "IPv4 address")
        String grafanaUrl;
    }

    @Command(name = "up", mixinStandardHelpOptions = true,
            description = "Start the metrics stack in the background, which keeps running until down stops it")
    static final class Up implements Callable<Integer> {
        @Mixin
        StackOptions options;

        @Override
        public Integer call() throws Exception {
            if (MetricsStack.isRunning()) {
                System.out.println("The metrics stack runs already");
                return 1;
            }
            System.out.println("Starting VictoriaMetrics and Grafana; the first start pulls the images, and sets up"
                    + " Grafana");
            MetricsStack.start(options.composeFile, options.bindAddress, true);
            System.out.println("Grafana: " + Grafana.baseUrl(options.grafanaUrl, options.bindAddress) + " (log in as "
                    + MetricsStack.GRAFANA_USER + ", password " + MetricsStack.GRAFANA_PASSWORD
                    + ", to edit; viewing needs no login)");
            String victoriaMetrics = ReportsUrl.baseUrl(null, options.bindAddress, MetricsStack.VICTORIAMETRICS_PORT);
            System.out.println("VictoriaMetrics: " + victoriaMetrics + "vmui/?#/metrics (browse the metrics), "
                    + victoriaMetrics + "vmui (PromQL queries)");
            System.out.println("Runs send their metrics here. It keeps running, also across restarts of Docker, until "
                    + "./gradlew :tests:performance:metrics:down stops it; the metrics and Grafana's settings stay in "
                    + "the Docker volumes " + String.join(" and ", MetricsStack.VOLUMES));
            return 0;
        }
    }

    @Command(name = "down", mixinStandardHelpOptions = true,
            description = "Stop the metrics stack; its volumes keep the metrics and Grafana's settings")
    static final class Down implements Callable<Integer> {
        @Mixin
        StackOptions options;

        @Override
        public Integer call() throws Exception {
            MetricsStack.stop(options.composeFile, options.bindAddress, true);
            return 0;
        }
    }
}
