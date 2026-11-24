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
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.pulsar.tests.performance.report.ReportsUrl;

/**
 * The performance tests' metrics stack, VictoriaMetrics and Grafana with its image renderer, which Docker Compose runs
 * in the background from {@code compose.yaml}, as the project {@value #COMPOSE_PROJECT}. Its volumes and the network
 * that the runs connect their containers to are external to the project, created with the {@code docker} CLI before
 * the stack starts, so that stopping the stack keeps them, and with them the metrics, the dashboards and Grafana's
 * settings.
 */
public final class MetricsStack implements AutoCloseable {
    /** The Docker Compose project of the stack, which a fixed name lets any process stop. */
    static final String COMPOSE_PROJECT = "pulsar-performance-metrics";
    /** The network that a run connects its containers to, so that VictoriaMetrics can scrape them. */
    public static final String NETWORK = "pulsar-performance-metrics";
    // The label of the stack's containers in compose.yaml, which names the service
    static final String SERVICE_LABEL = "org.apache.pulsar.tests.performance.metrics";
    static final String VICTORIAMETRICS_SERVICE = "victoriametrics";
    static final List<String> VOLUMES =
            List.of("pulsar-performance-victoriametrics-data", "pulsar-performance-grafana-data");
    public static final int VICTORIAMETRICS_PORT = 8428;
    public static final int GRAFANA_PORT = 3000;
    public static final String GRAFANA_USER = "admin";
    public static final String GRAFANA_PASSWORD = "pulsar-performance";
    /** The UID of Grafana's VictoriaMetrics data source, as grafana/provisioning/datasources provisions it. */
    public static final String DATA_SOURCE_UID = "victoriametrics";
    /** How long the stack may take to answer, which on the first start includes pulling the images. */
    private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(10);

    private final Path composeFile;
    private final String bindAddress;

    private MetricsStack(Path composeFile, String bindAddress) {
        this.composeFile = composeFile;
        this.bindAddress = bindAddress;
    }

    /** Whether the stack's VictoriaMetrics runs, which any process may have started. */
    public static boolean isRunning() throws IOException, InterruptedException {
        return victoriaMetricsContainerId() != null;
    }

    /** The ID of the running VictoriaMetrics container, or null when it isn't running. */
    static String victoriaMetricsContainerId() throws IOException, InterruptedException {
        CommandResult result = docker("ps", "--quiet", "--filter",
                "label=" + SERVICE_LABEL + "=" + VICTORIAMETRICS_SERVICE);
        result.check("list the running containers");
        String output = result.output().strip();
        return output.isEmpty() ? null : output.lines().findFirst().orElseThrow();
    }

    /**
     * Starts the stack in the background with Docker Compose, and waits until it answers. It keeps running until
     * {@link #close()} or {@link #stop} stops it.
     *
     * @param composeFile the stack's {@code compose.yaml}
     * @param bindAddress the address that VictoriaMetrics and Grafana are published on
     * @param showOutput whether Docker Compose shows its progress on the console, or only in a failure's message
     */
    public static MetricsStack start(Path composeFile, String bindAddress, boolean showOutput)
            throws IOException, InterruptedException {
        createPersistentResources();
        MetricsStack stack = new MetricsStack(composeFile, bindAddress);
        try {
            compose(composeFile, bindAddress, showOutput, "up", "--detach", "--wait").check("start the metrics stack");
            awaitHealthy(localUrl(bindAddress, VICTORIAMETRICS_PORT) + "health");
            awaitHealthy(localUrl(bindAddress, GRAFANA_PORT) + "api/health");
        } catch (IOException | InterruptedException | RuntimeException e) {
            // Also the containers of a start that failed part way, such as on a port that another process holds
            try {
                stack.close();
            } catch (IOException | InterruptedException | RuntimeException stopFailure) {
                e.addSuppressed(stopFailure);
            }
            throw e;
        }
        return stack;
    }

    /** Stops the stack, which any process may have started; its volumes stay. */
    public static void stop(Path composeFile, String bindAddress, boolean showOutput)
            throws IOException, InterruptedException {
        compose(composeFile, bindAddress, showOutput, "down").check("stop the metrics stack");
    }

    /** Stops the stack that {@link #start} started, which lets VictoriaMetrics write what it holds. */
    @Override
    public void close() throws IOException, InterruptedException {
        stop(composeFile, bindAddress, false);
    }

    /** The URL on this host of a port that the stack publishes on an address. */
    public static String localUrl(String bindAddress, int port) {
        String host = localHost(bindAddress);
        return "http://" + (host.contains(":") ? "[" + host + "]" : host) + ":" + port + "/";
    }

    // A wildcard address is reachable on the loopback interface too
    private static String localHost(String bindAddress) {
        return bindAddress == null || bindAddress.isBlank() || ReportsUrl.isWildcard(bindAddress)
                ? ReportsUrl.DEFAULT_BIND_ADDRESS : bindAddress;
    }

    /** Creates the network and the volumes of the stack, when they don't exist yet. */
    static void createPersistentResources() throws IOException, InterruptedException {
        if (docker("network", "inspect", NETWORK).exitCode() != 0) {
            docker("network", "create", NETWORK).check("create the network " + NETWORK);
        }
        for (String volume : VOLUMES) {
            // Leaves an existing volume as it is
            docker("volume", "create", volume).check("create the volume " + volume);
        }
    }

    /** A command's exit code and output. */
    record CommandResult(int exitCode, String output) {
        void check(String what) throws IOException {
            if (exitCode != 0) {
                throw new IOException("Couldn't " + what + " with the docker CLI: " + output.strip());
            }
        }
    }

    /** Runs the {@code docker} CLI, and returns its exit code and output. */
    static CommandResult docker(String... arguments) throws IOException, InterruptedException {
        List<String> command = new ArrayList<>();
        command.add("docker");
        command.addAll(List.of(arguments));
        return run(new ProcessBuilder(command), false);
    }

    // Docker Compose on the stack's project, with the address that compose.yaml publishes the ports on
    private static CommandResult compose(Path composeFile, String bindAddress, boolean showOutput,
                                         String... arguments) throws IOException, InterruptedException {
        List<String> command = new ArrayList<>(List.of("docker", "compose", "--file", composeFile.toString(),
                "--project-name", COMPOSE_PROJECT));
        command.addAll(List.of(arguments));
        ProcessBuilder builder = new ProcessBuilder(command);
        builder.environment().put("PERFORMANCE_METRICS_BIND_ADDRESS",
                bindAddress == null || bindAddress.isBlank() ? ReportsUrl.DEFAULT_BIND_ADDRESS : bindAddress);
        return run(builder, showOutput);
    }

    private static CommandResult run(ProcessBuilder builder, boolean showOutput)
            throws IOException, InterruptedException {
        if (showOutput) {
            return new CommandResult(builder.inheritIO().start().waitFor(), "");
        }
        Process process = builder.redirectErrorStream(true).start();
        String output;
        try (InputStream stream = process.getInputStream()) {
            output = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }
        return new CommandResult(process.waitFor(), output);
    }

    private static void awaitHealthy(String url) throws IOException, InterruptedException {
        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
        HttpRequest request = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(5)).GET().build();
        long deadline = System.nanoTime() + STARTUP_TIMEOUT.toNanos();
        while (true) {
            try {
                if (client.send(request, HttpResponse.BodyHandlers.discarding()).statusCode() == 200) {
                    return;
                }
            } catch (IOException e) {
                // Not listening yet
            }
            if (System.nanoTime() > deadline) {
                throw new IOException(url + " didn't answer within " + STARTUP_TIMEOUT.toMinutes() + " minutes");
            }
            Thread.sleep(500);
        }
    }
}
