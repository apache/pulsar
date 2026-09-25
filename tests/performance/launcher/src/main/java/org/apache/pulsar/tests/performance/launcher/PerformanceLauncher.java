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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.profiling.JonoffcpuAgent;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.apache.pulsar.tests.performance.common.YamlScenarioLoader;
import org.apache.pulsar.tests.performance.report.JfrFlamegraphViews;
import org.apache.pulsar.tests.performance.report.MarkdownPages;
import org.apache.pulsar.tests.performance.report.OffCpuFlamegraphs;
import org.apache.pulsar.tests.performance.report.ProfileReport;
import org.apache.pulsar.tests.performance.report.RunInfo;
import org.apache.pulsar.tests.performance.report.RunReport;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "pulsar-performance-launcher", mixinStandardHelpOptions = true)
public class PerformanceLauncher implements Callable<Integer> {
    private static final String ENV_PREFIX = "PULSAR_PERFORMANCE_";
    private static final String CONFIG_ENV = "PULSAR_PERFORMANCE_CONFIG";
    private static final String TOOLS_MOUNT = "/opt/pulsar-performance-tools";
    private static final String CONFIG_MOUNT = "/performance-config/resolved-config.yaml";
    private static final String COORDINATION_MOUNT = "/performance-coordination";
    private static final String OUTPUT_MOUNT = "/performance-output";
    private static final String CONTAINER_LOG = RunReport.CONTAINER_LOG;

    @Option(names = "--config", required = true)
    Path config;

    @Option(names = "--output", description = "Exact run directory, instead of one in the reports hierarchy")
    Path output;

    @Option(names = "--reports-dir", defaultValue = "${sys:performance.reports.dir}",
            description = "Root of the reports hierarchy <root>/<yyyy-MM-dd>/<branch>/<name>/<MM-dd-HH-mm-ss>; "
                    + "default: build/performance in the project directory")
    Path reportsDirectory;

    @Option(names = "--name", description = "The run's name in the reports hierarchy; default: the scenario's "
            + "output.name, else the scenario file name without .yaml")
    String name;

    @Option(names = "--tools-directory", description = "Installed pulsar-performance-tools distribution")
    Path toolsDirectory;

    public static void main(String[] args) {
        System.exit(new CommandLine(new PerformanceLauncher()).execute(args));
    }

    @Override
    public Integer call() throws Exception {
        YamlScenarioLoader loader = new YamlScenarioLoader();
        ObjectNode resolved = loader.resolve(config, null, System.getenv(), ENV_PREFIX, CONFIG_ENV);
        ObjectNode workload = (ObjectNode) loader.select(resolved, "workloads.iotTelemetry");
        ObjectNode clusterConfig = (ObjectNode) loader.select(resolved, "cluster");
        JsonNode profiling = resolved.path("profiling");
        String brokerProfileOptions = text(profiling, "brokerOptions");
        String producerProfileOptions = text(profiling, "producerOptions");
        String consumerProfileOptions = text(profiling, "consumerOptions");
        boolean retainOriginalRecording = booleanValue(profiling, "retainOriginalRecording", true);
        boolean createMeasurementRecording = booleanValue(profiling, "createMeasurementRecording", true);
        Map<String, Object> offCpuOptions = offCpuOptions(loader.mapper(), profiling);
        boolean profilingEnabled = brokerProfileOptions != null || producerProfileOptions != null
                || consumerProfileOptions != null;
        Path agentJar = null;
        if (profilingEnabled) {
            String configuredAgentJar = System.getProperty("performance.jonoffcpu.agent");
            if (!Boolean.parseBoolean(System.getenv("PERFORMANCE_PROFILER_AVAILABLE"))
                    || configuredAgentJar == null) {
                throw new IllegalArgumentException("This scenario enables profiling; run it with "
                        + "./gradlew :tests:performance:launcher:profile");
            }
            agentJar = Path.of(configuredAgentJar).toAbsolutePath().normalize();
        }
        int applications = workload.path("applicationCount").intValue();
        String runId = UUID.randomUUID().toString();
        String clusterName = "iot-" + ProcessHandle.current().pid();
        workload.put("serviceUrl", "pulsar://" + clusterName + "-pulsar-broker-0:6650");

        // Whole seconds, as the run directory names the start
        RunInfo runInfo = RunInfo.collect(Path.of("").toAbsolutePath(),
                ZonedDateTime.now().truncatedTo(ChronoUnit.SECONDS));
        Path runOutput = output != null ? output : RunDirectory.resolve(
                reportsDirectory != null ? reportsDirectory
                        : runInfo.projectDirectory().resolve(RunDirectory.DEFAULT_REPORTS_ROOT),
                runInfo.started(), RunDirectory.branchDirectory(runInfo.gitBranch(), runInfo.gitCommit()),
                runName(resolved));
        runOutput = runOutput.toAbsolutePath().normalize();
        Files.createDirectories(runOutput);
        System.out.println("Run directory: " + runOutput);
        runInfo.write(runOutput);
        Path coordinationDirectory = runOutput.resolve("coordination");
        Files.createDirectories(coordinationDirectory);
        Files.writeString(runOutput.resolve("run-id.txt"), runId + "\n");
        Set<Path> recordingsBeforeRun = JfrRecordingProcessor.findOriginalRecordings(runOutput);
        Path brokerProfileDirectory = runOutput.resolve("broker-profile");
        if (brokerProfileOptions != null) {
            Files.createDirectories(brokerProfileDirectory);
            System.setProperty("inttest.asyncprofiler.opts", brokerProfileOptions);
            System.setProperty("inttest.asyncprofiler.outputformat", "jfr");
        }
        Path resolvedConfig = runOutput.resolve(RunReport.RESOLVED_CONFIG);
        loader.write(resolvedConfig, resolved);
        // The scenario as written, beside its resolved form, so that the run report can link both
        Files.copy(config, runOutput.resolve(config.getFileName()), StandardCopyOption.REPLACE_EXISTING);

        Path resolvedToolsDirectory = (toolsDirectory != null ? toolsDirectory : Path.of(System.getProperty(
                "performance.tools.dir", "tests/performance/tools/build/install/pulsar-performance-tools")))
                .toAbsolutePath().normalize();
        if (!Files.isExecutable(resolvedToolsDirectory.resolve("bin/pulsar-performance-tools"))) {
            throw new IllegalArgumentException(
                    "Build the performance tools distribution first: " + resolvedToolsDirectory);
        }

        @SuppressWarnings("unchecked")
        Map<String, String> brokerEnvs = loader.mapper().convertValue(clusterConfig.path("brokerEnvs"), Map.class);
        @SuppressWarnings("unchecked")
        Map<String, String> bookkeeperEnvs =
                loader.mapper().convertValue(clusterConfig.path("bookkeeperEnvs"), Map.class);
        PulsarClusterSpec spec = PulsarClusterSpec.builder()
                .clusterName(clusterName)
                .numBrokers(clusterConfig.path("brokers").intValue())
                .numBookies(clusterConfig.path("bookies").intValue())
                .numProxies(0)
                .profileBroker(brokerProfileOptions != null)
                .profileDirectory(brokerProfileDirectory.toString())
                .jonoffcpuAgentJar(agentJar != null ? agentJar.toString() : null)
                .jonoffcpuOptions(offCpuOptions)
                .brokerEnvs(brokerEnvs)
                .bookkeeperEnvs(bookkeeperEnvs)
                .build();

        PulsarCluster cluster = PulsarCluster.forSpec(spec);
        List<GenericContainer<?>> consumers = new ArrayList<>(applications);
        GenericContainer<?> producer = null;
        TopicStatsSampler topicStatsSampler = null;
        ZonedDateTime workloadFinished;
        try {
            cluster.start();
            for (int application = 0; application < applications; application++) {
                Path appOutput = applicationOutput(runOutput, workload, application);
                Files.createDirectories(appOutput);
                consumers.add(workloadContainer(cluster, resolvedToolsDirectory, resolvedConfig,
                        coordinationDirectory, runId, appOutput, agentJar, offCpuOptions,
                        consumerProfileOptions, "iot-consume", "--application-index", Integer.toString(application))
                        .waitingFor(Wait.forLogMessage(".*READY application=.*", 1)
                                .withStartupTimeout(Duration.ofMinutes(5))));
            }
            Startables.deepStart(consumers.stream()).join();

            Path producerOutput = runOutput.resolve("producer");
            Files.createDirectories(producerOutput);
            producer = workloadContainer(cluster, resolvedToolsDirectory, resolvedConfig,
                    coordinationDirectory, runId, producerOutput, agentJar, offCpuOptions,
                    producerProfileOptions, "iot-produce");
            topicStatsSampler = startTopicStatsSampler(cluster, workload, runOutput);
            producer.start();
            int timeout = workload.path("consumerTimeoutSeconds").intValue() + 60;
            int producerExit = waitForExit(producer, timeout);
            saveContainerLog(producer, producerOutput.resolve(CONTAINER_LOG));
            if (producerExit != 0) {
                throw new IllegalStateException("IoT producer exited with status " + producerExit);
            }
            for (int application = 0; application < consumers.size(); application++) {
                GenericContainer<?> consumer = consumers.get(application);
                int consumerExit = waitForExit(consumer, timeout);
                saveContainerLog(consumer, applicationOutput(runOutput, workload, application).resolve(CONTAINER_LOG));
                if (consumerExit != 0) {
                    throw new IllegalStateException("IoT consumer exited with status " + consumerExit);
                }
            }
            // The run's end in the charts: every consumer has finished, before the profiles are processed
            workloadFinished = ZonedDateTime.now().truncatedTo(ChronoUnit.SECONDS);
            verifyStates(runOutput, workload, applications);
        } finally {
            if (topicStatsSampler != null) {
                topicStatsSampler.close();
            }
            if (producer != null) {
                saveContainerLog(producer, runOutput.resolve("producer").resolve(CONTAINER_LOG));
                producer.stop();
            }
            for (int application = 0; application < consumers.size(); application++) {
                GenericContainer<?> consumer = consumers.get(application);
                saveContainerLog(consumer, applicationOutput(runOutput, workload, application).resolve(CONTAINER_LOG));
                consumer.stop();
            }
            cluster.stop();
        }
        if (profilingEnabled) {
            JsonNode summary = loader.mapper().readTree(runOutput.resolve("producer/producer-summary.json").toFile());
            Instant measurementStart = Instant.ofEpochMilli(requiredLong(summary, "measurementStartEpochMs"));
            long lastConsumerReceiptEpochMs = Long.MIN_VALUE;
            for (int application = 0; application < applications; application++) {
                JsonNode consumerSummary = loader.mapper().readTree(
                        applicationOutput(runOutput, workload, application).resolve("consumer-summary.json").toFile());
                lastConsumerReceiptEpochMs = Math.max(lastConsumerReceiptEpochMs,
                        requiredLong(consumerSummary, "lastMeasurementMessageReceivedEpochMs"));
            }
            // Consumer timestamps have millisecond precision. Use the following millisecond as the exclusive bound
            // so that events from the millisecond containing the final receipt are retained.
            Instant measurementEnd = Instant.ofEpochMilli(lastConsumerReceiptEpochMs).plusMillis(1);
            Set<Path> recordings = JfrRecordingProcessor.findOriginalRecordings(runOutput);
            recordings.removeAll(recordingsBeforeRun);
            if (recordings.isEmpty()) {
                throw new IllegalStateException("Profiling completed without producing a JFR recording");
            }
            // Correlate against the untouched recording first: the stream binds its size and digest, and
            // retention may delete it afterwards.
            if (offCpuCaptureEnabled(offCpuOptions)) {
                for (Path recording : recordings) {
                    Path outputDirectory = OffCpuFlamegraphs.process(recording,
                            JonoffcpuAgent.capture(recording), measurementStart, measurementEnd);
                    System.out.println("Off-CPU profile: " + outputDirectory);
                }
            }
            JfrRecordingProcessor.process(recordings, measurementStart, measurementEnd,
                    retainOriginalRecording, createMeasurementRecording);
            for (Path recording : recordings) {
                Path source = createMeasurementRecording ? JfrRecordingProcessor.measurementPath(recording)
                        : recording;
                Set<JfrFlamegraphViews.View> views = JfrFlamegraphViews.configuredViews(
                        asyncProfilerOptions(loader.mapper(), recording));
                if (!views.isEmpty() && Files.isRegularFile(source)) {
                    System.out.println("Flame graphs: " + JfrFlamegraphViews.render(recording, source, views));
                }
            }
            ProfileReport.Run run = new ProfileReport.Run(config.getFileName().toString(), runId,
                    measurementStart, measurementEnd, summary.path("messagesPerSecond").asDouble());
            Map<Path, List<Path>> recordingsByDirectory = recordings.stream().sorted()
                    .collect(Collectors.groupingBy(Path::getParent, TreeMap::new, Collectors.toList()));
            for (Map.Entry<Path, List<Path>> entry : recordingsByDirectory.entrySet()) {
                System.out.println("Profile report: "
                        + ProfileReport.write(entry.getKey(), entry.getValue(), run, loader.mapper(), runOutput));
            }
        }
        Path runReport = RunReport.write(runOutput, new RunReport.Run(config.getFileName().toString(), runId,
                PulsarContainer.DEFAULT_IMAGE_NAME, clusterConfig, workload, runInfo, workloadFinished),
                loader.mapper());
        RunDirectory.linkIndexes(runOutput);
        System.out.println("Run report: " + MarkdownPages.htmlPage(runReport));
        return 0;
    }

    /** The run's name in the reports hierarchy: --name, else the scenario's output.name, else its file name. */
    private String runName(JsonNode resolved) {
        if (name != null && !name.isBlank()) {
            return name;
        }
        String scenarioName = resolved.path("output").path("name").textValue();
        if (scenarioName != null && !scenarioName.isBlank()) {
            return scenarioName;
        }
        String fileName = config.getFileName().toString();
        return fileName.replaceFirst("\\.ya?ml$", "");
    }

    /**
     * Starts sampling the workload topics' stats for the run report. Sampling is an observation, so a failure to
     * start it is reported and the run goes on without it.
     */
    private static TopicStatsSampler startTopicStatsSampler(PulsarCluster cluster, JsonNode workload,
                                                            Path runOutput) {
        String prefix = workload.path("topicPrefix").textValue();
        List<String> topics = IntStream.range(0, workload.path("topicCount").intValue())
                .mapToObj(topic -> prefix + topic).toList();
        try {
            return TopicStatsSampler.start(cluster.getAnyBroker().getHttpServiceUrl(), topics, runOutput);
        } catch (Exception e) {
            System.out.println("Topic stats sampling is off for this run: " + e);
            return null;
        }
    }

    /**
     * The async-profiler options a recording was made with, as the agent configuration beside it records them.
     */
    private static String asyncProfilerOptions(ObjectMapper mapper, Path recording) throws IOException {
        Path config = JonoffcpuAgent.config(recording);
        if (!Files.isRegularFile(config)) {
            return null;
        }
        JsonNode options = mapper.readTree(config.toFile()).path("asyncProfilerOptions");
        return options.isTextual() ? options.textValue() : null;
    }

    /**
     * The {@code profiling.offCpu} section as the jonoffcpu agent's {@code sampling} block. Types are kept as
     * the scenario wrote them, so that a quoted probability such as {@code "0.010"} stays a string and is
     * recorded in the capture metadata as spelled.
     */
    private static Map<String, Object> offCpuOptions(ObjectMapper mapper, JsonNode profiling) {
        JsonNode section = profiling.path("offCpu");
        if (section.isMissingNode() || section.isNull()) {
            return Map.of();
        }
        if (!section.isObject()) {
            throw new IllegalArgumentException("profiling.offCpu must be the jonoffcpu agent's sampling block");
        }
        return mapper.convertValue(section, new TypeReference<LinkedHashMap<String, Object>>() { });
    }

    /**
     * Whether the agent records off-CPU samples at all: the admission policy {@code none} runs plain
     * async-profiler through the same agent, leaving nothing to correlate.
     */
    private static boolean offCpuCaptureEnabled(Map<String, Object> offCpuOptions) {
        Object admission = offCpuOptions.get("admission");
        return !(admission instanceof Map<?, ?> policy && "none".equals(policy.get("policy")));
    }

    private GenericContainer<?> workloadContainer(PulsarCluster cluster, Path tools, Path configFile,
                                                   Path coordinationDirectory, String runId,
                                                   Path outputDirectory, Path agentJar,
                                                   Map<String, Object> offCpuOptions, String profileOptions,
                                                   String command, String... extraArguments) throws IOException {
        List<String> arguments = new ArrayList<>();
        arguments.add(TOOLS_MOUNT + "/bin/pulsar-performance-tools");
        arguments.add(command);
        arguments.add("--config");
        arguments.add(CONFIG_MOUNT);
        arguments.add("--output");
        arguments.add(OUTPUT_MOUNT);
        arguments.add("--coordination-directory");
        arguments.add(COORDINATION_MOUNT);
        arguments.add("--run-id");
        arguments.add(runId);
        arguments.addAll(List.of(extraArguments));
        String javaOptions = "-Xms128m -Xmx512m -XX:MaxDirectMemorySize=256m";
        if (profileOptions != null) {
            // The launcher owns the recording name so that it lands inside the run directory
            javaOptions += " -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints "
                    + JonoffcpuAgent.writeConfig(outputDirectory, OUTPUT_MOUNT,
                    "profile-" + command + "-" + System.currentTimeMillis(), profileOptions, offCpuOptions);
        }
        GenericContainer<?> container = new GenericContainer<>(PulsarContainer.DEFAULT_IMAGE_NAME)
                .withNetwork(cluster.getNetwork())
                .withFileSystemBind(tools.toString(), TOOLS_MOUNT, BindMode.READ_ONLY)
                .withFileSystemBind(configFile.toString(), CONFIG_MOUNT, BindMode.READ_ONLY)
                .withFileSystemBind(coordinationDirectory.toString(), COORDINATION_MOUNT, BindMode.READ_WRITE)
                .withFileSystemBind(outputDirectory.toString(), OUTPUT_MOUNT, BindMode.READ_WRITE)
                .withEnv("JAVA_TOOL_OPTIONS", javaOptions)
                .withCommand(arguments.toArray(String[]::new));
        if (profileOptions != null) {
            JonoffcpuAgent.attach(container, agentJar);
        }
        return container;
    }

    private static String text(JsonNode parent, String field) {
        JsonNode value = parent.path(field);
        return value.isTextual() && !value.textValue().isBlank() ? value.textValue() : null;
    }

    private static boolean booleanValue(JsonNode parent, String field, boolean defaultValue) {
        JsonNode value = parent.path(field);
        if (value.isMissingNode() || value.isNull()) {
            return defaultValue;
        }
        if (!value.isBoolean()) {
            throw new IllegalArgumentException("profiling." + field + " must be a boolean");
        }
        return value.booleanValue();
    }

    private static long requiredLong(JsonNode parent, String field) {
        JsonNode value = parent.path(field);
        if (!value.canConvertToLong()) {
            throw new IllegalArgumentException("Missing numeric performance summary field " + field);
        }
        return value.longValue();
    }

    private static int waitForExit(GenericContainer<?> container, int timeoutSeconds) throws Exception {
        return container.getDockerClient().waitContainerCmd(container.getContainerId()).start()
                .awaitStatusCode(timeoutSeconds, TimeUnit.SECONDS);
    }

    private static void saveContainerLog(GenericContainer<?> container, Path path) {
        if (container.getContainerId() == null) {
            return;
        }
        try {
            Files.writeString(path, container.getLogs());
        } catch (Exception ignored) {
            // Preserve the workload result when optional diagnostic log collection fails.
        }
    }

    // An application's outputs are in a directory named after it, as the run report names the application
    private static Path applicationOutput(Path runOutput, JsonNode workload, int application) {
        return RunReport.applicationDirectory(runOutput, workload, application);
    }

    private static void verifyStates(Path output, JsonNode workload, int applications) throws Exception {
        long[] produced = readState(output.resolve("producer/produced-state.bin"));
        for (int application = 0; application < applications; application++) {
            long[] consumed = readState(applicationOutput(output, workload, application).resolve("consumed-state.bin"));
            if (!java.util.Arrays.equals(produced, consumed)) {
                throw new IllegalStateException("Application " + application
                        + " did not receive every device sequence");
            }
        }
    }

    private static long[] readState(Path path) throws Exception {
        try (var input = new DataInputStream(new BufferedInputStream(Files.newInputStream(path)))) {
            int version = input.readInt();
            if (version != 1) {
                throw new IllegalArgumentException("Unsupported sequence state version " + version + " in " + path);
            }
            long[] result = new long[input.readInt()];
            for (int i = 0; i < result.length; i++) {
                result[i] = input.readLong();
            }
            return result;
        }
    }
}
