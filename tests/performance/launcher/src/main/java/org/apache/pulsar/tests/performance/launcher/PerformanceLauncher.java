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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.api.model.Info;
import io.github.merlimat.slog.Logger;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.tests.integration.containers.BrokerContainer;
import org.apache.pulsar.tests.integration.containers.CSContainer;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.containers.ZKContainer;
import org.apache.pulsar.tests.integration.profiling.JonoffcpuAgent;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.apache.pulsar.tests.performance.common.YamlScenarioLoader;
import org.apache.pulsar.tests.performance.report.DockerEngine;
import org.apache.pulsar.tests.performance.report.JfrFlamegraphViews;
import org.apache.pulsar.tests.performance.report.MarkdownPages;
import org.apache.pulsar.tests.performance.report.NettyAllocatorEvents;
import org.apache.pulsar.tests.performance.report.OffCpuFlamegraphs;
import org.apache.pulsar.tests.performance.report.ProfileReport;
import org.apache.pulsar.tests.performance.report.ReportsUrl;
import org.apache.pulsar.tests.performance.report.RunInfo;
import org.apache.pulsar.tests.performance.report.RunReport;
import org.apache.pulsar.tests.performance.tools.IotScenario;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
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
    // The producer's measurement control endpoints, inside its container
    private static final int CONTROL_PORT = 8089;
    // The proxy only forwards the launcher's admin requests, which needs little memory
    static final Map<String, String> PROXY_ENV =
            Map.of("PULSAR_MEM", "-Xms96m -Xmx256m -XX:MaxDirectMemorySize=128m");
    private static final String OUTPUT_MOUNT = "/performance-output";
    // Where the one-off container that merges the JFR configurations sees the .jfc files and its output directory
    private static final String JFC_MOUNT = "/jfr";
    private static final String MERGE_OUTPUT_MOUNT = "/jfr-output";
    // Where PulsarContainer binds a profiled broker's profile directory
    private static final String BROKER_PROFILE_MOUNT = "/profiles";
    // The merged JFR configuration of a profiled component, in its profile or output directory
    static final String JFR_CONFIGURATION_FILE = "jfr-configuration.jfc";
    static final String JAVA_TOOL_OPTIONS = "JAVA_TOOL_OPTIONS";
    static final String PULSAR_MEM = "PULSAR_MEM";
    // The JVM options that Pulsar's scripts put last on a Pulsar component's command line
    static final String PULSAR_EXTRA_OPTS = "PULSAR_EXTRA_OPTS";
    // A workload's heap and direct memory, unless the workload's gateways.env or applications.env set PULSAR_MEM
    static final String WORKLOAD_MEMORY = "-Xms128m -Xmx512m -XX:MaxDirectMemorySize=256m";
    private static final String CONTAINER_LOG = RunReport.CONTAINER_LOG;
    // The gateways' outputs, named after them as the run report names them
    private static final String GATEWAYS_DIRECTORY = "gateways";
    // The applications' outputs: their container's, and a directory per application
    private static final String APPLICATIONS_DIRECTORY = RunReport.APPLICATIONS_DIRECTORY;
    // The remote applications' outputs, as the network impairments name their container
    static final String REMOTE_APPLICATIONS_DIRECTORY = RunReport.REMOTE_APPLICATIONS_DIRECTORY;
    // The remote applications' configuration: the resolved one with their service URL and their settings
    static final String REMOTE_APPLICATIONS_CONFIG = RunReport.REMOTE_APPLICATIONS_CONFIG;
    // The remote cluster's topic stats, as topic-stats.csv has the first cluster's
    static final String REMOTE_TOPIC_STATS = RunReport.REMOTE_TOPIC_STATS_FILE;
    // The topics that a remote cluster gets: those of the namespace whose replication clusters include it
    private static final Pattern REPLICATED_TOPICS = Pattern.compile("(persistent://)?public/default/.+");
    // Every log goes to this file in the run directory, and the console shows only the launcher's own messages
    static final String LAUNCHER_LOG = "launcher.log";
    private static final String LOG_CONFIGURATION = "performance-launcher-log4j2.xml";
    private static final DateTimeFormatter STATUS_TIME = DateTimeFormatter.ofPattern("HH:mm:ss", Locale.ROOT);

    @Option(names = "--scenario", required = true, description = "The scenario file")
    Path scenario;

    @Option(names = "--extends", paramLabel = "<scenario>",
            description = "Merge this scenario file on top of the scenario, as if the scenario extended it last, "
                    + "such as configs/profile-broker to profile the broker or configs/iot-telemetry-high-mem to give "
                    + "it more memory. A relative path is looked for in the --scenario "
                    + "file's directory, then in the working directory, and .yaml may be left out; an absolute "
                    + "path is used as given. Repeatable, applied in order")
    List<Path> extendedScenarios = new ArrayList<>();

    @Option(names = "--set", paramLabel = "<path>=<value>",
            description = "Set a value of the resolved scenario, such as workloads.iotTelemetry.rate=5000, after the "
                    + "inheritance and the environment overrides. Repeatable, applied in order")
    List<String> settings = new ArrayList<>();

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

    @Option(names = "--cooldown-temperature", defaultValue = "${sys:performance.cooldown.temperature}",
            description = "Before starting the cluster, wait until the CPU package temperature is at most this "
                    + "many °C, so that runs start from comparable thermal conditions; default: no wait")
    Double cooldownCelsius;

    @Option(names = "--cooldown-timeout", defaultValue = "600",
            description = "The longest wait for --cooldown-temperature, in seconds; the run starts anyway after it")
    int cooldownTimeoutSeconds;

    @Option(names = "--progress-interval", defaultValue = "10",
            description = "Report the workload's throughput, latency and backlog every this many seconds")
    int progressIntervalSeconds;

    @Option(names = "--keep-launcher-log", defaultValue = "${sys:performance.keepLauncherLog:-false}",
            description = "Keep launcher.log when the run succeeds; without it, a successful run deletes it, since the "
                    + "containers' logs make it large. It is written during the run, so that it can be followed, and "
                    + "a failed run keeps it, as does a run whose applications received duplicates, ordering "
                    + "violations or invalid messages")
    boolean keepLauncherLog;

    @Option(names = "--metrics", negatable = true, defaultValue = "${sys:performance.metrics:-true}",
            fallbackValue = "true", description = "Have VictoriaMetrics scrape the metrics of the brokers, the "
                    + "bookies and ZooKeeper during the run, the default: the running metrics stack's, or else the "
                    + "stack started for the run. --no-metrics doesn't. See docs/metrics.md")
    boolean metrics;

    @Option(names = "--perf-stat", negatable = true, defaultValue = "${sys:performance.perfStat:-true}",
            fallbackValue = "true", description = "Count each container's CPU time, context switches, CPU "
                    + "migrations, cycles and instructions with perf stat in a privileged sidecar container, the "
                    + "default on Linux; --no-perf-stat doesn't. The containers' CPU use and voluntary and involuntary "
                    + "context switches are sampled from /proc either way. See docs/run-reports.md")
    boolean perfStat;

    @Option(names = "--procfs", defaultValue = "/proc", hidden = true)
    Path procfs;

    @Option(names = "--cgroupfs", defaultValue = "/sys/fs/cgroup", hidden = true)
    Path cgroupfs;

    @Option(names = "--sysfs", defaultValue = "/sys", hidden = true)
    Path sysfs;

    public static void main(String[] args) {
        if (System.getProperty("log4j2.configurationFile") == null) {
            System.setProperty("log4j2.configurationFile", LOG_CONFIGURATION);
        }
        System.exit(new CommandLine(new PerformanceLauncher())
                .setExecutionExceptionHandler((e, commandLine, parseResult) -> {
                    if (!(e instanceof ReportedFailure)) {
                        reportFailure(e);
                    }
                    return commandLine.getCommandSpec().exitCodeOnExecutionException();
                })
                .execute(args));
    }

    /**
     * A failure that the launcher has reported already, when it happened: the launcher throws it so that the run
     * fails without reporting it again, after it has shut the cluster down.
     */
    static final class ReportedFailure extends Exception {
        ReportedFailure(Throwable cause) {
            super(cause.getMessage(), cause);
        }
    }

    /**
     * Reports a failure on the console in one line, and its stack trace in the launcher's log. The console shows
     * where the log is, and the containers' logs when the failure came from a workload.
     */
    static void reportFailure(Throwable e) {
        status("The run failed: " + Objects.requireNonNullElse(e.getMessage(), e.toString()));
        log().error().exception(e).log("The run failed");
        String launcherLog = System.getProperty("performance.launcher.log");
        if (launcherLog != null) {
            status("Stack trace: " + launcherLog);
        }
    }

    @Override
    public Integer call() throws Exception {
        YamlScenarioLoader loader = new YamlScenarioLoader();
        List<Path> appendedScenarios = extendedScenarios.stream().map(this::appendedScenario).toList();
        ObjectNode resolved =
                loader.resolve(scenario, appendedScenarios, null, System.getenv(), ENV_PREFIX, CONFIG_ENV);
        settings.forEach(setting -> loader.set(resolved, setting));
        ObjectNode workload = (ObjectNode) loader.select(resolved, "workloads.iotTelemetry");
        JsonNode profiling = resolved.path("profiling");
        ProfilingSettings profilingSettings = ProfilingSettings.read(loader.mapper(), profiling);
        ClusterSettings clusterSettings = ClusterSettings.read(loader.mapper(), resolved.path("cluster"));
        HeapDumpSettings heapDumpSettings = HeapDumpSettings.read(resolved.path("heapDumps"));
        MetricsSettings metricsSettings = MetricsSettings.read(resolved.path("metrics"));
        OffloadSettings offloadSettings = OffloadSettings.read(resolved.path("offload"));
        SeekSettings seekSettings = offloadSettings != null ? offloadSettings.seeks() : null;
        NetworkSettings.Topology topology = clusterSettings.topology().withOffloadStorage(offloadSettings != null);
        ClusterSettings.Remote remote = clusterSettings.remote();
        NetworkSettings networkSettings = NetworkSettings.read(resolved.path("network"), topology,
                measurementSeconds(workload));
        // The cluster as the scenario wrote it, for the run report
        ObjectNode clusterConfig = (ObjectNode) resolved.get("cluster");
        boolean profilingEnabled = profilingSettings.anyProfiled();
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
        int applications = workload.path("applications").path("count").intValue();
        String runId = UUID.randomUUID().toString();
        String clusterName = "iot-" + ProcessHandle.current().pid();
        workload.put("serviceUrl", clusterSettings.serviceUrl(clusterName));
        checkWorkload(loader.mapper(), workload);
        if (offloadSettings != null) {
            offloadSettings.checkTopics(workload.path("topics").path("prefix").textValue());
        }
        String remoteClusterName = ClusterSettings.remoteClusterName(clusterName);
        ObjectNode remoteWorkload = null;
        if (remote != null) {
            remoteWorkload = remoteApplicationsWorkload(workload, remote, remote.serviceUrl(clusterName));
            checkWorkload(loader.mapper(), remoteWorkload);
        }
        int remoteApplications = remoteWorkload != null ? remoteWorkload.path("applications").path("count").intValue()
                : 0;

        // Whole seconds, as the run directory names the start
        RunInfo runInfo = RunInfo.collect(Path.of("").toAbsolutePath(),
                ZonedDateTime.now().truncatedTo(ChronoUnit.SECONDS));
        // ZooKeeper, the bookies and the brokers run a released Pulsar in a test image built on it, see
        // -Pperformance.clusterPulsarImage
        String clusterPulsarImage = System.getProperty("performance.cluster.pulsarImage");
        String clusterImage = System.getProperty("performance.cluster.image");
        if ((clusterPulsarImage == null) != (clusterImage == null)) {
            throw new IllegalArgumentException("Set both performance.cluster.pulsarImage and"
                    + " performance.cluster.image, or neither; ./gradlew :tests:performance:launcher:run"
                    + " -Pperformance.clusterPulsarImage=<image> sets both");
        }
        if (clusterPulsarImage != null) {
            runInfo = runInfo.withCluster(new RunInfo.Cluster(clusterPulsarImage, ""));
        }
        if (offloadSettings != null && clusterImage != null) {
            requireOffloader(clusterImage, clusterPulsarImage);
        }
        Path reportsRoot = reportsDirectory != null ? reportsDirectory
                : runInfo.projectDirectory().resolve(RunDirectory.DEFAULT_REPORTS_ROOT);
        Path runOutput = output != null ? output : RunDirectory.resolve(reportsRoot, runInfo.started(),
                clusterPulsarImage != null ? RunDirectory.clusterDirectory(clusterPulsarImage)
                        : RunDirectory.branchDirectory(runInfo.gitBranch(), runInfo.gitCommit()),
                runName(resolved));
        runOutput = runOutput.toAbsolutePath().normalize();
        Files.createDirectories(runOutput);
        copyConsoleTo(runOutput.resolve(RunReport.CONSOLE_LOG));
        System.out.println("Run directory: " + runOutput);
        Path launcherLog = runOutput.resolve(LAUNCHER_LOG);
        // Before anything logs, which is when the logging reads its configuration
        System.setProperty("performance.launcher.log", launcherLog.toString());
        status("Logs: " + launcherLog);
        runInfo = runInfo.withDockerEngine(dockerEngine());
        runInfo.write(runOutput);
        Path coordinationDirectory = runOutput.resolve("coordination");
        Files.createDirectories(coordinationDirectory);
        Files.writeString(runOutput.resolve("run-id.txt"), runId + "\n");
        Set<Path> recordingsBeforeRun = JfrRecordingProcessor.findOriginalRecordings(runOutput);
        Path brokerProfileDirectory = runOutput.resolve("broker-profile");
        if (profilingEnabled) {
            // Each component's JFR configuration goes beside its recordings: the broker's profile directory is its
            // /profiles, and a workload's output directory its output mount. The brokers run the cluster's image,
            // the gateways and the applications the test image.
            String brokerImage = clusterImage != null ? clusterImage : PulsarContainer.DEFAULT_IMAGE_NAME;
            profilingSettings = mergeJfrConfigurations(profilingSettings,
                    runInfo.projectDirectory().resolve(ProfilingSettings.JFR_CONFIGURATIONS_DIRECTORY), Map.of(
                            ProfilingSettings.BROKER, new JfrOutput(brokerImage, brokerProfileDirectory,
                                    BROKER_PROFILE_MOUNT),
                            ProfilingSettings.GATEWAYS, new JfrOutput(PulsarContainer.DEFAULT_IMAGE_NAME,
                                    runOutput.resolve(GATEWAYS_DIRECTORY), OUTPUT_MOUNT),
                            ProfilingSettings.APPLICATIONS, new JfrOutput(PulsarContainer.DEFAULT_IMAGE_NAME,
                                    runOutput.resolve(APPLICATIONS_DIRECTORY), OUTPUT_MOUNT)));
        }
        if (profilingSettings.broker().profiled()) {
            Files.createDirectories(brokerProfileDirectory);
            System.setProperty("inttest.asyncprofiler.opts", profilingSettings.broker().asyncProfilerOptions());
            System.setProperty("inttest.asyncprofiler.outputformat", "jfr");
        }
        Map<String, Path> heapDumpDirectories = heapDumpSettings.any() ? HeapDumper.prepare(runOutput) : Map.of();
        // The brokers update the stats that their metrics show every metrics interval, also when no metrics are
        // collected, so that runs with and without them are alike
        Map<String, String> brokerEnv = metricsSettings.withBrokerStatsSettings(clusterSettings.brokers().env());
        if (offloadSettings != null) {
            brokerEnv = offloadSettings.withBrokerOffloadSettings(brokerEnv);
        }
        Map<String, String> brokerMounts = new LinkedHashMap<>();
        if (heapDumpSettings.broker().any()) {
            brokerMounts.put(heapDumpDirectories.get(HeapDumpSettings.BROKER).toString(), HeapDumper.MOUNT);
        }
        if (heapDumpSettings.broker().onOutOfMemoryError()) {
            // The test image's scripts put -XX:HeapDumpPath=/var/log/pulsar into the broker's command line, which the
            // broker's extra options come after
            brokerEnv = withJvmOptions(brokerEnv, PULSAR_EXTRA_OPTS,
                    HeapDumper.outOfMemoryOptions(heapDumpSettings.gzipLevel()));
        }
        Path resolvedConfig = runOutput.resolve(RunReport.RESOLVED_CONFIG);
        if (cooldownCelsius != null) {
            // The workloads wait while the host cools down before the measurement; give them the time for it
            workload.put("timeoutSeconds", workload.path("timeoutSeconds").intValue()
                    + cooldownTimeoutSeconds);
            if (remoteWorkload != null) {
                remoteWorkload.put("timeoutSeconds", remoteWorkload.path("timeoutSeconds").intValue()
                        + cooldownTimeoutSeconds);
            }
        }
        loader.write(resolvedConfig, resolved);
        Path remoteConfig = runOutput.resolve(REMOTE_APPLICATIONS_CONFIG);
        if (remoteWorkload != null) {
            ObjectNode remoteResolved = resolved.deepCopy();
            ((ObjectNode) remoteResolved.get("workloads")).set("iotTelemetry", remoteWorkload);
            loader.write(remoteConfig, remoteResolved);
        }
        status("Scenario " + scenarioName(resolved) + " ("
                + Stream.concat(Stream.of(scenario), appendedScenarios.stream())
                        .map(file -> file.getFileName().toString()).collect(Collectors.joining(" + "))
                + (settings.isEmpty() ? "" : ", " + String.join(", ", settings)) + "), resolved:");
        System.out.print(indent(loader.mapper().writerWithDefaultPrettyPrinter().writeValueAsString(resolved)
                .replaceFirst("^---\\R", "")));
        // The scenario as written, beside its resolved form, so that the run report can link both
        Files.copy(scenario, runOutput.resolve(scenario.getFileName()), StandardCopyOption.REPLACE_EXISTING);
        for (Path appendedScenario : appendedScenarios) {
            Files.copy(appendedScenario, runOutput.resolve(appendedScenario.getFileName()),
                    StandardCopyOption.REPLACE_EXISTING);
        }

        Path resolvedToolsDirectory = (toolsDirectory != null ? toolsDirectory : Path.of(System.getProperty(
                "performance.tools.dir", "tests/performance/tools/build/install/pulsar-performance-tools")))
                .toAbsolutePath().normalize();
        if (!Files.isExecutable(resolvedToolsDirectory.resolve("bin/pulsar-performance-tools"))) {
            throw new IllegalArgumentException(
                    "Build the performance tools distribution first: " + resolvedToolsDirectory);
        }

        Map<String, String> gatewaysEnv = ClusterSettings.env(loader.mapper(), workload.path("gateways").path("env"),
                "workloads.iotTelemetry.gateways.env");
        Map<String, String> applicationsEnv = ClusterSettings.env(loader.mapper(),
                workload.path("applications").path("env"), "workloads.iotTelemetry.applications.env");
        PulsarClusterSpec spec = PulsarClusterSpec.builder()
                .clusterName(clusterName)
                .numBrokers(clusterSettings.brokers().replicas())
                .numBookies(clusterSettings.bookies().replicas())
                // The launcher's admin requests go through the proxy, which forwards each to the broker that owns
                // the topic: a broker redirects them to the owner's name in the cluster's network, which the
                // launcher's host can't resolve
                .numProxies(1)
                .proxyEnvs(PROXY_ENV)
                .profileBroker(profilingSettings.broker().profiled())
                .profileDirectory(brokerProfileDirectory.toString())
                .jonoffcpuAgentJar(agentJar != null ? agentJar.toString() : null)
                .jonoffcpuOptions(profilingSettings.broker().offCpuOptions())
                .clusterImage(clusterImage)
                .brokerEnvs(brokerEnv)
                .brokerMountFiles(brokerMounts)
                .bookkeeperEnvs(metricsSettings.withBookieStatsSettings(clusterSettings.bookies().containerEnv()))
                .build();
        // The remote cluster runs the same image, without profiling, heap dumps or mounts
        PulsarClusterSpec remoteSpec = remote == null ? null : PulsarClusterSpec.builder()
                .clusterName(remoteClusterName)
                .numBrokers(remote.brokers().replicas())
                .numBookies(remote.bookies().replicas())
                // The launcher's admin requests go through the proxy, which forwards each to the broker that owns
                // the topic: a broker redirects them to the owner's name in the cluster's network, which the
                // launcher's host can't resolve
                .numProxies(1)
                .proxyEnvs(PROXY_ENV)
                .clusterImage(clusterImage)
                .brokerEnvs(metricsSettings.withBrokerStatsSettings(remote.brokers().env()))
                .bookkeeperEnvs(metricsSettings.withBookieStatsSettings(remote.bookies().containerEnv()))
                .build();

        HostStatsSampler.Sensors sensors = HostStatsSampler.discover(sysfs);
        List<RunReport.Cooldown> cooldowns = new CopyOnWriteArrayList<>();
        RunReport.Cooldown beforeRun = coolDown(sensors, RunReport.Cooldown.BEFORE_RUN);
        if (beforeRun != null) {
            cooldowns.add(beforeRun);
        }
        Thread measurementGate = null;
        // With a remote cluster, both clusters share a configuration store, which makes the namespace public/default
        // replicate to both: initializing a cluster's metadata adds the cluster to the namespace's replication
        // clusters. Both are in the store's network.
        CSContainer configurationStore = remote == null ? null
                : new CSContainer(clusterName, clusterImage != null ? clusterImage : PulsarContainer.DEFAULT_IMAGE_NAME)
                        .withNetwork(Network.newNetwork())
                        .withNetworkAliases(CSContainer.NAME);
        PulsarCluster cluster = configurationStore == null ? PulsarCluster.forSpec(spec)
                : PulsarCluster.forSpec(spec, configurationStore);
        PulsarCluster remoteCluster = configurationStore == null ? null
                : PulsarCluster.forSpec(remoteSpec, configurationStore);
        // The offloads' tiered storage, on the cluster's network
        GenericContainer<?> offloadStorage = offloadSettings != null
                ? offloadSettings.storageContainer(cluster.getNetwork()) : null;
        // The launcher closes the cluster's network after the storage, which is on it, when the network is the
        // cluster's own
        boolean closeClusterNetwork = offloadStorage != null && remoteCluster == null
                && !PulsarContainer.PULSAR_CONTAINERS_LEAVE_RUNNING;
        if (closeClusterNetwork) {
            cluster.closeNetworkOnExit = false;
        }
        if (remoteCluster != null) {
            // The launcher closes the shared network after both clusters
            cluster.closeNetworkOnExit = false;
            remoteCluster.closeNetworkOnExit = false;
        }
        // The containers are created, not started yet
        Map<String, String> journalTmpfsMount = clusterSettings.bookies().journalTmpfsMount();
        if (!journalTmpfsMount.isEmpty()) {
            cluster.getBookies().forEach(bookie -> bookie.withTmpFs(journalTmpfsMount));
        }
        if (remoteCluster != null && !remote.bookies().journalTmpfsMount().isEmpty()) {
            remoteCluster.getBookies().forEach(bookie -> bookie.withTmpFs(remote.bookies().journalTmpfsMount()));
        }
        Path remoteApplicationsOutput = runOutput.resolve(REMOTE_APPLICATIONS_DIRECTORY);
        GenericContainer<?> remoteConsumer = null;
        Path applicationsOutput = runOutput.resolve(APPLICATIONS_DIRECTORY);
        GenericContainer<?> consumer = null;
        GenericContainer<?> producer = null;
        TopicStatsSampler topicStatsSampler = null;
        TopicStatsSampler remoteTopicStatsSampler = null;
        ProgressMonitor progress = null;
        HostStatsSampler hostStatsSampler = startHostStatsSampler(sensors, runOutput);
        // The sidecar in the Docker engine's host counts the containers' CPU events, and reads the engine host's
        // counters when that is a VM, such as Docker Desktop's or OrbStack's on macOS
        PerfStatSidecar perfStatSidecar = perfStat ? startPerfStatSidecar(runOutput) : null;
        boolean engineOnThisHost = engineOnThisHost(perfStatSidecar);
        HostIoSampler hostIoSampler = startHostIoSampler(engineOnThisHost || perfStatSidecar == null
                ? new HostIoSampler.LocalSource(procfs, sysfs) : perfStatSidecar.hostSource(), runOutput);
        ContainerStatsSampler containerStatsSampler = null;
        HeapDumper heapDumper = heapDumpSettings.any()
                ? new HeapDumper(runOutput, PulsarContainer.DEFAULT_IMAGE_NAME, heapDumpSettings.gzipLevel()) : null;
        MetricsCollection metricsCollection = null;
        NetworkImpairments network = null;
        JoinPreparation joinPreparation = null;
        Exception networkCleanupFailure = null;
        Instant gatewaysStarted = null;
        // When the seeks after the workloads ended, or 0 without them
        long seeksEndEpochMs = 0;
        String metricsBindAddress = System.getProperty("performance.metrics.bindAddress",
                ReportsUrl.DEFAULT_BIND_ADDRESS);
        ZonedDateTime workloadFinished;
        try {
            String journalTmpfs = clusterSettings.bookies().journalTmpfs();
            status(String.format(Locale.ROOT, "Starting the Pulsar cluster: %d broker(s), %d bookie(s), %d proxy%s",
                    spec.numBrokers(), spec.numBookies(), spec.numProxies(),
                    journalTmpfs != null ? ", with each bookie's journal on a tmpfs of " + journalTmpfs : ""));
            if (networkSettings.any()) {
                // Before the cluster, so that a Docker engine that can't apply them fails the run at once
                network = NetworkImpairments.start(networkSettings, topology, runOutput, PerformanceLauncher::status);
                status("Network impairments: " + (networkSettings.impairments().isEmpty() ? "none, offloads off"
                        : networkSettings.impairments().stream().map(NetworkSettings.Impairment::name)
                        .collect(Collectors.joining(", "))) + "; the Docker engine's host can apply them");
            }
            long clusterStart = System.nanoTime();
            if (configurationStore != null) {
                configurationStore.start();
            }
            if (offloadStorage != null) {
                // Before the brokers, which offload to it
                status("Starting the offload storage: " + offloadSettings.image());
                offloadStorage.start();
            }
            cluster.start();
            status(String.format(Locale.ROOT, "Started the Pulsar cluster in %.0f s",
                    (System.nanoTime() - clusterStart) / 1e9));
            if (remoteCluster != null) {
                long remoteStart = System.nanoTime();
                status(String.format(Locale.ROOT, "Starting the remote cluster %s, which public/default replicates"
                        + " to: %d broker(s), %d bookie(s), %d proxy", remoteClusterName, remoteSpec.numBrokers(),
                        remoteSpec.numBookies(), remoteSpec.numProxies()));
                remoteCluster.start();
                status(String.format(Locale.ROOT, "Started the remote cluster in %.0f s",
                        (System.nanoTime() - remoteStart) / 1e9));
            }
            if (clusterPulsarImage != null) {
                String clusterVersion = brokerVersion(cluster);
                runInfo = runInfo.withCluster(new RunInfo.Cluster(clusterPulsarImage, clusterVersion));
                runInfo.write(runOutput);
                status("The cluster runs Pulsar " + (clusterVersion.isEmpty() ? "of unknown version" : clusterVersion)
                        + " from " + clusterPulsarImage);
            }
            if (offloadSettings != null) {
                setOffloadPolicies(cluster, offloadSettings);
            }
            if (metrics) {
                String composeFile = System.getProperty("performance.metrics.composeFile");
                metricsCollection = MetricsCollection.start(cluster, composeFile != null ? Path.of(composeFile) : null,
                        metricsBindAddress, MetricsCollection.clusterLabel(reportsRoot, runOutput), runId,
                        metricsSettings, PerformanceLauncher::status);
            }
            status(String.format(Locale.ROOT, "Starting %d application(s) with %d pod(s) each",
                    applications, workload.path("applications").path("podsPerApplication").intValue()));
            // One container runs every application, as one runs every gateway; each application writes into its
            // directory in the applications' directory
            Files.createDirectories(applicationsOutput);
            consumer = workloadContainer(cluster, resolvedToolsDirectory, resolvedConfig, coordinationDirectory,
                    runId, applicationsOutput, agentJar, profilingSettings.applications(), APPLICATIONS_DIRECTORY,
                    applicationsEnv, heapDumpSettings.applications(), heapDumpSettings.gzipLevel(),
                    heapDumpDirectories.get(HeapDumpSettings.APPLICATIONS), "iot-consume", "--control-port",
                    Integer.toString(CONTROL_PORT))
                    .withExposedPorts(CONTROL_PORT);
            long applicationsStart = System.nanoTime();
            startWorkload(consumer, ".*READY applications=.*", "The applications",
                    applicationsOutput.resolve(CONTAINER_LOG));
            status(String.format(Locale.ROOT, "Started the applications in %.0f s",
                    (System.nanoTime() - applicationsStart) / 1e9));
            if (remoteWorkload != null) {
                status(String.format(Locale.ROOT, "Starting %d remote application(s) with %d pod(s) each, which"
                        + " consume the replicated messages in the remote cluster", remoteApplications,
                        remoteWorkload.path("applications").path("podsPerApplication").intValue()));
                Files.createDirectories(remoteApplicationsOutput);
                // Their own coordination directory: the gateways wait only for the first cluster's applications in
                // the warmup, since a replicated warmup message arrives later
                Path remoteCoordinationDirectory = runOutput.resolve("coordination-remote");
                Files.createDirectories(remoteCoordinationDirectory);
                remoteConsumer = workloadContainer(cluster, resolvedToolsDirectory, remoteConfig,
                        remoteCoordinationDirectory, runId, remoteApplicationsOutput, agentJar,
                        ProfilingSettings.Component.NONE, REMOTE_APPLICATIONS_DIRECTORY,
                        remoteApplicationsEnv(applicationsEnv, remote), HeapDumpSettings.Component.NONE,
                        heapDumpSettings.gzipLevel(), null, "iot-consume", "--control-port",
                        Integer.toString(CONTROL_PORT))
                        .withExposedPorts(CONTROL_PORT);
                long remoteApplicationsStart = System.nanoTime();
                startWorkload(remoteConsumer, ".*READY applications=.*", "The remote applications",
                        remoteApplicationsOutput.resolve(CONTAINER_LOG));
                status(String.format(Locale.ROOT, "Started the remote applications in %.0f s",
                        (System.nanoTime() - remoteApplicationsStart) / 1e9));
            }
            topicStatsSampler = startTopicStatsSampler(cluster, workload, runOutput, remoteCluster != null,
                    TopicStatsSampler.FILE_NAME);
            if (remoteCluster != null) {
                // The remote subscriptions' backlog tells messages that the remote topics lack from messages that the
                // remote applications didn't receive
                remoteTopicStatsSampler = startTopicStatsSampler(remoteCluster, workload, runOutput, false,
                        REMOTE_TOPIC_STATS);
            }
            IotScenario iotScenario = loader.mapper().treeToValue(workload, IotScenario.class);
            if (offloadSettings != null || iotScenario.preparesJoin()) {
                // It records the offload status when the late applications join, and at the run's end
                joinPreparation = JoinPreparation.start(cluster.getHttpServiceUrl(), iotScenario,
                        offloadSettings != null, runOutput, coordinationDirectory, runId, PerformanceLauncher::status);
            }
            TopicStatsSampler backlogSource = topicStatsSampler;
            progress = new ProgressMonitor(loader.mapper(), System.out,
                    workload.path("payload").path("size").intValue(),
                    applications, () -> backlogSource != null ? backlogSource.latestBacklog() : null);
            GenericContainer<?> runningConsumer = consumer;
            progress.follow(APPLICATIONS_DIRECTORY, controlUrl(consumer), runningConsumer::isRunning);
            if (remoteConsumer != null) {
                GenericContainer<?> runningRemoteConsumer = remoteConsumer;
                TopicStatsSampler remoteBacklogSource = remoteTopicStatsSampler;
                progress.followRemote(REMOTE_APPLICATIONS_DIRECTORY, remoteApplications,
                        () -> remoteBacklogSource != null ? remoteBacklogSource.latestBacklog() : null,
                        controlUrl(remoteConsumer), runningRemoteConsumer::isRunning);
            }

            Path producerOutput = runOutput.resolve(GATEWAYS_DIRECTORY);
            Files.createDirectories(producerOutput);
            boolean gatedMeasurement = cooldownCelsius != null || networkSettings.anyMeasurementAnchored();
            List<String> producerArguments = new ArrayList<>(List.of("--control-port", Integer.toString(CONTROL_PORT)));
            if (gatedMeasurement) {
                producerArguments.add("--await-measurement-start");
            }
            if (network != null) {
                producerArguments.add("--await-run-start");
            }
            producer = workloadContainer(cluster, resolvedToolsDirectory, resolvedConfig,
                    coordinationDirectory, runId, producerOutput, agentJar, profilingSettings.gateways(),
                    GATEWAYS_DIRECTORY, gatewaysEnv, heapDumpSettings.gateways(), heapDumpSettings.gzipLevel(),
                    heapDumpDirectories.get(HeapDumpSettings.GATEWAYS), "iot-produce",
                    producerArguments.toArray(String[]::new));
            // The launcher reaches the producer's control endpoints through the port mapped on the host
            producer.withExposedPorts(CONTROL_PORT);
            status(String.format(Locale.ROOT, "Starting the gateways: %,d warmup and %,d measured message(s) at "
                            + "%,d msg/s from %d gateway(s) to %d topic(s)",
                    warmupMessageCount(workload), measurementMessageCount(workload), workload.path("rate").intValue(),
                    workload.path("gateways").path("count").intValue(),
                    workload.path("topics").path("count").intValue()));
            startWorkload(producer, ".*CONTROL_READY.*", "The gateways", producerOutput.resolve(CONTAINER_LOG));
            if (network != null) {
                // The gateways wait before they create their clients, so that their first lookups and connections
                // already have the whole-run conditions
                Map<String, GenericContainer<?>> endpoints = networkEndpoints(cluster, clusterName, producer,
                        consumer, remoteCluster, remoteClusterName, remoteConsumer);
                if (offloadStorage != null) {
                    endpoints.put(NetworkSettings.OFFLOAD_STORAGE, offloadStorage);
                }
                network.setUp(endpoints, cluster.getNetwork().getId());
                network.startRun();
                startRun(producer);
            }
            gatewaysStarted = Instant.now();
            GenericContainer<?> runningProducer = producer;
            List<MeasuredContainer> measured = measuredContainers(cluster, clusterName, producer, consumer);
            if (remoteCluster != null) {
                measured = new ArrayList<>(measured);
                measured.addAll(remoteContainers(remoteCluster, remoteClusterName, remoteConsumer));
            }
            if (offloadStorage != null) {
                measured = new ArrayList<>(measured);
                measured.add(new MeasuredContainer(NetworkSettings.OFFLOAD_STORAGE, offloadStorage));
            }
            boolean counted = perfStatSidecar != null && countContainers(perfStatSidecar, measured);
            containerStatsSampler = startContainerStatsSampler(measured, engineOnThisHost,
                    counted ? perfStatSidecar : null, runOutput);
            if (heapDumper != null) {
                heapDumper.start(heapDumpTargets(cluster, heapDumpSettings, producer, consumer));
            }
            progress.follow("producer", controlUrl(producer), runningProducer::isRunning);
            progress.start(progressIntervalSeconds);
            if (gatedMeasurement) {
                measurementGate = startMeasurementGate(sensors, producer, cooldowns, network);
            }
            List<Workload> workloads = new ArrayList<>();
            workloads.add(new Workload("The gateways", producer, producerOutput.resolve(CONTAINER_LOG)));
            workloads.add(new Workload("The applications", consumer, applicationsOutput.resolve(CONTAINER_LOG)));
            if (remoteConsumer != null) {
                workloads.add(new Workload("The remote applications", remoteConsumer,
                        remoteApplicationsOutput.resolve(CONTAINER_LOG)));
            }
            CompletableFuture<?> runFailure = network != null ? network.failure() : new CompletableFuture<>();
            if (joinPreparation != null) {
                runFailure = CompletableFuture.anyOf(runFailure, joinPreparation.failure());
            }
            awaitWorkloads(workloads, workload.path("timeoutSeconds").intValue() + 60, runFailure);
            if (seekSettings != null) {
                // While the network impairments still apply, such as the latency to the offload storage
                JsonNode gatewaysSummary = loader.mapper().readTree(
                        runOutput.resolve(GATEWAYS_DIRECTORY).resolve("gateways-summary.json").toFile());
                long seeksStarted = System.nanoTime();
                RandomSeeks.run(cluster.getHttpServiceUrl(), iotScenario.topicNames(),
                        requiredLong(gatewaysSummary, "measurementStartEpochMs"),
                        requiredLong(gatewaysSummary, "measurementEndEpochMs"), seekSettings, runOutput,
                        runFailure, PerformanceLauncher::status);
                seeksEndEpochMs = System.currentTimeMillis();
                status(String.format(Locale.ROOT, "Made %,d seek(s) in %.1f s", seekSettings.count(),
                        (System.nanoTime() - seeksStarted) / 1e9));
            }
            if (network != null) {
                // No traffic sees an impairment from now on; one that hasn't started is missed
                network.finishRun();
            }
            if (joinPreparation != null && offloadSettings != null) {
                // Diagnostic: the run, whose deliveries are checked below, doesn't fail with it
                joinPreparation.recordOffloadStatusOrWarn("end");
            }
            progress.report();
            progress.close();
            if (heapDumper != null) {
                heapDumper.end();
            }
            // The run's end in the charts: every consumer has finished, before the profiles are processed
            workloadFinished = ZonedDateTime.now().truncatedTo(ChronoUnit.SECONDS);
            status("Every application has received every message; verifying the device sequences");
            verifyStates(runOutput, runOutput.resolve(APPLICATIONS_DIRECTORY), workload, applications);
            if (remoteWorkload != null) {
                verifyStates(runOutput, remoteApplicationsOutput, remoteWorkload, remoteApplications);
                status("Every remote application has received every replicated message, in order");
            }
            if (metricsCollection != null) {
                // The metrics are an addition to the run, which doesn't fail with them
                try {
                    List<MetricsCollection.Event> events =
                            runEvents(loader.mapper(), runOutput, workload, applications, gatewaysStarted);
                    if (network != null) {
                        events.addAll(network.annotations());
                    }
                    String dashboard = metricsCollection.finish(runOutput, events,
                            System.getProperty("performance.metrics.grafanaUrl"), metricsBindAddress,
                            PerformanceLauncher::status);
                    status("Metrics in Grafana: " + dashboard + (metricsCollection.startedStack()
                            ? " (start the metrics stack with ./gradlew :tests:performance:metrics:up to view it)"
                            : ""));
                } catch (IOException | RuntimeException e) {
                    status("Metrics: couldn't finish collecting them: " + e.getMessage());
                    log().warn().exception(e).log("Finishing the metrics collection failed");
                }
            }
        } catch (Exception e) {
            // Reported before the shutdown, which takes a while, so that the failure is the last thing on the console
            reportFailure(e);
            throw new ReportedFailure(e);
        } finally {
            // A failure while shutting down is only a warning: it must not hide the run's result or its failure
            ProgressMonitor progressToClose = progress;
            TopicStatsSampler topicStatsToClose = topicStatsSampler;
            TopicStatsSampler remoteTopicStatsToClose = remoteTopicStatsSampler;
            Thread gateToStop = measurementGate;
            JoinPreparation joinPreparationToClose = joinPreparation;
            shutDown("stopping the join preparation", () -> {
                if (joinPreparationToClose != null) {
                    joinPreparationToClose.close();
                }
            });
            GenericContainer<?> producerToStop = producer;
            GenericContainer<?> consumerToStop = consumer;
            GenericContainer<?> remoteConsumerToStop = remoteConsumer;
            // First, so that no dump is being written into a container that stops
            shutDown("finishing the heap dumps", () -> {
                if (heapDumper != null) {
                    heapDumper.close();
                }
            });
            shutDown("stopping the progress report", () -> {
                if (progressToClose != null) {
                    progressToClose.close();
                }
            });
            shutDown("closing the topic stats sampler", () -> {
                if (topicStatsToClose != null) {
                    topicStatsToClose.close();
                }
            });
            shutDown("closing the remote topic stats sampler", () -> {
                if (remoteTopicStatsToClose != null) {
                    remoteTopicStatsToClose.close();
                }
            });
            shutDown("closing the host stats sampler", () -> {
                if (hostStatsSampler != null) {
                    hostStatsSampler.close();
                }
            });
            shutDown("closing the host I/O sampler", () -> {
                if (hostIoSampler != null) {
                    hostIoSampler.close();
                }
            });
            ContainerStatsSampler containerStatsToClose = containerStatsSampler;
            shutDown("closing the container stats sampler", () -> {
                if (containerStatsToClose != null) {
                    containerStatsToClose.close();
                }
            });
            PerfStatSidecar perfStatToClose = perfStatSidecar;
            shutDown("collecting the perf counts", () -> {
                if (perfStatToClose != null) {
                    perfStatToClose.close();
                }
            });
            if (gateToStop != null) {
                gateToStop.interrupt();
                try {
                    gateToStop.join(TimeUnit.SECONDS.toMillis(10));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            // While the containers run, so that their interfaces are cleaned up when they are left running
            if (network != null) {
                try {
                    network.close();
                } catch (IOException | RuntimeException e) {
                    // The run fails with it below: containers left running may keep the impairments
                    networkCleanupFailure = e;
                    status("Warning: removing the network impairments failed: " + e.getMessage());
                    log().warn().exception(e).log("Removing the network impairments failed");
                }
            }
            if (producerToStop != null) {
                saveContainerLog(producerToStop, runOutput.resolve(GATEWAYS_DIRECTORY).resolve(CONTAINER_LOG));
                shutDown("stopping the producer", producerToStop::stop);
            }
            if (consumerToStop != null) {
                saveContainerLog(consumerToStop, applicationsOutput.resolve(CONTAINER_LOG));
                shutDown("stopping the applications", consumerToStop::stop);
            }
            if (remoteConsumerToStop != null) {
                saveContainerLog(remoteConsumerToStop, remoteApplicationsOutput.resolve(CONTAINER_LOG));
                shutDown("stopping the remote applications", remoteConsumerToStop::stop);
            }
            MetricsCollection metricsToClose = metricsCollection;
            shutDown("stopping the metrics collection", () -> {
                if (metricsToClose != null) {
                    metricsToClose.close();
                }
            });
            if (remoteCluster != null) {
                status("Stopping the remote cluster");
                shutDown("stopping the remote cluster", remoteCluster::stop);
            }
            status("Stopping the Pulsar cluster");
            shutDown("stopping the Pulsar cluster", cluster::stop);
            if (offloadStorage != null) {
                saveContainerLog(offloadStorage, runOutput.resolve(RunReport.OFFLOAD_STORAGE_LOG_FILE));
            }
            if (offloadStorage != null && !PulsarContainer.PULSAR_CONTAINERS_LEAVE_RUNNING) {
                shutDown("stopping the offload storage", offloadStorage::stop);
            }
            if (closeClusterNetwork) {
                shutDown("removing the cluster's network", () -> cluster.getNetwork().close());
            }
            if (configurationStore != null) {
                shutDown("stopping the configuration store", configurationStore::stop);
                shutDown("removing the clusters' network", () -> configurationStore.getNetwork().close());
            }
        }
        if (profilingEnabled) {
            status("Processing the profiles");
            JsonNode summary = loader.mapper().readTree(runOutput.resolve("gateways/gateways-summary.json").toFile());
            Instant measurementStart = Instant.ofEpochMilli(requiredLong(summary, "measurementStartEpochMs"));
            long lastConsumerReceiptEpochMs = Long.MIN_VALUE;
            for (int application = 0; application < applications; application++) {
                JsonNode consumerSummary = loader.mapper().readTree(
                        applicationOutput(runOutput, workload, application).resolve("application-summary.json")
                                .toFile());
                lastConsumerReceiptEpochMs = Math.max(lastConsumerReceiptEpochMs,
                        requiredLong(consumerSummary, "lastMeasurementMessageReceivedEpochMs"));
            }
            // Without measured receipts, such as when the applications skipped every measured message, the gateways'
            // measurement end
            if (lastConsumerReceiptEpochMs < measurementStart.toEpochMilli()) {
                lastConsumerReceiptEpochMs = requiredLong(summary, "measurementEndEpochMs");
            }
            // The seeks after the workloads, which the profiles cover too
            lastConsumerReceiptEpochMs = Math.max(lastConsumerReceiptEpochMs, seeksEndEpochMs);
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
            for (Path recording : recordings) {
                if (offCpuCaptured(loader.mapper(), recording)) {
                    Path outputDirectory = OffCpuFlamegraphs.process(recording,
                            JonoffcpuAgent.capture(recording), measurementStart, measurementEnd);
                    System.out.println("Off-CPU profile: " + outputDirectory);
                }
            }
            JfrRecordingProcessor.process(recordings, measurementStart, measurementEnd);
            for (Path recording : recordings) {
                Path source = JfrRecordingProcessor.measurementPath(recording);
                String component = recordingComponent(runOutput, recording);
                if (Files.isRegularFile(source) && profilingSettings.component(component).nettyAllocationsReport()) {
                    System.out.println("Netty allocator events: " + NettyAllocatorEvents.write(source,
                            Duration.between(measurementStart, measurementEnd),
                            summary.path("measurementMessages").asLong(), loader.mapper()));
                }
                Set<JfrFlamegraphViews.View> views = JfrFlamegraphViews.configuredViews(
                        asyncProfilerOptions(loader.mapper(), recording));
                if (!views.isEmpty() && Files.isRegularFile(source)) {
                    System.out.println("Flame graphs: " + JfrFlamegraphViews.render(recording, source, views));
                }
            }
            ProfileReport.Run run = new ProfileReport.Run(scenario.getFileName().toString(), runId,
                    measurementStart, measurementEnd, summary.path("messagesPerSecond").asDouble());
            Map<Path, List<Path>> recordingsByDirectory = recordings.stream().sorted()
                    .collect(Collectors.groupingBy(Path::getParent, TreeMap::new, Collectors.toList()));
            for (Map.Entry<Path, List<Path>> entry : recordingsByDirectory.entrySet()) {
                printReport("Profile report", MarkdownPages.htmlPage(
                        ProfileReport.write(entry.getKey(), entry.getValue(), run, loader.mapper(), runOutput)),
                        reportsRoot);
            }
        }
        Path runReport = RunReport.write(runOutput, new RunReport.Run(scenario.getFileName().toString(), runId,
                PulsarContainer.DEFAULT_IMAGE_NAME, clusterConfig, workload, runInfo, workloadFinished,
                List.copyOf(cooldowns)), loader.mapper());
        printReport("Run report", MarkdownPages.htmlPage(runReport), reportsRoot);
        List<String> unexercised = network != null ? network.unexercised() : List.of();
        if (!unexercised.isEmpty()) {
            status("The run didn't apply its network impairments as the scenario asked: "
                    + String.join("; ", unexercised));
            return 1;
        }
        if (networkCleanupFailure != null) {
            status("The run failed to remove its network impairments: " + networkCleanupFailure.getMessage());
            return 1;
        }
        if (!keepLauncherLog) {
            if (deliveredIncorrectly(loader.mapper(), runOutput.resolve(APPLICATIONS_DIRECTORY), workload,
                    applications) || remoteWorkload != null && deliveredIncorrectly(loader.mapper(),
                    remoteApplicationsOutput, remoteWorkload, remoteApplications)) {
                System.out.println("Kept " + launcherLog + ", with the containers' logs, since the applications"
                        + " received duplicates, ordering violations or invalid messages");
            } else {
                deleteLauncherLog(launcherLog);
            }
        }
        return 0;
    }

    /**
     * Whether an application received duplicates, ordering violations or invalid messages in the run: one of the
     * {@code applications} with their outputs in {@code applicationsDirectory}.
     */
    static boolean deliveredIncorrectly(ObjectMapper mapper, Path applicationsDirectory, JsonNode workload,
                                        int applications) throws IOException {
        for (int application = 0; application < applications; application++) {
            JsonNode summary = mapper.readTree(applicationsDirectory.resolve(
                    RunReport.applicationName(workload, application)).resolve("application-summary.json").toFile());
            if (summary.path("duplicates").asLong() > 0 || summary.path("orderingViolations").asLong() > 0
                    || summary.path("invalidMessages").asLong() > 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * The run's events for Grafana's annotations: the gateways' start, which starts the warmup, the measurement's
     * start, the gateways' finish, and the applications' finish, when they had received every message.
     */
    static List<MetricsCollection.Event> runEvents(ObjectMapper mapper, Path runOutput, JsonNode workload,
                                                   int applications, Instant gatewaysStarted) throws IOException {
        List<MetricsCollection.Event> events = new ArrayList<>();
        if (gatewaysStarted != null) {
            events.add(new MetricsCollection.Event("gateways-started",
                    "Gateways started: the producers publish, the warmup starts", gatewaysStarted));
        }
        JsonNode gateways = mapper.readTree(runOutput.resolve("gateways/gateways-summary.json").toFile());
        events.add(new MetricsCollection.Event("warmup-finished", "Warmup finished: the measurement starts",
                Instant.ofEpochMilli(requiredLong(gateways, "measurementStartEpochMs"))));
        events.add(new MetricsCollection.Event("gateways-finished",
                "Gateways finished: the producers have published every message",
                Instant.ofEpochMilli(requiredLong(gateways, "measurementEndEpochMs"))));
        long lastReceived = Long.MIN_VALUE;
        for (int application = 0; application < applications; application++) {
            JsonNode summary = mapper.readTree(applicationOutput(runOutput, workload, application)
                    .resolve("application-summary.json").toFile());
            lastReceived = Math.max(lastReceived, requiredLong(summary, "lastMeasurementMessageReceivedEpochMs"));
        }
        if (lastReceived != Long.MIN_VALUE) {
            events.add(new MetricsCollection.Event("applications-finished",
                    "Applications finished: the consumers have received every message",
                    Instant.ofEpochMilli(lastReceived)));
        }
        return events;
    }

    /**
     * Prints a report's page, and its URL on the reports server when the report is in the reports root: the server's
     * base URL, as {@code performance.reportsServer.baseUrl} or its bind address and port give it, with the report's
     * path in the root appended.
     */
    private static void printReport(String label, Path page, Path reportsRoot) {
        System.out.println(label + ": " + page);
        String baseUrl = ReportsUrl.baseUrl(System.getProperty("performance.reportsServer.baseUrl"),
                System.getProperty("performance.reportsServer.bindAddress"),
                Integer.getInteger("performance.reportsServer.port", ReportsUrl.DEFAULT_PORT));
        ReportsUrl.url(baseUrl, reportsRoot, page).ifPresent(url -> System.out.println(label + " URL: " + url));
    }

    /**
     * Copies what the launcher prints on the console from now on into {@code file}, which the run keeps also when it
     * deletes {@code launcher.log}. Each write goes to the file as it happens, so that it can be followed during the
     * run and has everything up to a failure.
     */
    private static void copyConsoleTo(Path file) throws IOException {
        PrintStream console = System.out;
        OutputStream copy = Files.newOutputStream(file);
        System.setOut(new PrintStream(new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                console.write(b);
                copy.write(b);
            }

            @Override
            public void write(byte[] bytes, int offset, int length) throws IOException {
                console.write(bytes, offset, length);
                copy.write(bytes, offset, length);
            }

            @Override
            public void flush() throws IOException {
                console.flush();
                copy.flush();
            }
        }, true, console.charset()));
    }

    // Stops the logging first, which closes the log file and frees its space, and keeps anything from writing it again
    private static void deleteLauncherLog(Path launcherLog) {
        LogManager.shutdown();
        try {
            if (Files.deleteIfExists(launcherLog)) {
                System.out.println("Deleted " + launcherLog + " of the successful run; --keep-launcher-log keeps it");
            }
        } catch (IOException e) {
            System.out.println("Couldn't delete " + launcherLog + ": " + e);
        }
    }

    /**
     * Sets the offload policies of the offloads' namespace, which make the brokers offload a topic's ledgers as they
     * roll over, and delete them from BookKeeper after the deletion lag.
     */
    private static void setOffloadPolicies(PulsarCluster cluster, OffloadSettings offload)
            throws PulsarClientException, PulsarAdminException {
        try (PulsarAdmin admin = PulsarAdmin.builder()
                .serviceHttpUrl(cluster.getHttpServiceUrl())
                .build()) {
            admin.namespaces().setOffloadThreshold(offload.namespace(), offload.thresholdBytes());
            if (offload.thresholdSeconds() != null) {
                admin.namespaces().setOffloadThresholdInSeconds(offload.namespace(), offload.thresholdSeconds());
            }
            admin.namespaces().setOffloadDeleteLag(offload.namespace(), offload.deletionLagMillis(),
                    TimeUnit.MILLISECONDS);
        }
        status(String.format(Locale.ROOT, "Offloading the ledgers of %s: threshold %,d bytes%s, deletion lag %,d ms",
                offload.namespace(), offload.thresholdBytes(), offload.thresholdSeconds() != null
                        ? String.format(Locale.ROOT, " and %,d s", offload.thresholdSeconds()) : "",
                offload.deletionLagMillis()));
    }

    // The version that a broker reports, or empty when it doesn't answer; it names a release such as latest, which
    // the whole cluster runs
    private static String brokerVersion(PulsarCluster cluster) {
        try (PulsarAdmin admin = PulsarAdmin.builder()
                .serviceHttpUrl(cluster.getHttpServiceUrl())
                .connectionTimeout(5, TimeUnit.SECONDS)
                .readTimeout(5, TimeUnit.SECONDS)
                .build()) {
            return admin.brokers().getVersion();
        } catch (PulsarClientException | PulsarAdminException e) {
            log().warn().exception(e).log("Could not read the brokers' Pulsar version");
            return "";
        }
    }

    /**
     * The launcher's logger. It isn't a static field, because the logging reads its configuration when the first
     * logger is created, which has to be after main() has chosen the configuration and call() has named the log file.
     */
    private static Logger log() {
        return Logger.get(PerformanceLauncher.class);
    }

    /**
     * Checks the workload's settings as the gateways and the applications read them, so that an invalid scenario
     * fails before a cluster starts instead of in the workload containers.
     */
    static void checkWorkload(ObjectMapper mapper, JsonNode workload) {
        try {
            mapper.treeToValue(workload, IotScenario.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e.getCause() instanceof IllegalArgumentException invalid
                    ? invalid.getMessage() : "Invalid workloads.iotTelemetry: " + e.getOriginalMessage(), e);
        }
    }

    /** Prints a status line, with the time of day, as the run goes from one phase to the next. */
    private static void status(String message) {
        System.out.println(STATUS_TIME.format(LocalTime.now()) + " " + message);
    }

    /** The URL of a workload container's control port, through the port Testcontainers maps on the host. */
    private static String controlUrl(GenericContainer<?> container) {
        return "http://" + container.getHost() + ":" + container.getMappedPort(CONTROL_PORT);
    }

    private static long warmupMessageCount(JsonNode workload) {
        JsonNode warmup = workload.path("warmup");
        long perRound = warmup.path("messages").longValue() > 0 ? warmup.path("messages").longValue()
                : warmup.path("seconds").longValue() * workload.path("rate").longValue();
        return perRound * Math.max(1, warmup.path("rounds").intValue());
    }

    private static long measurementMessageCount(JsonNode workload) {
        JsonNode measurement = workload.path("measurement");
        return measurement.path("messages").longValue() > 0 ? measurement.path("messages").longValue()
                : measurement.path("seconds").longValue() * workload.path("rate").longValue();
    }

    /** Indents every line of {@code text} by two spaces, so that a block stands out from the status lines. */
    static String indent(String text) {
        return text.lines().map(line -> "  " + line + System.lineSeparator()).collect(Collectors.joining());
    }

    /**
     * The file of an --extends scenario. An absolute path is used as given. A relative one is looked for first in the
     * directory of the --scenario file, then in the working directory, each time as given and then with .yaml added
     * when the name has no extension.
     */
    private Path appendedScenario(Path file) {
        if (file.isAbsolute()) {
            if (!Files.isRegularFile(file)) {
                throw new IllegalArgumentException("No scenario file for --extends " + file);
            }
            return file;
        }
        List<Path> candidates = new ArrayList<>();
        for (Path base : List.of(scenario.toAbsolutePath().getParent(), Path.of("").toAbsolutePath())) {
            Path candidate = base.resolve(file);
            candidates.add(candidate);
            if (!candidate.getFileName().toString().matches(".*\\.ya?ml")) {
                candidates.add(candidate.resolveSibling(candidate.getFileName() + ".yaml"));
            }
        }
        return candidates.stream().filter(Files::isRegularFile).findFirst().orElseThrow(() ->
                new IllegalArgumentException("No scenario file for --extends " + file + "; looked for "
                        + candidates));
    }

    /** The run's name in the reports hierarchy: --name, else the scenario's output.name, else its file name. */
    private String runName(JsonNode resolved) {
        return name != null && !name.isBlank() ? name : scenarioName(resolved);
    }

    /** The scenario's name: its output.name, else its file name without .yaml. */
    private String scenarioName(JsonNode resolved) {
        String scenarioName = resolved.path("output").path("name").textValue();
        if (scenarioName != null && !scenarioName.isBlank()) {
            return scenarioName;
        }
        String fileName = scenario.getFileName().toString();
        return fileName.replaceFirst("\\.ya?ml$", "");
    }

    /**
     * Starts sampling the workload topics' stats for the run report. Sampling is an observation, so a failure to
     * start it is reported and the run goes on without it.
     */
    private static TopicStatsSampler startTopicStatsSampler(PulsarCluster cluster, JsonNode workload,
                                                            Path runOutput, boolean replicated, String fileName) {
        String prefix = workload.path("topics").path("prefix").textValue();
        List<String> topics = IntStream.range(0, workload.path("topics").path("count").intValue())
                .mapToObj(topic -> prefix + topic).toList();
        try {
            // Each broker, by its name in the cluster's network, which Docker names the container with a leading slash
            Map<String, String> brokerHttpUrls = new LinkedHashMap<>();
            for (BrokerContainer broker : cluster.getBrokers()) {
                brokerHttpUrls.put(broker.getContainerName().replaceFirst("^/", ""), broker.getHttpServiceUrl());
            }
            return TopicStatsSampler.start(brokerHttpUrls, topics, runOutput, replicated, fileName);
        } catch (Exception e) {
            System.out.println("Topic stats sampling is off for this run: " + e);
            return null;
        }
    }

    /** A container of the run, by its name in the report. */
    record MeasuredContainer(String name, GenericContainer<?> container) {
    }

    /**
     * The cluster's and the workloads' containers, named without the cluster's prefix, such as broker-0 and proxy.
     */
    static List<MeasuredContainer> measuredContainers(PulsarCluster cluster, String clusterName,
                                                      GenericContainer<?> producer, GenericContainer<?> consumer) {
        List<MeasuredContainer> containers = new ArrayList<>();
        List<GenericContainer<?>> clusterContainers = new ArrayList<>();
        clusterContainers.addAll(cluster.getBrokers());
        clusterContainers.addAll(cluster.getBookies());
        if (cluster.getZooKeeper() != null) {
            clusterContainers.add(cluster.getZooKeeper());
        }
        // The proxy only forwards the launcher's admin requests, but counts in the totals of all the containers
        if (cluster.getProxy() != null) {
            clusterContainers.add(cluster.getProxy());
        }
        for (GenericContainer<?> container : clusterContainers) {
            containers.add(new MeasuredContainer(componentName(container.getContainerName(), clusterName),
                    container));
        }
        if (producer != null) {
            containers.add(new MeasuredContainer(GATEWAYS_DIRECTORY, producer));
        }
        if (consumer != null) {
            containers.add(new MeasuredContainer(APPLICATIONS_DIRECTORY, consumer));
        }
        return containers;
    }

    /**
     * A cluster container's name without the cluster's, such as broker-0 for {@code /iot-1-pulsar-broker-0}. A cluster
     * that shares its configuration store, as with a remote cluster, prefixes its containers' names in its network
     * with its name, and a broker's container name then repeats it.
     */
    static String componentName(String containerName, String clusterName) {
        return containerName.replaceFirst("^/", "").replaceFirst("^(" + Pattern.quote(clusterName) + "-)+", "")
                .replaceFirst("^pulsar-", "");
    }

    /**
     * The containers that the network impairments' endpoints name: gateways, applications, broker-N, bookie-N,
     * zookeeper and configuration-store.
     */
    static Map<String, GenericContainer<?>> networkEndpoints(PulsarCluster cluster, String clusterName,
                                                             GenericContainer<?> producer,
                                                             GenericContainer<?> consumer,
                                                             PulsarCluster remoteCluster, String remoteClusterName,
                                                             GenericContainer<?> remoteConsumer) {
        Map<String, GenericContainer<?>> endpoints = networkEndpoints(cluster, clusterName, producer, consumer);
        if (remoteCluster != null) {
            for (MeasuredContainer measured : remoteContainers(remoteCluster, remoteClusterName, remoteConsumer)) {
                endpoints.put(measured.name(), measured.container());
            }
        }
        return endpoints;
    }

    /**
     * The remote cluster's containers and the remote applications, named as the network impairments' endpoints name
     * them: remote-broker-N, remote-bookie-N, remote-zookeeper and remote-applications.
     */
    static List<MeasuredContainer> remoteContainers(PulsarCluster remoteCluster, String remoteClusterName,
                                                    GenericContainer<?> remoteConsumer) {
        List<MeasuredContainer> containers = new ArrayList<>();
        for (MeasuredContainer measured : measuredContainers(remoteCluster, remoteClusterName, null, null)) {
            if (measured.container() != null) {
                containers.add(new MeasuredContainer(NetworkSettings.REMOTE_PREFIX + measured.name(),
                        measured.container()));
            }
        }
        if (remoteConsumer != null) {
            containers.add(new MeasuredContainer(REMOTE_APPLICATIONS_DIRECTORY, remoteConsumer));
        }
        return containers;
    }

    /**
     * The workload of the remote applications: the workload with the remote cluster's service URL and the remote
     * applications' count and pods. They open their pods at the start, without the workload's {@code joinSeconds},
     * since they don't see the gateways' measurement start.
     */
    static ObjectNode remoteApplicationsWorkload(ObjectNode workload, ClusterSettings.Remote remote,
                                                 String serviceUrl) {
        // Initializing the clusters' metadata adds both clusters to the replication clusters of public/default only
        String prefix = workload.path("topics").path("prefix").asText();
        if (!REPLICATED_TOPICS.matcher(prefix).matches()) {
            throw new IllegalArgumentException("cluster.remote replicates the namespace public/default, so the"
                    + " workload's topics must be persistent topics in it, not " + prefix);
        }
        ObjectNode remoteWorkload = workload.deepCopy();
        remoteWorkload.put("serviceUrl", serviceUrl);
        ObjectNode applications = (ObjectNode) remoteWorkload.get("applications");
        if (remote.applications().count() != null) {
            applications.put("count", remote.applications().count());
        }
        if (remote.applications().podsPerApplication() != null) {
            applications.put("podsPerApplication", remote.applications().podsPerApplication());
        }
        applications.remove("joinSeconds");
        // which need the late joins
        applications.remove("skipMessages");
        applications.remove("unloadBeforeJoin");
        applications.remove("resetToMillis");
        if (remote.applications().env() != null) {
            applications.set("env", new ObjectMapper().valueToTree(remote.applications().env()));
        }
        return remoteWorkload;
    }

    /** The remote applications' variables: the workload's applications', unless the remote applications set theirs. */
    static Map<String, String> remoteApplicationsEnv(Map<String, String> applicationsEnv,
                                                     ClusterSettings.Remote remote) {
        return remote.applications().env() != null ? remote.applications().env() : applicationsEnv;
    }

    static Map<String, GenericContainer<?>> networkEndpoints(PulsarCluster cluster, String clusterName,
                                                             GenericContainer<?> producer,
                                                             GenericContainer<?> consumer) {
        Map<String, GenericContainer<?>> endpoints = new LinkedHashMap<>();
        for (MeasuredContainer measured : measuredContainers(cluster, clusterName, producer, consumer)) {
            if (measured.name().matches("(broker|bookie)-[0-9]+")) {
                endpoints.put(measured.name(), measured.container());
            }
        }
        endpoints.put(NetworkSettings.GATEWAYS, producer);
        endpoints.put(NetworkSettings.APPLICATIONS, consumer);
        cluster.forEachContainer(container -> {
            if (container instanceof ZKContainer) {
                endpoints.put(NetworkSettings.ZOOKEEPER, container);
            } else if (container instanceof CSContainer) {
                endpoints.put(NetworkSettings.CONFIGURATION_STORE, container);
            }
        });
        return endpoints;
    }

    /**
     * The measurement's length in seconds, for checking the network impairments' times: as long as its messages take
     * at its rate when it sets a number of messages, which the gateways then send whatever its seconds, or its seconds
     * otherwise; -1 when it isn't known, a number of messages without a rate.
     */
    static double measurementSeconds(JsonNode workload) {
        JsonNode measurement = workload.path("measurement");
        long messages = measurement.path("messages").asLong();
        if (messages > 0) {
            long rate = workload.path("rate").asLong();
            return rate > 0 ? (double) messages / rate : -1;
        }
        long seconds = measurement.path("seconds").asLong();
        return seconds > 0 ? seconds : -1;
    }

    /** The PID of a container's main process in the Docker engine's host, from Docker's container inspect. */
    private static Long pid(MeasuredContainer measured) {
        return measured.container().getContainerInfo().getState().getPidLong();
    }

    /**
     * The containers' cgroup directories on this host; empty when the Docker engine runs in a VM, such as Docker
     * Desktop or OrbStack on macOS, whose processes and cgroups this host doesn't see.
     */
    private Map<String, Path> localCgroups(List<MeasuredContainer> containers) {
        Map<String, Path> cgroups = new LinkedHashMap<>();
        for (MeasuredContainer measured : containers) {
            Long pid = pid(measured);
            Path cgroup = pid != null ? ContainerStatsSampler.cgroupOf(procfs, pid, cgroupfs) : null;
            if (cgroup != null) {
                cgroups.put(measured.name(), cgroup);
            }
        }
        return cgroups;
    }

    /**
     * Whether the Docker engine runs on this host's kernel, from the kernel's boot ID, which isn't namespaced; assumed
     * without the sidecar. When it doesn't, such as with Docker Desktop or OrbStack on macOS, this host's files don't
     * describe the containers' host.
     */
    private boolean engineOnThisHost(PerfStatSidecar sidecar) {
        if (sidecar == null) {
            return true;
        }
        try {
            String local = Files.readString(procfs.resolve("sys/kernel/random/boot_id")).trim();
            return local.equals(sidecar.bootId());
        } catch (IOException | RuntimeException e) {
            return false;
        }
    }

    /**
     * Starts sampling the containers' CPU use and context switches: from this host's files when the Docker engine
     * runs on it, else through the sidecar in the engine's host. Sampling is an observation, so a failure to start it
     * is reported and the run goes on without it.
     */
    private ContainerStatsSampler startContainerStatsSampler(List<MeasuredContainer> containers,
                                                             boolean engineOnThisHost, PerfStatSidecar sidecar,
                                                             Path runOutput) {
        try {
            Map<String, Path> cgroups = engineOnThisHost ? localCgroups(containers) : Map.of();
            ContainerStatsSampler.Source source = !cgroups.isEmpty()
                    ? new ContainerStatsSampler.LocalSource(procfs, cgroups)
                    : sidecar != null ? sidecar.containerSource() : null;
            if (source == null) {
                System.out.println("Container stats sampling is off for this run: the containers' cgroups aren't on "
                        + "this host, and --no-perf-stat turned off the sidecar that reads them in the Docker engine");
                return null;
            }
            return ContainerStatsSampler.start(source, runOutput);
        } catch (Exception e) {
            System.out.println("Container stats sampling is off for this run: " + e);
            return null;
        }
    }

    /**
     * Starts the idle sidecar container in the Docker engine's host. It is an observation, so a failure to start it
     * is reported and the run goes on without it.
     */
    private static PerfStatSidecar startPerfStatSidecar(Path runOutput) {
        try {
            return PerfStatSidecar.start(runOutput);
        } catch (Exception e) {
            System.out.println("perf stat and the Docker engine host's counters are off for this run: "
                    + e.getMessage());
            return null;
        }
    }

    /** Has the sidecar count the containers' CPU events; returns whether it found their cgroups. */
    private static boolean countContainers(PerfStatSidecar sidecar, List<MeasuredContainer> containers) {
        try {
            List<PerfStatSidecar.Target> targets = new ArrayList<>();
            for (MeasuredContainer measured : containers) {
                Long pid = pid(measured);
                if (pid != null) {
                    targets.add(new PerfStatSidecar.Target(measured.name(), pid));
                }
            }
            boolean found = sidecar.count(targets);
            if (sidecar.counting()) {
                status("Counting the containers' CPU events with perf stat");
            }
            return found;
        } catch (Exception e) {
            System.out.println("perf stat is off for this run: " + e.getMessage());
            return false;
        }
    }

    /**
     * Starts sampling the host's thermal state for the run report. Sampling is an observation, so a failure to
     * start it is reported and the run goes on without it.
     */
    private static HostStatsSampler startHostStatsSampler(HostStatsSampler.Sensors sensors, Path runOutput) {
        try {
            HostStatsSampler sampler = HostStatsSampler.start(sensors, runOutput);
            if (sampler == null) {
                System.out.println("Host stats sampling is off for this run: no CPU sensors under the sysfs root");
            }
            return sampler;
        } catch (Exception e) {
            System.out.println("Host stats sampling is off for this run: " + e);
            return null;
        }
    }

    /**
     * Starts sampling the host's CPU utilization and disk throughput into {@code host-io.csv}. Sampling is an
     * observation, so a failure to start it is reported and the run goes on without it.
     */
    private static HostIoSampler startHostIoSampler(HostIoSampler.Source source, Path runOutput) {
        try {
            return HostIoSampler.start(source, runOutput);
        } catch (Exception e) {
            System.out.println("Host I/O sampling is off for this run: " + e);
            return null;
        }
    }

    /**
     * Waits until the CPU package has cooled down to --cooldown-temperature, or --cooldown-timeout has passed, so
     * that a run doesn't start on a CPU that the previous run or the image build left hot. Returns what happened,
     * for the run report, or {@code null} when no cool-down was asked for or the host has no temperature sensor.
     */
    private RunReport.Cooldown coolDown(HostStatsSampler.Sensors sensors, String phase)
            throws InterruptedException {
        if (cooldownCelsius == null) {
            return null;
        }
        OptionalDouble initial = sensors.packageCelsius();
        if (initial.isEmpty()) {
            System.out.println("No cool-down: the host has no CPU temperature sensor");
            return null;
        }
        long startEpochMillis = System.currentTimeMillis();
        long start = System.nanoTime();
        long deadline = start + TimeUnit.SECONDS.toNanos(cooldownTimeoutSeconds);
        long nextProgress = start;
        double current = initial.getAsDouble();
        while (current > cooldownCelsius && System.nanoTime() < deadline) {
            if (System.nanoTime() >= nextProgress) {
                System.out.printf(Locale.ROOT, "Cooling down: CPU package at %.0f °C, waiting for %.0f °C%n",
                        current, cooldownCelsius);
                nextProgress = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            }
            Thread.sleep(2000);
            current = sensors.packageCelsius().orElse(current);
        }
        double waitedSeconds = (System.nanoTime() - start) / 1e9;
        boolean reached = current <= cooldownCelsius;
        System.out.printf(Locale.ROOT, "%s: CPU package at %.0f °C after %.0f s%n",
                reached ? "Cooled down" : "Cool-down timed out", current, waitedSeconds);
        return new RunReport.Cooldown(phase, cooldownCelsius, initial.getAsDouble(), current, waitedSeconds,
                reached, startEpochMillis, System.currentTimeMillis());
    }

    /**
     * Lets the host cool down again between the warmup and the measurement. The producer serves its measurement
     * control endpoints over HTTP on {@link #CONTROL_PORT}, which this thread reaches through the port Testcontainers
     * maps on the host: it waits on the ready endpoint, which answers as soon as every warmup round has been
     * received, cools down, and starts the measurement. The start is always sent, also when the cool-down fails, so
     * that the producer never waits for a launcher that has given up.
     */
    private Thread startMeasurementGate(HostStatsSampler.Sensors sensors, GenericContainer<?> producer,
                                        List<RunReport.Cooldown> cooldowns, NetworkImpairments network) {
        String control = "http://" + producer.getHost() + ":" + producer.getMappedPort(CONTROL_PORT);
        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        Thread gate = new Thread(() -> {
            boolean release = true;
            try {
                if (awaitReady(client, control, producer)) {
                    if (cooldownCelsius != null) {
                        System.out.println("Warmup received; cooling down before the measurement");
                        RunReport.Cooldown cooldown = coolDown(sensors, RunReport.Cooldown.BEFORE_MEASUREMENT);
                        if (cooldown != null) {
                            cooldowns.add(cooldown);
                        }
                    }
                    if (network != null) {
                        try {
                            network.prepareMeasurement();
                        } catch (RuntimeException e) {
                            // The measurement doesn't start without its impairments; the run fails with them
                            release = false;
                            network.fail(e);
                        }
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                if (release) {
                    boolean started = startMeasurement(client, control);
                    if (network != null) {
                        if (started) {
                            // The impairments' times count from the producers' release
                            network.startMeasurement();
                        } else {
                            network.fail(new IllegalStateException("Couldn't start the measurement, so the network"
                                    + " impairments that count from it wouldn't happen"));
                        }
                    }
                }
            }
        }, "measurement-gate");
        gate.setDaemon(true);
        gate.start();
        return gate;
    }

    /**
     * Waits until the producer is ready for the measurement; false when it stopped before that. Each request waits
     * up to 10 s, within the JDK server's 30 s idle connection timeout, and is repeated until the producer is ready.
     */
    private static boolean awaitReady(HttpClient client, String control, GenericContainer<?> producer)
            throws InterruptedException {
        HttpRequest ready = HttpRequest.newBuilder(URI.create(control + "/measurement/ready?waitMillis=10000"))
                .timeout(Duration.ofSeconds(30)).GET().build();
        while (producer.isRunning()) {
            try {
                if (client.send(ready, HttpResponse.BodyHandlers.discarding()).statusCode() == 200) {
                    return true;
                }
            } catch (IOException e) {
                // The producer may be starting its server, or have stopped; the loop checks which
                Thread.sleep(1000);
            }
        }
        return false;
    }

    /**
     * Lets the gateways, which wait with {@code --await-run-start}, create their clients and start the run; fails when
     * they can't be reached.
     */
    private static void startRun(GenericContainer<?> producer) throws IOException, InterruptedException {
        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        HttpRequest start = HttpRequest.newBuilder(URI.create(controlUrl(producer) + "/run/start"))
                .timeout(Duration.ofSeconds(10)).POST(HttpRequest.BodyPublishers.noBody()).build();
        IOException failure = null;
        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                int statusCode = client.send(start, HttpResponse.BodyHandlers.discarding()).statusCode();
                if (statusCode == 200) {
                    return;
                }
                failure = new IOException("The gateways answered " + statusCode + " to the run's start");
            } catch (IOException e) {
                failure = e;
            }
        }
        throw new IOException("Couldn't start the gateways' run: " + failure.getMessage(), failure);
    }

    /** Starts the producers' measurement; returns whether they confirmed it. */
    private static boolean startMeasurement(HttpClient client, String control) {
        HttpRequest start = HttpRequest.newBuilder(URI.create(control + "/measurement/start"))
                .timeout(Duration.ofSeconds(10)).POST(HttpRequest.BodyPublishers.noBody()).build();
        for (int attempt = 1; attempt <= 3; attempt++) {
            try {
                if (client.send(start, HttpResponse.BodyHandlers.discarding()).statusCode() == 200) {
                    return true;
                }
            } catch (IOException e) {
                System.out.println("Couldn't start the measurement (attempt " + attempt + "): " + e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    /** The command of {@code jfr configure} that merges {@code input} and applies the event settings after it. */
    static String[] jfrConfigureCommand(String input, List<String> eventSettings, String output) {
        List<String> command = new ArrayList<>(List.of("configure", "--input", input));
        command.addAll(eventSettings);
        command.addAll(List.of("--output", output));
        return command.toArray(String[]::new);
    }

    /** The profiled component whose recording {@code recording} is, by the directory that it is in. */
    static String recordingComponent(Path runOutput, Path recording) {
        Path directory = runOutput.relativize(recording.toAbsolutePath().normalize()).getName(0);
        return switch (directory.toString()) {
            case GATEWAYS_DIRECTORY -> ProfilingSettings.GATEWAYS;
            case APPLICATIONS_DIRECTORY -> ProfilingSettings.APPLICATIONS;
            default -> ProfilingSettings.BROKER;
        };
    }

    /**
     * Where a component's merged JFR configuration goes: the image that the component runs, and the directory on the
     * host that its container binds at {@code containerDirectory}.
     */
    record JfrOutput(String image, Path directory, String containerDirectory) {
    }

    /**
     * Merges each profiled component's {@code jfrConfigurations} with {@code jfr configure} into
     * {@value #JFR_CONFIGURATION_FILE} in its output directory, and sets its async-profiler {@code jfrsync} to the
     * file as its container sees it, with its {@code jfrEventConfig} applied after them, or to the JDK's
     * {@code default} configuration without configurations, as {@code jfr configure} starts from without
     * {@code --input}. A single configuration of the JDK without {@code jfrEventConfig}, such as the default
     * {@code profile}, is passed to {@code jfrsync} as it is; without configurations and {@code jfrEventConfig},
     * {@code jfrsync} is left out, which records only async-profiler's events. The merge runs in a one-off container
     * of the component's image, so that a configuration of the JDK, such as {@code profile}, is the one of the JVM
     * that records with it; the {@code .jfc} files come from {@code jfcDirectory}. When the image's JDK can't merge
     * them, such as a released Pulsar's image whose JDK has no jfr tool, the component records with the JDK's
     * {@value ProfilingSettings#FALLBACK_JFR_CONFIGURATION} configuration.
     */
    static ProfilingSettings mergeJfrConfigurations(ProfilingSettings settings, Path jfcDirectory,
                                                    Map<String, JfrOutput> outputs) throws IOException {
        Map<String, String> configurations = new HashMap<>();
        for (String name : ProfilingSettings.components()) {
            ProfilingSettings.Component component = settings.component(name);
            if (!component.profiled() || !component.recordsJfrEvents()) {
                continue;
            }
            List<String> listed = component.jfrConfigurations();
            // A configuration of the JDK, but not an empty one, which async-profiler doesn't know by name
            if (component.jfrEventConfig().isEmpty() && listed.size() == 1
                    && !listed.get(0).endsWith(ProfilingSettings.JFC_SUFFIX)
                    && !ProfilingSettings.JFR_CONFIGURE_EMPTY_INPUT.equals(listed.get(0))) {
                configurations.put(name, listed.get(0));
                continue;
            }
            for (String configuration : listed) {
                if (configuration.endsWith(ProfilingSettings.JFC_SUFFIX)
                        && !Files.isRegularFile(jfcDirectory.resolve(configuration))) {
                    throw new IllegalArgumentException("The profiling of " + name + " lists the JFR configuration "
                            + configuration + ", which isn't a file of " + jfcDirectory);
                }
            }
            JfrOutput output = outputs.get(name);
            Files.createDirectories(output.directory());
            String input = component.jfrConfigureInput(JFC_MOUNT);
            try (GenericContainer<?> merge = new GenericContainer<>(output.image())
                    .withFileSystemBind(jfcDirectory.toString(), JFC_MOUNT, BindMode.READ_ONLY)
                    .withFileSystemBind(output.directory().toString(), MERGE_OUTPUT_MOUNT, BindMode.READ_WRITE)
                    .withCreateContainerCmdModifier(command -> command.withUser("0").withEntrypoint("jfr"))
                    .withCommand(jfrConfigureCommand(input, component.jfrConfigureEventSettings(),
                            MERGE_OUTPUT_MOUNT + "/" + JFR_CONFIGURATION_FILE))
                    .withStartupCheckStrategy(new OneShotStartupCheckStrategy()
                            .withTimeout(Duration.ofMinutes(1)))) {
                merge.start();
                configurations.put(name, output.containerDirectory() + "/" + JFR_CONFIGURATION_FILE);
                System.out.println("JFR configuration of " + name + ": " + String.join(", ",
                        component.jfrConfigurations()) + (component.jfrEventConfig().isEmpty() ? ""
                        : " with " + String.join(" ", component.jfrConfigureEventSettings())) + ", merged into "
                        + output.directory().resolve(JFR_CONFIGURATION_FILE));
            } catch (RuntimeException e) {
                System.out.println("Couldn't merge the JFR configurations " + input + " of " + name + " in the image "
                        + output.image() + " (" + e.getMessage() + "), so " + name + " records with the JDK's "
                        + ProfilingSettings.FALLBACK_JFR_CONFIGURATION + " JFR configuration");
                configurations.put(name, ProfilingSettings.FALLBACK_JFR_CONFIGURATION);
            }
        }
        return settings.withJfrsync(configurations);
    }

    /**
     * Fails when the cluster's test image, built on a released Pulsar image, has no jcloud offloader: the Pulsar images
     * of 4.x releases don't include it, and the test image installs it only with
     * {@code -Pperformance.installOffloaders=true}. Without it, the brokers wouldn't offload, and the run would quietly
     * measure BookKeeper's reads.
     */
    private static void requireOffloader(String clusterImage, String clusterPulsarImage) {
        try (GenericContainer<?> check = new GenericContainer<>(clusterImage)
                .withCreateContainerCmdModifier(command -> command.withEntrypoint("sh"))
                .withCommand("-c", "ls /pulsar/offloaders/tiered-storage-jcloud-*.nar")
                .withStartupCheckStrategy(new OneShotStartupCheckStrategy().withTimeout(Duration.ofMinutes(1)))) {
            check.start();
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("This scenario offloads, but the cluster's image " + clusterImage
                    + ", built on " + clusterPulsarImage + ", has no jcloud offloader in /pulsar/offloaders: the"
                    + " images of 4.x releases don't include it. Run with -Pperformance.installOffloaders=true, which"
                    + " installs the release's offloader in the test image", e);
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
     * Whether the agent recorded off-CPU samples beside {@code recording}, as the sampling block of the agent
     * configuration beside it says: the admission policy {@code none} runs plain async-profiler through the same
     * agent, leaving nothing to correlate.
     */
    private static boolean offCpuCaptured(ObjectMapper mapper, Path recording) throws IOException {
        Path config = JonoffcpuAgent.config(recording);
        if (!Files.isRegularFile(config)) {
            return false;
        }
        return !"none".equals(mapper.readTree(config.toFile()).path("sampling").path("admission").path("policy")
                .asText());
    }

    private GenericContainer<?> workloadContainer(PulsarCluster cluster, Path tools, Path configFile,
                                                   Path coordinationDirectory, String runId,
                                                   Path outputDirectory, Path agentJar,
                                                   ProfilingSettings.Component profiling, String component,
                                                   Map<String, String> envs, HeapDumpSettings.Component heapDumps,
                                                   int heapDumpGzipLevel, Path heapDumpDirectory, String command,
                                                   String... extraArguments) throws IOException {
        List<String> arguments = new ArrayList<>();
        // Starts the tools with the JVM options of Pulsar's client tools, see run-workload
        arguments.add(TOOLS_MOUNT + "/bin/run-workload");
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
        String javaOptions = "";
        if (profiling.profiled()) {
            // The launcher owns the recording name so that it lands inside the run directory
            javaOptions = "-XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints "
                    + JonoffcpuAgent.writeConfig(outputDirectory, OUTPUT_MOUNT,
                    "profile-" + component + "-" + System.currentTimeMillis(), profiling.asyncProfilerOptions(),
                    profiling.offCpuOptions());
        }
        if (heapDumps.onOutOfMemoryError()) {
            javaOptions = (javaOptions + " " + HeapDumper.outOfMemoryOptions(heapDumpGzipLevel)).trim();
        }
        GenericContainer<?> container = new GenericContainer<>(PulsarContainer.DEFAULT_IMAGE_NAME)
                .withNetwork(cluster.getNetwork())
                .withFileSystemBind(tools.toString(), TOOLS_MOUNT, BindMode.READ_ONLY)
                .withFileSystemBind(configFile.toString(), CONFIG_MOUNT, BindMode.READ_ONLY)
                .withFileSystemBind(coordinationDirectory.toString(), COORDINATION_MOUNT, BindMode.READ_WRITE)
                .withFileSystemBind(outputDirectory.toString(), OUTPUT_MOUNT, BindMode.READ_WRITE)
                .withEnv(workloadEnvironment(javaOptions, envs))
                .withCommand(arguments.toArray(String[]::new));
        if (profiling.profiled()) {
            JonoffcpuAgent.attach(container, agentJar);
        }
        if (heapDumps.any()) {
            container.withFileSystemBind(heapDumpDirectory.toString(), HeapDumper.MOUNT, BindMode.READ_WRITE);
        }
        return container;
    }

    /** The JVMs whose heap the launcher dumps during the run: each broker, the gateways and the applications. */
    static List<HeapDumper.Target> heapDumpTargets(PulsarCluster cluster, HeapDumpSettings settings,
                                                   GenericContainer<?> producer, GenericContainer<?> consumer) {
        List<HeapDumper.Target> targets = new ArrayList<>();
        int index = 0;
        for (GenericContainer<?> broker : cluster.getBrokers()) {
            targets.add(new HeapDumper.Target(HeapDumpSettings.BROKER + "-" + index++, HeapDumpSettings.BROKER,
                    settings.broker(), broker));
        }
        targets.add(new HeapDumper.Target(HeapDumpSettings.GATEWAYS, HeapDumpSettings.GATEWAYS, settings.gateways(),
                producer));
        targets.add(new HeapDumper.Target(HeapDumpSettings.APPLICATIONS, HeapDumpSettings.APPLICATIONS,
                settings.applications(), consumer));
        return targets;
    }

    /** An environment with options added to a variable of JVM options, such as {@code PULSAR_EXTRA_OPTS}. */
    static Map<String, String> withJvmOptions(Map<String, String> env, String variable, String options) {
        Map<String, String> environment = new LinkedHashMap<>(env != null ? env : Map.of());
        String configured = environment.get(variable);
        environment.put(variable, configured == null || configured.isBlank() ? options : configured + " " + options);
        return environment;
    }

    /**
     * The environment of a workload container: the configured variables (the workload's {@code gateways.env} or
     * {@code applications.env}), {@code PULSAR_MEM} with the workload's heap unless they set it, and
     * {@code JAVA_TOOL_OPTIONS} with the launcher's JVM options, such as the profiling agent. A configured
     * {@code JAVA_TOOL_OPTIONS} is appended to the launcher's options, so that it can add or override options without
     * dropping the profiling agent.
     */
    static Map<String, String> workloadEnvironment(String javaOptions, Map<String, String> envs) {
        Map<String, String> environment = new LinkedHashMap<>();
        if (envs != null) {
            envs.forEach((name, value) -> environment.put(name, value != null ? value : ""));
        }
        environment.putIfAbsent(PULSAR_MEM, WORKLOAD_MEMORY);
        String configured = environment.get(JAVA_TOOL_OPTIONS);
        String toolOptions = Stream.of(javaOptions, configured).filter(options -> options != null && !options.isBlank())
                .collect(Collectors.joining(" "));
        if (toolOptions.isEmpty()) {
            environment.remove(JAVA_TOOL_OPTIONS);
        } else {
            environment.put(JAVA_TOOL_OPTIONS, toolOptions);
        }
        return environment;
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

    /** A workload container, with its name on the console and the file its log is saved to. */
    private record Workload(String name, GenericContainer<?> container, Path log) {
    }

    /**
     * Starts a workload container and waits until it logs its ready line, showing the startup progress it logs
     * meanwhile. Fails as soon as the container exits or stops making progress, with the cause from its log, and saves
     * the log, which Testcontainers removes with a container whose startup failed.
     */
    private static void startWorkload(GenericContainer<?> container, String ready, String name, Path log) {
        WorkloadStartup startup = new WorkloadStartup(ready, PerformanceLauncher::status);
        container.waitingFor(startup);
        try {
            container.start();
        } catch (RuntimeException e) {
            try {
                Files.writeString(log, startup.output());
            } catch (IOException ignored) {
                // The failure matters more than its log
            }
            String reason = startup.failure() != null ? startup.failure() : "didn't start: " + e.getMessage();
            throw new IllegalStateException(name + " " + reason + " (log: " + log + ")", e);
        }
    }

    /**
     * Waits until every workload has exited, and fails as soon as one exits with an error, or when they haven't
     * finished within {@code timeoutSeconds}, with the cause from the failed workload's log.
     */
    private static void awaitWorkloads(List<Workload> workloads, int timeoutSeconds, CompletableFuture<?> failure)
            throws Exception {
        Map<Workload, CompletableFuture<Integer>> running = new LinkedHashMap<>();
        for (Workload workload : workloads) {
            running.put(workload, exitCode(workload.container()));
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        while (!running.isEmpty()) {
            if (failure.isCompletedExceptionally()) {
                // Such as a network impairment that couldn't be applied: the run measures something else then
                failure.get();
            }
            try {
                CompletableFuture<?>[] waits = Stream.concat(running.values().stream(), Stream.of(failure))
                        .toArray(CompletableFuture[]::new);
                CompletableFuture.anyOf(waits).get(1, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("The workload didn't finish within " + timeoutSeconds + " s; "
                            + running.keySet().stream().map(Workload::name).collect(Collectors.joining(", "))
                            + " still running");
                }
                continue;
            } catch (ExecutionException e) {
                // A failed wait is found below, with its workload
            }
            for (Iterator<Map.Entry<Workload, CompletableFuture<Integer>>> iterator = running.entrySet().iterator();
                 iterator.hasNext(); ) {
                Map.Entry<Workload, CompletableFuture<Integer>> entry = iterator.next();
                if (!entry.getValue().isDone()) {
                    continue;
                }
                iterator.remove();
                Workload workload = entry.getKey();
                int exitCode = entry.getValue().get();
                saveContainerLog(workload.container(), workload.log());
                if (exitCode != 0) {
                    String cause = Files.isRegularFile(workload.log())
                            ? FailureCause.of(Files.readString(workload.log())) : null;
                    throw new IllegalStateException(workload.name() + " exited with status " + exitCode
                            + (cause != null ? ": " + cause : "") + " (log: " + workload.log() + ")");
                }
                if (workload.container() == workloads.get(0).container()) {
                    status("The gateways have finished; waiting for the applications to receive every message");
                }
            }
        }
        if (failure.isCompletedExceptionally()) {
            failure.get();
        }
    }

    /** The container's exit code, once it has exited. */
    private static CompletableFuture<Integer> exitCode(GenericContainer<?> container) {
        CompletableFuture<Integer> exitCode = new CompletableFuture<>();
        Thread waiter = new Thread(() -> {
            try {
                exitCode.complete(container.getDockerClient().waitContainerCmd(container.getContainerId()).start()
                        .awaitStatusCode());
            } catch (RuntimeException e) {
                exitCode.completeExceptionally(e);
            }
        }, "wait-" + container.getContainerId());
        waiter.setDaemon(true);
        waiter.start();
        return exitCode;
    }

    /** A step of shutting a run down, which may fail. */
    private interface ShutdownStep {
        void run() throws Exception;
    }

    /** Runs a shutdown step, and reports its failure as a warning, with the stack trace in the launcher's log. */
    private static void shutDown(String description, ShutdownStep step) {
        try {
            step.run();
        } catch (Exception e) {
            status("Warning: " + description + " failed: " + e);
            log().warn().exception(e).attr("step", description).log("A shutdown step failed");
        }
    }

    // Streams the container's log into the file, which can be large, such as a storage that logs every request
    private static void saveContainerLog(GenericContainer<?> container, Path path) {
        if (container.getContainerId() == null) {
            return;
        }
        try (OutputStream output = new BufferedOutputStream(Files.newOutputStream(path))) {
            AtomicReference<IOException> writeFailure = new AtomicReference<>();
            ResultCallback.Adapter<Frame> callback = container.getDockerClient()
                    .logContainerCmd(container.getContainerId())
                    .withStdOut(true)
                    .withStdErr(true)
                    .exec(new ResultCallback.Adapter<Frame>() {
                        @Override
                        public void onNext(Frame frame) {
                            synchronized (output) {
                                try {
                                    output.write(frame.getPayload());
                                } catch (IOException e) {
                                    writeFailure.compareAndSet(null, e);
                                }
                            }
                        }
                    });
            boolean completed = callback.awaitCompletion(5, TimeUnit.MINUTES);
            if (!completed) {
                callback.close();
            }
            if (writeFailure.get() != null) {
                throw writeFailure.get();
            }
            if (!completed) {
                // Marks the file as cut short, such as the storage's request log, whose requests the report counts
                synchronized (output) {
                    output.write((RunReport.CUT_SHORT_LOG_LINE + "\n").getBytes(StandardCharsets.UTF_8));
                }
                status("Warning: saving the log of " + path.getFileName() + " took longer than 5 minutes; it is"
                        + " cut short");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            // Preserve the workload result when optional diagnostic log collection fails.
            log().warn().attr("path", path).exception(e).log("Saving a container's log failed");
        }
    }

    // An application's outputs are in a directory named after it, as the run report names the application
    private static Path applicationOutput(Path runOutput, JsonNode workload, int application) {
        return RunReport.applicationDirectory(runOutput, workload, application);
    }

    /**
     * Checks that each of the {@code applications}, with their outputs in {@code applicationsDirectory}, received
     * every device's sequence that the gateways published.
     */
    private static void verifyStates(Path output, Path applicationsDirectory, JsonNode workload, int applications)
            throws Exception {
        long[] produced = readState(output.resolve("gateways/gateways-state.bin"));
        JsonNode skipMessages = workload.path("applications").path("skipMessages");
        JsonNode resetToMillis = workload.path("applications").path("resetToMillis");
        for (int application = 0; application < applications; application++) {
            long[] consumed = readState(applicationsDirectory.resolve(RunReport.applicationName(workload, application))
                    .resolve("application-state.bin"));
            // An application that skipped messages, or reset its subscription, has no state for a device whose messages
            // it all skipped; it has checked that it received as many messages as followed its position
            boolean skipped = skipMessages.path(application).asLong() > 0
                    || resetToMillis.path(application).asLong(-1) >= 0;
            if (!statesMatch(produced, consumed, skipped)) {
                throw new IllegalStateException((applicationsDirectory.endsWith(REMOTE_APPLICATIONS_DIRECTORY)
                        ? "Remote application " : "Application ") + application
                        + " did not receive every device sequence");
            }
        }
    }

    static boolean statesMatch(long[] produced, long[] consumed, boolean skipped) {
        if (produced.length != consumed.length) {
            return false;
        }
        for (int device = 0; device < produced.length; device++) {
            if (consumed[device] != produced[device] && !(skipped && consumed[device] == 0)) {
                return false;
            }
        }
        return true;
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

    /**
     * The Docker engine that runs the containers, as {@code docker info} describes it, or null when Docker can't be
     * asked. On macOS its CPUs and memory are those of Docker Desktop's virtual machine rather than the host's.
     */
    static DockerEngine dockerEngine() {
        try {
            Info info = DockerClientFactory.instance().getInfo();
            return new DockerEngine(Objects.toString(info.getServerVersion(), ""),
                    Objects.requireNonNullElse(info.getNCPU(), 0), Objects.requireNonNullElse(info.getMemTotal(), 0L),
                    Objects.toString(info.getOperatingSystem(), ""), Objects.toString(info.getKernelVersion(), ""),
                    Objects.toString(info.getArchitecture(), ""));
        } catch (RuntimeException e) {
            return null;
        }
    }
}
