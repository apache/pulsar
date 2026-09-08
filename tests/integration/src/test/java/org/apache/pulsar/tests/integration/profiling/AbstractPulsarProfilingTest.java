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
package org.apache.pulsar.tests.integration.profiling;

import static org.assertj.core.api.SoftAssertions.assertSoftly;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.tests.ManualTestUtil;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.apache.pulsar.tests.integration.utils.DockerUtils;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;

/**
 * Base class for the sample tests that profile the broker side with Async Profiler.
 *
 * The concrete subclasses only pick which client generation the load is driven with:
 * {@link PulsarProfilingTest} drives a v5 scalable topic with the v5 pulsar-perf commands and
 * {@link PulsarProfilingV4Test} drives a classic v4 topic with the {@code -v4} pulsar-perf commands.
 * Everything else - the cluster spec, the broker and bookie tuning, the pulsar-perf containers and
 * the profiling wiring - is shared, so the two runs differ only in the client and the topic domain.
 * They are not like-for-like beyond that: scalable topics auto-split their segments under load
 * (scalableTopicAutoScaleEnabled defaults to true), so the v5 run profiles a topology that reshapes
 * itself while the v4 run's stays fixed.
 *
 * Example usage (this has been tested on Mac with Orbstack (https://orbstack.dev/) docker):
 * ./gradlew :tests:integration:profilingIntegrationTest
 * That single task builds the test image with async-profiler in it, relaxes the kernel perf_event
 * limits that the cpu sampling engine needs, and runs {@link PulsarProfilingTest} against the
 * result. Add --tests "*PulsarProfilingV4Test" to profile the v4 variant instead. See
 * CONTRIBUTING.md for the properties that tune it.
 * On a Linux host the perf_event limits can also be set persistently with sysctl, in which case
 * -Pinttest.asyncprofiler.skipPerfEventTuning skips the container that sets them:
 * kernel.perf_event_paranoid=1
 * kernel.kptr_restrict=0
 * kernel.perf_event_max_stack=1024
 * kernel.perf_event_mlock_kb=2048
 * Add -Pdocker.wolfi to build the base image from Wolfi, which is what makes the GLIBC_TUNABLES
 * below take effect.
 * By default, the .jfr files and logs will go into tests/integration/build/pulsar-profiling
 * You can use jfrconv from async profiler to convert them into html flamegraphs or use other tools such
 * as Eclipse Mission Control (https://adoptium.net/jmc) or IntelliJ to open them.
 */
@CustomLog
public abstract class AbstractPulsarProfilingTest extends PulsarTestSuite {
    // this assumes that Transparent Huge Pages are available on the host machine
    // Please notice that "madvise" mode is recommended for performance reasons.
    // For example:
    // echo madvise | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
    // echo madvise | sudo tee /sys/kernel/mm/transparent_hugepage/defrag
    // More info about -XX:+UseTransparentHugePages at
    // https://shipilev.net/jvm/anatomy-quarks/2-transparent-huge-pages/
    private static final String DEFAULT_PULSAR_MEM = "-Xmx1g -XX:+UseTransparentHugePages -XX:+AlwaysPreTouch";
    private static final String PERF_PULSAR_MEM =
            "-XX:+UseTransparentHugePages -XX:+AlwaysPreTouch -XX:+HeapDumpOnOutOfMemoryError "
                    + "-XX:HeapDumpPath=/testoutput/%HEAP_DUMP_NAME%";
    private static final String PRODUCE_MEM_LIMIT = "200M";
    private static final String CONSUME_MEM_LIMIT = "200M";
    private static final String BROKER_PULSAR_MEM = "-Xms2g -Xmx2g -XX:+UseTransparentHugePages -XX:+AlwaysPreTouch";

    /**
     * The topic domain to drive the load against: {@link TopicDomain#topic} for a v5 scalable topic,
     * {@link TopicDomain#persistent} for a classic v4 topic.
     */
    protected abstract TopicDomain getTopicDomain();

    /**
     * The suffix that selects the pulsar-perf client generation: an empty string runs the v5
     * {@code produce}/{@code consume} commands, {@code "-v4"} runs {@code produce-v4}/{@code consume-v4}
     * on the v4 client.
     *
     * This is not independent of {@link #getTopicDomain()}: the v4 client refuses scalable topics
     * outright (PulsarClientImpl rejects the {@code topic://} and {@code segment://} domains with an
     * InvalidTopicNameException), so {@code "-v4"} only pairs with {@link TopicDomain#persistent}.
     */
    protected abstract String getPerfCommandSuffix();

    /**
     * One admin endpoint the print-stats container polls: the prefix its response is written under in
     * the test output directory, and the path to GET on the broker.
     */
    protected record TopicStatsEndpoint(String fileNamePrefix, String path) {
    }

    /**
     * The topic admin endpoints polled once per round, in order. Each topic domain is served by its
     * own admin resource and they do not expose the same operations, so the concrete test says what
     * exists for the domain it drives. Returning an empty list collects broker metrics only, which is
     * the right answer for a domain with no admin stats at all.
     */
    protected abstract List<TopicStatsEndpoint> getTopicStatsEndpoints(String topicName);

    /**
     * Admin path for {@code topicName} under the {@code /admin/v2/<resource>} REST resource. The
     * resource is not the topic domain: {@code persistent://} topics are served by {@code persistent}
     * but {@code topic://} (scalable) topics are served by {@code scalable}.
     */
    protected static String adminV2Path(String resource, String topicName) {
        TopicName topic = TopicName.get(topicName);
        return "/admin/v2/" + resource + "/" + topic.getNamespace() + "/" + topic.getLocalName();
    }

    // A container that runs pulsar-perf, arguments are currently hard-coded since this is an example
    static class PulsarPerfContainer extends GenericContainer<PulsarPerfContainer> {
        private final String brokerHostname;
        private final String commandSuffix;
        // Sized to finish well inside the wait in runPulsarPerfBenchmark. The containers sustain
        // roughly 290k msg/s, so this is a bit over a minute of load - long enough for a profile,
        // short enough that a run which does not finish is a real stall rather than the normal end.
        private final long numberOfMessages = 20_000_000;

        public PulsarPerfContainer(File testOutputDir,
                                   String clusterName,
                                   String brokerHostname,
                                   String hostname,
                                   String memArgs,
                                   String commandSuffix) {
            super(PulsarContainer.DEFAULT_IMAGE_NAME);
            this.brokerHostname = brokerHostname;
            this.commandSuffix = commandSuffix;
            withCreateContainerCmdModifier(createContainerCmd -> {
                createContainerCmd.withHostName(hostname);
                createContainerCmd.withName(clusterName + "-" + hostname);
            });
            String heapDumpName = hostname + "-oom-" + System.currentTimeMillis() + ".hprof";
            withEnv("PULSAR_MEM", PERF_PULSAR_MEM.replace("%HEAP_DUMP_NAME%", heapDumpName) + " " + memArgs);
            withEnv("PULSAR_GC", "-XX:+UseZGC");
            setCommand("sleep 1000000");
            withFileSystemBind(testOutputDir.getAbsolutePath(), "/testoutput", BindMode.READ_WRITE);
        }

        public CompletableFuture<Long> consume(String topicName) throws Exception {
            return DockerUtils.runCommandAsyncWithLogging(getDockerClient(), getContainerId(),
                    "bash", "-c", "set -o pipefail; echo $$ > /tmp/command.pid; "
                            + "/pulsar/bin/pulsar-perf consume" + commandSuffix + " " + topicName + " "
                            + "-u pulsar://" + brokerHostname + ":6650 "
                            + "-st Shared "
                            + "-q 50000 "
                            + "-m " + numberOfMessages + " -ml " + CONSUME_MEM_LIMIT + " "
                            + "--histogram-file=/testoutput/consume" + commandSuffix
                            + ".histogram.$(date +%s).hdr "
                            + "2>&1 | tee /testoutput/consume" + commandSuffix + ".$(date +%s).txt");
        }

        public CompletableFuture<Long> produce(String topicName) throws Exception {
            return DockerUtils.runCommandAsyncWithLogging(getDockerClient(), getContainerId(),
                    "bash", "-c", "set -o pipefail; echo $$ > /tmp/command.pid; "
                            + "/pulsar/bin/pulsar-perf produce" + commandSuffix + " " + topicName + " "
                            + "-u pulsar://" + brokerHostname + ":6650 "
                            + "-au http://" + brokerHostname + ":8080 "
                            + "-r " + Integer.MAX_VALUE + " "
                            + "-s 128 -db "
                            // maxOutstanding only applies to the v4 client; the v5 client accepts
                            // the flag for back-compat but ignores it
                            + "-o 20000 "
                            + "-m " + numberOfMessages + " -ml " + PRODUCE_MEM_LIMIT + " "
                            + "--histogram-file=/testoutput/produce" + commandSuffix
                            + ".histogram.$(date +%s).hdr "
                            + "2>&1 | tee /testoutput/produce" + commandSuffix + ".$(date +%s).txt");
        }

        /**
         * Polls the given topic admin endpoints and the broker metrics every 10 seconds. The metrics
         * are always collected; the topic endpoints may be empty when the domain has no admin stats.
         */
        public CompletableFuture<Long> stats(List<TopicStatsEndpoint> endpoints) throws Exception {
            String brokerUrl = "http://" + brokerHostname + ":8080";
            StringBuilder script = new StringBuilder("echo $$ > /tmp/command.pid; while [[ 1 ]]; do ");
            boolean firstEndpoint = true;
            for (TopicStatsEndpoint endpoint : endpoints) {
                if (!firstEndpoint) {
                    script.append("sleep 1; ");
                }
                firstEndpoint = false;
                script.append("curl -s ").append(brokerUrl).append(endpoint.path())
                        .append(" | jq | tee /testoutput/").append(endpoint.fileNamePrefix())
                        .append(commandSuffix).append(".$(date +%s).txt; ");
            }
            script.append("curl -s ").append(brokerUrl).append("/metrics/ > /testoutput/metrics")
                    .append(commandSuffix).append(".$(date +%s).txt; sleep 10; done");
            return DockerUtils.runCommandAsyncWithLogging(getDockerClient(), getContainerId(),
                    "bash", "-c", script.toString());
        }

        public void triggerShutdown() {
            if (isRunning()) {
                // attempt to stop containers gracefully
                DockerUtils.runCommandAsyncWithLogging(getDockerClient(), getContainerId(),
                                "bash", "-c", "pkill java; while pgrep -c java; do "
                                        + "echo Waiting for java processes to stop.; sleep 1; done; "
                                        + "kill $(cat /tmp/command.pid)")
                        .orTimeout(10, TimeUnit.SECONDS)
                        .exceptionally(t -> null)
                        .join();
            }
        }

        public void stop() {
            if (isRunning()) {
                // attempt to stop containers gracefully
                dockerClient.stopContainerCmd(getContainerId())
                        .withTimeout(15)
                        .exec();
            }
            super.stop();
        }
    }

    private PulsarPerfContainer perfConsume;
    private PulsarPerfContainer perfProduce;
    private PulsarPerfContainer printStats;
    private File testOutputDir;

    @Override
    public void setupCluster() throws Exception {
        ManualTestUtil.skipManualTestIfNotEnabled();
        createTestOutputDir();
        super.setupCluster();
    }

    private void createTestOutputDir() {
        testOutputDir = new File("build/pulsar-profiling");
        if (!testOutputDir.exists()) {
            if (!testOutputDir.mkdirs()) {
                throw new IllegalArgumentException("Test output directory + '" + testOutputDir.getAbsolutePath()
                        + "' doesn't exist and cannot be created.");
            }
        }
        if (!testOutputDir.isDirectory()) {
            throw new IllegalArgumentException(
                    "Test output directory '" + testOutputDir.getAbsolutePath() + "' isn't a directory.");
        }
        // change access to testOutputDir to allow all access so the the container user can write to it
        // This matters only on Linux
        try {
            Files.setPosixFilePermissions(testOutputDir.toPath(), PosixFilePermissions.fromString("rwxrwxrwx"));
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot change access to test output directory", e);
        }
    }

    @Override
    public void tearDownCluster() throws Exception {
        if (printStats != null) {
            printStats.triggerShutdown();
        }
        if (perfProduce != null) {
            perfProduce.triggerShutdown();
        }
        if (perfConsume != null) {
            perfConsume.triggerShutdown();
        }
        if (printStats != null) {
            printStats.stop();
            printStats = null;
        }
        if (perfProduce != null) {
            perfProduce.stop();
            perfProduce = null;
        }
        if (perfConsume != null) {
            perfConsume.stop();
            perfConsume = null;
        }
        super.tearDownCluster();
    }

    @Override
    protected void beforeStartCluster() throws Exception {
        super.beforeStartCluster();
        pulsarCluster.forEachContainer(
                // This is effective only when -Pdocker.wolfi has been passed when building java-test-image
                // setting mmap_threshold explicitly will avoid it's dynamic increase
                // https://sourceware.org/glibc/manual/latest/html_node/Memory-Allocation-Tunables.html
                c -> c.withEnv("GLIBC_TUNABLES",
                        "glibc.malloc.hugetlb=1:glibc.malloc.mmap_threshold=131072:glibc.malloc.arena_max=4"));
    }

    @Override
    protected PulsarClusterSpec.PulsarClusterSpecBuilder beforeSetupCluster(String clusterName,
        PulsarClusterSpec.PulsarClusterSpecBuilder specBuilder) {

        // Enable profiling on the broker
        specBuilder.profileBroker(true);
        specBuilder.profileDirectory(testOutputDir.getAbsolutePath());

        // Only run one broker so that all load goes to a single broker
        specBuilder.numBrokers(1);
        // Have 3 bookies to reduce bottleneck on bookie
        specBuilder.numBookies(3);
        // no need for proxy
        specBuilder.numProxies(0);

        // Increase memory for brokers and configure more aggressive rollover
        Map<String, String> brokerEnvs = new HashMap<>();
        brokerEnvs.put("PULSAR_MEM", BROKER_PULSAR_MEM);
        brokerEnvs.put("managedLedgerMinLedgerRolloverTimeMinutes", "1");
        brokerEnvs.put("managedLedgerMaxLedgerRolloverTimeMinutes", "5");
        brokerEnvs.put("managedLedgerMaxSizePerLedgerMbytes", "512");
        brokerEnvs.put("managedLedgerDefaultEnsembleSize", "1");
        brokerEnvs.put("managedLedgerDefaultWriteQuorum", "1");
        brokerEnvs.put("managedLedgerDefaultAckQuorum", "1");
        //brokerEnvs.put("maxPendingPublishRequestsPerConnection", "1000");
        brokerEnvs.put("dispatcherRetryBackoffInitialTimeInMs", "0");
        brokerEnvs.put("dispatcherRetryBackoffMaxTimeInMs", "0");
        brokerEnvs.put("preciseDispatcherFlowControl", "true");
        //brokerEnvs.put("PULSAR_PREFIX_subscriptionKeySharedUseClassicPersistentImplementation", "true");
        //brokerEnvs.put("PULSAR_PREFIX_subscriptionSharedUseClassicPersistentImplementation", "true");
        brokerEnvs.put("dispatcherMaxReadBatchSize", "1000");
        //brokerEnvs.put("dispatcherMaxReadSizeBytes", "10000000");
        //brokerEnvs.put("dispatcherDispatchMessagesInSubscriptionThread", "false");
        //brokerEnvs.put("dispatcherMaxRoundRobinBatchSize", "1000");
        specBuilder.brokerEnvs(brokerEnvs);

        // Increase memory for bookkeepers and make compaction run more often
        Map<String, String> bkEnv = new HashMap<>();
        bkEnv.put("PULSAR_MEM", DEFAULT_PULSAR_MEM);
        bkEnv.put("dbStorage_writeCacheMaxSizeMb", "64");
        bkEnv.put("dbStorage_readAheadCacheMaxSizeMb", "96");
        bkEnv.put("journalMaxSizeMB", "256");
        bkEnv.put("journalSyncData", "false");
        bkEnv.put("majorCompactionInterval", "300");
        bkEnv.put("minorCompactionInterval", "30");
        bkEnv.put("compactionRateByEntries", "20000");
        bkEnv.put("gcWaitTime", "30000");
        bkEnv.put("isForceGCAllowWhenNoSpace", "true");
        bkEnv.put("diskUsageLwmThreshold", "0.75");
        bkEnv.put("diskCheckInterval", "60");
        specBuilder.bookkeeperEnvs(bkEnv);

        // Create pulsar-perf containers
        String brokerHostname = clusterName + "-pulsar-broker-0";
        String commandSuffix = getPerfCommandSuffix();
        perfProduce = new PulsarPerfContainer(testOutputDir, clusterName, brokerHostname, "perf-produce", "-Xmx2g",
                commandSuffix);
        perfConsume = new PulsarPerfContainer(testOutputDir, clusterName, brokerHostname, "perf-consume", "-Xmx1g",
                commandSuffix);
        printStats = new PulsarPerfContainer(testOutputDir, clusterName, brokerHostname, "print-stats", "-Xmx1g",
                commandSuffix);
        specBuilder.externalServices(Map.of(
                "pulsar-produce", perfProduce,
                "pulsar-consume", perfConsume,
                "print-stats", printStats
        ));

        return specBuilder;
    }

    /**
     * Drives pulsar-perf against a freshly generated topic and waits for both sides to finish.
     *
     * The concrete subclasses wrap this in the actual {@code @Test} method: Gradle's TestNG detector
     * never scans method annotations on an abstract class, so an {@code @Test} that lived only here
     * would leave both subclasses looking like non-test classes and neither would be handed to
     * TestNG. (The detector does follow the superclass chain, so a subclass of a *concrete* base does
     * inherit its test methods - it is abstractness, not inheritance, that stops it.)
     */
    protected void runPulsarPerfBenchmark() throws Exception {
        String topicName = generateTopicName("profiletest", getTopicDomain());
        CompletableFuture<Long> consumeFuture = perfConsume.consume(topicName);
        Thread.sleep(1000);
        CompletableFuture<Long> produceFuture = perfProduce.produce(topicName);
        Thread.sleep(4000);
        printStats.stats(getTopicStatsEndpoints(topicName));
        // pulsar-perf is sized to finish inside this window, so running out of it is a failure.
        FutureUtil.waitForAll(List.of(consumeFuture, produceFuture))
                .orTimeout(3, TimeUnit.MINUTES)
                .exceptionally(t -> {
                    log.error().exception(t).log("Failed to run pulsar-perf");
                    throw FutureUtil.wrapToCompletionException(t);
                })
                .get();
        assertSoftly(softly -> {
            softly.assertThat(consumeFuture).as("consume should have completed successfully").isCompletedWithValue(0L);
            softly.assertThat(produceFuture).as("produce should have completed successfully").isCompletedWithValue(0L);
        });
    }
}
