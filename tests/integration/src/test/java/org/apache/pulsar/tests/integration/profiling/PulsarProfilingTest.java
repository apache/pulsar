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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.tests.ManualTestUtil;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.apache.pulsar.tests.integration.utils.DockerUtils;
import org.testcontainers.containers.GenericContainer;
import org.testng.annotations.Test;

/**
 * Sample test that profiles the broker side with Async Profiler.
 *
 * Example usage:
 * # This has been tested on Mac with Orbstack (https://orbstack.dev/) docker
 * # compile integration test dependencies
 * mvn -am -pl tests/integration -DskipTests install
 * # compile apachepulsar/java-test-image with async profiler (add "clean" to ensure a clean build with recent changes)
 * ./build/build_java_test_image.sh -Ddocker.install.asyncprofiler=true
 * # set environment variables
 * export PULSAR_TEST_IMAGE_NAME=apachepulsar/java-test-image:latest
 * export NETTY_LEAK_DETECTION=off
 * export ENABLE_MANUAL_TEST=true
 * # enable perf events for profiling and tune it
 * docker run --rm -it --privileged --cap-add SYS_ADMIN --security-opt seccomp=unconfined \
 *   alpine sh -c "echo 1 > /proc/sys/kernel/perf_event_paranoid \
 *   && echo 0 > /proc/sys/kernel/kptr_restrict \
 *   && echo 1024 > /proc/sys/kernel/perf_event_max_stack \
 *   && echo 2048 > /proc/sys/kernel/perf_event_mlock_kb"
 * # translated to sysctl settings (for persistent configuration on Linux hosts)
 * kernel.perf_event_paranoid=1
 * kernel.kptr_restrict=0
 * kernel.perf_event_max_stack=1024
 * kernel.perf_event_mlock_kb=2048
 * # run the test
 * mvn -DintegrationTests -pl tests/integration -Dtest=PulsarProfilingTest -DtestRetryCount=0 \
 *   -DredirectTestOutputToFile=false test
 * By default, the .jfr files will go into tests/integration/target
 * You can use jfrconv from async profiler to convert them into html flamegraphs or use other tools such
 * as Eclipse Mission Control (https://adoptium.net/jmc) or IntelliJ to open them.
 */
@Slf4j
public class PulsarProfilingTest extends PulsarTestSuite {
    // this assumes that Transparent Huge Pages are available on the host machine
    // Please notice that "madvise" mode is recommended for performance reasons.
    // For example:
    // echo madvise | sudo tee /sys/kernel/mm/transparent_hugepage/enabled
    // echo madvise | sudo tee /sys/kernel/mm/transparent_hugepage/defrag
    // More info about -XX:+UseTransparentHugePages at
    // https://shipilev.net/jvm/anatomy-quarks/2-transparent-huge-pages/
    private static final String DEFAULT_PULSAR_MEM = "-Xms512m -Xmx1g -XX:+UseTransparentHugePages -XX:+AlwaysPreTouch";
    private static final String BROKER_PULSAR_MEM = "-Xms2g -Xmx2g -XX:+UseTransparentHugePages -XX:+AlwaysPreTouch";

    // A container that runs pulsar-perf, arguments are currently hard-coded since this is an example
    static class PulsarPerfContainer extends GenericContainer<PulsarPerfContainer> {
        private final String brokerHostname;
        private final long numberOfMessages = 100_000_000;

        public PulsarPerfContainer(String clusterName,
                                   String brokerHostname,
                                   String hostname) {
            super(PulsarContainer.DEFAULT_IMAGE_NAME);
            this.brokerHostname = brokerHostname;
            withCreateContainerCmdModifier(createContainerCmd -> {
                createContainerCmd.withHostName(hostname);
                createContainerCmd.withName(clusterName + "-" + hostname);
            });
            withEnv("PULSAR_MEM", DEFAULT_PULSAR_MEM);
            setCommand("sleep 1000000");
        }

        public CompletableFuture<Long> consume(String topicName) throws Exception {
            return DockerUtils.runCommandAsyncWithLogging(getDockerClient(), getContainerId(),
                    "/pulsar/bin/pulsar-perf", "consume", topicName,
                    "-u", "pulsar://" + brokerHostname + ":6650",
                    "-st", "Shared",
                    "-m", String.valueOf(numberOfMessages), "-ml", "200M");
        }

        public CompletableFuture<Long> produce(String topicName) throws Exception {
            return DockerUtils.runCommandAsyncWithLogging(getDockerClient(), getContainerId(),
                    "/pulsar/bin/pulsar-perf", "produce", topicName,
                    "-u", "pulsar://" + brokerHostname + ":6650",
                    "-au", "http://" + brokerHostname + ":8080",
                    "-r", String.valueOf(Integer.MAX_VALUE), // max-rate
                    "-s", "8192", // 8kB message size
                    "-m", String.valueOf(numberOfMessages), "-ml", "200M");
        }
    }

    private PulsarPerfContainer perfConsume;
    private PulsarPerfContainer perfProduce;

    @Override
    public void setupCluster() throws Exception {
        ManualTestUtil.skipManualTestIfNotEnabled();
        super.setupCluster();
    }

    @Override
    public void tearDownCluster() throws Exception {
        if (perfConsume != null) {
            perfConsume.stop();
            perfConsume = null;
        }
        if (perfProduce != null) {
            perfProduce.stop();
            perfProduce = null;
        }
        super.tearDownCluster();
    }

    @Override
    protected PulsarClusterSpec.PulsarClusterSpecBuilder beforeSetupCluster(String clusterName,
        PulsarClusterSpec.PulsarClusterSpecBuilder specBuilder) {

        // Enable profiling on the broker
        specBuilder.profileBroker(true);

        // Only run one broker so that all load goes to a single broker
        specBuilder.numBrokers(1);
        // Have 3 bookies to reduce bottleneck on bookie
        specBuilder.numBookies(3);
        // no need for proxy
        specBuilder.numProxies(0);

        // Increase memory for brokers and configure more aggressive rollover
        specBuilder.brokerEnvs(Map.of("PULSAR_MEM", BROKER_PULSAR_MEM,
                "managedLedgerMinLedgerRolloverTimeMinutes", "1",
                "managedLedgerMaxLedgerRolloverTimeMinutes", "5",
                "managedLedgerMaxSizePerLedgerMbytes", "512",
                "managedLedgerDefaultEnsembleSize", "1",
                "managedLedgerDefaultWriteQuorum", "1",
                "managedLedgerDefaultAckQuorum", "1"
        ));

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
        perfProduce = new PulsarPerfContainer(clusterName, brokerHostname, "perf-produce");
        perfConsume = new PulsarPerfContainer(clusterName, brokerHostname, "perf-consume");
        specBuilder.externalServices(Map.of(
                "pulsar-produce", perfProduce,
                "pulsar-consume", perfConsume
        ));

        return specBuilder;
    }

    @Test(timeOut = 600_000)
    public void runPulsarPerf() throws Exception {
        String topicName = generateTopicName("profiletest", true);
        CompletableFuture<Long> consumeFuture = perfConsume.consume(topicName);
        Thread.sleep(1000);
        CompletableFuture<Long> produceFuture = perfProduce.produce(topicName);
        FutureUtil.waitForAll(List.of(consumeFuture, produceFuture))
                .orTimeout(3, TimeUnit.MINUTES)
                .exceptionally(t -> {
                    if (FutureUtil.unwrapCompletionException(t) instanceof TimeoutException) {
                        // ignore test timeout
                        log.info("Test timed out, ignoring this in profiling.");
                        return null;
                    } else {
                        log.error("Failed to run pulsar-perf", t);
                    }
                    throw FutureUtil.wrapToCompletionException(t);
                })
                .get();
    }
}
