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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import com.github.dockerjava.api.model.Capability;
import com.github.dockerjava.api.model.Device;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.metadata.bookkeeper.PulsarMetadataClientDriver;
import org.apache.pulsar.tests.integration.containers.BKContainer;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.topologies.PulsarCluster;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.MountableFile;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * End-to-end storage regressions, deliberately separate from ordinary launcher unit tests. All faults are confined
 * to bounded, container-private tmpfs mounts; no host directory or block device is accepted. Run with storageCheck.
 */
public class StorageFailureCheck {
    private static final Duration RECOVERY = Duration.ofSeconds(45);
    private String image;

    enum Fault {
        DISK_FULL("healthy"), READ_ONLY("readonly"), IO_ERROR("eio"), JOURNAL_STALL("stall");

        private final String mode;

        Fault(String mode) {
            this.mode = mode;
        }
    }

    @BeforeClass
    public void buildFaultImage() throws Exception {
        PulsarMetadataClientDriver.init();
        assertThat(PulsarContainer.PULSAR_CONTAINERS_LEAVE_RUNNING)
                .as("storage tests must remove all fault containers; disable PULSAR_CONTAINERS_LEAVE_RUNNING")
                .isFalse();
        image = new ImageFromDockerfile()
                .withBuildArg("PULSAR_IMAGE", PulsarContainer.DEFAULT_IMAGE_NAME)
                .withFileFromClasspath("Dockerfile", "storage/Dockerfile")
                .withFileFromClasspath("journal-faultfs.c", "storage/journal-faultfs.c")
                .withFileFromClasspath("run-bookie.sh", "storage/run-bookie.sh")
                .get();
    }

    @Test(timeOut = 60_000)
    public void verifiesKernelErrorsAndRecoveryOnAnAlreadyOpenJournal() throws Exception {
        try (GenericContainer<?> probe = new GenericContainer<>(image)
                .withEnv("STORAGE_FAULTS", "true").withEnv("NO_AUTOSTART", "true")
                .withCommand("/pulsar/bin/run-bookie.sh")
                .withCopyFileToContainer(MountableFile.forClasspathResource("storage/probe.py"),
                        "/tmp/storage-probe.py")
                .withCopyFileToContainer(MountableFile.forClasspathResource("storage/fill.py"), "/tmp/storage-fill.py")
                .waitingFor(Wait.forLogMessage(".*supervisord started.*", 1)
                        .withStartupTimeout(Duration.ofSeconds(20)))) {
            configureFaultContainer(probe);
            probe.start();
            Container.ExecResult result = probe.execInContainer("timeout", "20", "python3", "/tmp/storage-probe.py");
            assertThat(result.getExitCode()).as("%s %s", result.getStdout(), result.getStderr()).isZero();
            assertThat(result.getStdout()).contains("PASS:");
            assertThat(exec(probe, "timeout 10 python3 /tmp/storage-fill.py; cat /storage/control/full-error"))
                    .contains("ENOSPC errno=");
        }
    }

    @DataProvider
    public Object[][] faults() {
        return new Object[][] {{Fault.DISK_FULL}, {Fault.READ_ONLY}, {Fault.IO_ERROR}, {Fault.JOURNAL_STALL}};
    }

    @Test(dataProvider = "faults", timeOut = 360_000)
    public void failsSafelyReplacesTheEnsembleAndRecoversConfirmedMessages(Fault fault) throws Exception {
        String name = "storage-" + UUID.randomUUID().toString().substring(0, 8);
        Path artifacts = Path.of(System.getProperty("storage.check.output", "build/storage-check"), name + "-" + fault);
        Files.createDirectories(artifacts);
        PulsarCluster cluster = PulsarCluster.forSpec(PulsarClusterSpec.builder()
                .clusterName(name).clusterImage(image).numBrokers(1).numBookies(3).numProxies(1)
                .brokerEnvs(brokerSettings()).bookkeeperEnvs(bookieSettings()).build());
        List<BKContainer> bookies = new ArrayList<>(cluster.getBookies());
        BKContainer target = bookies.get(0);
        BKContainer spare = bookies.get(2);
        target.withEnv("STORAGE_FAULTS", "true")
                .withEnv("journalDirectory", "/storage/journal")
                .withEnv("ledgerDirectories", "/storage/ledgers")
                .withCopyFileToContainer(MountableFile.forClasspathResource("storage/fill.py"), "/tmp/storage-fill.py");
        configureFaultContainer(target);
        BookieId targetId = BookieId.parse(target.getHostname() + ":3181");
        BookieId spareId = BookieId.parse(spare.getHostname() + ":3181");
        Throwable failure = null;
        try {
            cluster.start();
            try (BookKeeper metadata = new BookKeeper(new ClientConfiguration()
                         .setMetadataServiceUri("metadata-store:zk:" + cluster.getZKConnString()));
                 PulsarAdmin admin = PulsarAdmin.builder().serviceHttpUrl(cluster.getHttpServiceUrl()).build();
                 PulsarClient client = PulsarClient.builder().serviceUrl(cluster.getPlainTextServiceUrl())
                         .operationTimeout(15, TimeUnit.SECONDS).build()) {
                // All three processes have been validated by cluster.start; remove spare capacity before the ledger
                // is created. getAllBookies/admin.bookies() includes stopped bookies and is NOT a liveness oracle.
                service(spare, "stop", "bookie");
                await().atMost(RECOVERY).untilAsserted(() -> assertThat(writable(metadata))
                        .contains(targetId).doesNotContain(spareId).hasSize(2));
                String topic = "persistent://public/default/" + name;
                admin.topics().createNonPartitionedTopic(topic);
                admin.topics().createSubscription(topic, "durability", MessageId.earliest);
                Map<MessageId, String> confirmed = new LinkedHashMap<>();
                try (Producer<String> producer = client.newProducer(Schema.STRING).topic(topic)
                        .enableBatching(false).maxPendingMessages(8).blockIfQueueFull(false)
                        .sendTimeout(5, TimeUnit.SECONDS).create()) {
                    publish(producer, "before", 20, confirmed);
                    MessageIdImpl baseline = (MessageIdImpl) confirmed.keySet().iterator().next();
                    assertThat(ensemble(metadata, baseline)).contains(targetId).doesNotContain(spareId).hasSize(2);
                    Files.writeString(artifacts.resolve("initial-ensemble.txt"), baseline + " "
                            + ensemble(metadata, baseline) + "\n");
                    // Prove this is a real journal with confirmed entries before enabling filesystem faults.
                    exec(target, "test -n \"$(ls /storage/backing/current/*.txn)\"");
                    inject(target, fault);
                    if (fault == Fault.DISK_FULL) {
                        await().atMost(RECOVERY).untilAsserted(() -> assertThat(metadata.getMetadataClientDriver()
                                .getRegistrationClient().getReadOnlyBookies().get(10, TimeUnit.SECONDS).getValue())
                                .contains(targetId));
                    }
                    // A timed-out publish may have persisted. It is deliberately NOT put in the confirmed oracle,
                    // and its delivery is allowed. There is no unbounded background producer or growing queue.
                    List<CompletableFuture<MessageId>> writes = new ArrayList<>();
                    for (int i = 0; i < 8; i++) {
                        writes.add(producer.sendAsync("during-" + i));
                    }
                    for (CompletableFuture<MessageId> write : writes) {
                        try {
                            MessageId id = write.get(15, TimeUnit.SECONDS);
                            throw new AssertionError("Publish unexpectedly confirmed without a writable quorum: " + id);
                        } catch (ExecutionException e) {
                            assertThat(e.getCause()).isInstanceOf(PulsarClientException.class);
                        }
                    }
                    if (fault != Fault.DISK_FULL) {
                        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(events(target))
                                .containsPattern("op=(write|fsync) path=[^\\n]+\\.txn mode=" + fault.mode)
                                .doesNotContain("EXPIRED"));
                    }
                    Files.writeString(artifacts.resolve("fault-events.txt"), events(target));
                    // Keep the fault active: successful sends must now use the healthy bookie plus the spare.
                    service(spare, "start", "bookie");
                    await().atMost(RECOVERY).untilAsserted(() -> assertThat(writable(metadata)).contains(spareId));
                    await().atMost(RECOVERY).untilAsserted(() -> {
                        try {
                            MessageId id = producer.sendAsync("replacement").get(10, TimeUnit.SECONDS);
                            confirmed.put(id, "replacement");
                            assertThat(ensemble(metadata, (MessageIdImpl) id)).contains(spareId)
                                    .doesNotContain(targetId).hasSize(2);
                            Files.writeString(artifacts.resolve("replacement-ensemble.txt"), id + " "
                                    + ensemble(metadata, (MessageIdImpl) id) + "\n");
                        } catch (ExecutionException e) {
                            assertThat(e.getCause()).isInstanceOf(PulsarClientException.class);
                            throw new AssertionError("Waiting for a replacement ensemble", e);
                        }
                    });
                    publish(producer, "after-replacement", 20, confirmed);
                }
                Files.write(artifacts.resolve("confirmed-messages.txt"), confirmed.entrySet().stream()
                        .map(entry -> entry.getKey() + "\t" + entry.getValue()).toList());
                restore(target);
                // Restart JVMs, not containers: the exact same tmpfs files/cookies remain. Stop the broker first so
                // it cannot retain entries or repopulate a bookie's write cache while the bookies restart.
                service(cluster.getAnyBroker(), "stop", "broker");
                for (BKContainer bookie : bookies) {
                    service(bookie, "stop", "bookie");
                }
                for (BKContainer bookie : bookies) {
                    service(bookie, "start", "bookie");
                }
                await().atMost(RECOVERY).untilAsserted(() -> assertThat(writable(metadata)).hasSize(3));
                service(cluster.getAnyBroker(), "start", "broker");
                // No consumer existed before the restart, so prefetch cannot mask lost storage.
                try (Consumer<String> consumer = client.newConsumer(Schema.STRING).topic(topic)
                        .subscriptionName("durability").receiverQueueSize(10).subscribe()) {
                    Map<MessageId, String> remaining = new LinkedHashMap<>(confirmed);
                    long deadline = System.nanoTime() + RECOVERY.toNanos();
                    while (!remaining.isEmpty() && System.nanoTime() < deadline) {
                        Message<String> message = consumer.receive(1, TimeUnit.SECONDS);
                        if (message != null) {
                            String expected = remaining.remove(message.getMessageId());
                            if (expected != null) {
                                assertThat(message.getValue()).as("confirmed payload at %s", message.getMessageId())
                                        .isEqualTo(expected);
                            }
                            consumer.acknowledge(message);
                        }
                    }
                    assertThat(remaining).as("successfully confirmed publishes missing after storage recovery")
                            .isEmpty();
                }
                assertThat(events(target)).doesNotContain("EXPIRED");
                Files.writeString(artifacts.resolve("result.txt"), "Recovered " + confirmed.size()
                        + " confirmed message IDs and payloads after " + fault + "\n");
            }
        } catch (Exception | AssertionError e) {
            failure = e;
            throw e;
        } finally {
            // Preserve the original assertion/setup failure if cleanup also fails.
            try {
                try {
                    if (target.getContainerId() != null && target.isRunning()) {
                        Container.ExecResult prepared = target.execInContainer("test", "-f", "/storage/control/mode");
                        if (prepared.getExitCode() == 0) {
                            restore(target);
                            Files.writeString(artifacts.resolve("fault-events-final.txt"), events(target));
                            Files.writeString(artifacts.resolve("fuse.log"),
                                    exec(target, "cat /storage/control/fuse.log"));
                        }
                    }
                } finally {
                    cluster.stop();
                }
            } catch (Exception | AssertionError cleanup) {
                if (failure == null) {
                    throw cleanup;
                }
                failure.addSuppressed(cleanup);
            }
        }
    }

    private static void configureFaultContainer(GenericContainer<?> container) {
        container.withTmpFs(Map.of("/storage/control", "rw,size=1m,mode=1777"))
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig()
                        .withMemory(2L * 1024 * 1024 * 1024).withMemorySwap(2L * 1024 * 1024 * 1024)
                        .withCapAdd(Capability.SYS_ADMIN)
                        .withDevices(Device.parse("/dev/fuse:/dev/fuse:rwm"))
                        .withSecurityOpts(List.of("apparmor=unconfined")));
    }

    private static Map<String, String> brokerSettings() {
        return Map.of("managedLedgerDefaultEnsembleSize", "2", "managedLedgerDefaultWriteQuorum", "2",
                "managedLedgerDefaultAckQuorum", "2", "bookkeeperClientTimeoutInSeconds", "2",
                "managedLedgerCacheSizeMB", "0", "managedLedgerMinLedgerRolloverTimeMinutes", "0");
    }

    private static Map<String, String> bookieSettings() {
        return Map.ofEntries(Map.entry("journalSyncData", "true"), Map.entry("journalWriteData", "true"),
                Map.entry("PULSAR_PREFIX_readOnlyModeEnabled", "true"), Map.entry("journalPreAllocSizeMB", "1"),
                Map.entry("journalMaxSizeMB", "8"), Map.entry("journalMaxBackups", "0"),
                Map.entry("logSizeLimit", "8388608"), Map.entry("diskCheckInterval", "1000"),
                Map.entry("diskUsageThreshold", "0.90"), Map.entry("PULSAR_PREFIX_diskUsageWarnThreshold", "0.85"),
                Map.entry("PULSAR_PREFIX_diskUsageLwmThreshold", "0.80"),
                Map.entry("PULSAR_PREFIX_minUsableSizeForEntryLogCreation", "1048576"),
                Map.entry("PULSAR_PREFIX_minUsableSizeForIndexFileCreation", "1048576"),
                Map.entry("PULSAR_PREFIX_minUsableSizeForHighPriorityWrites", "1048576"));
    }

    private static void inject(BKContainer target, Fault fault) throws Exception {
        if (fault == Fault.DISK_FULL) {
            // Check the mount identity/capacity before allocation and assert numeric ENOSPC, not localized text.
            exec(target, "timeout 10 python3 /tmp/storage-fill.py");
        } else {
            exec(target, "printf '%s\\n' '" + fault.mode + "' > /storage/control/mode.next; "
                    + "mv /storage/control/mode.next /storage/control/mode");
        }
    }

    private static void restore(BKContainer target) throws Exception {
        exec(target, "printf 'healthy\\n' > /storage/control/mode.next; "
                + "mv /storage/control/mode.next /storage/control/mode; "
                + "test \"$(readlink -f /storage/ledgers/fill)\" = /storage/ledgers/fill; "
                + "rm -f /storage/ledgers/fill");
    }

    private static String events(BKContainer target) throws Exception {
        return exec(target, "cat /storage/control/events; "
                + "if test -f /storage/control/full-error; then cat /storage/control/full-error; fi");
    }

    private static List<BookieId> ensemble(BookKeeper metadata, MessageIdImpl id) throws Exception {
        return metadata.getLedgerMetadata(id.getLedgerId()).get(10, TimeUnit.SECONDS).getEnsembleAt(id.getEntryId());
    }

    private static Set<BookieId> writable(BookKeeper metadata) throws Exception {
        return metadata.getMetadataClientDriver().getRegistrationClient().getWritableBookies()
                .get(10, TimeUnit.SECONDS).getValue();
    }

    private static void publish(Producer<String> producer, String phase, int count, Map<MessageId, String> confirmed)
            throws Exception {
        for (int i = 0; i < count; i++) {
            String value = phase + "-" + i;
            confirmed.put(producer.sendAsync(value).get(15, TimeUnit.SECONDS), value);
        }
    }

    private static void service(GenericContainer<?> container, String action, String component) throws Exception {
        Container.ExecResult result = container.execInContainer("supervisorctl", action, component);
        if (action.equals("stop") && result.getStdout().contains(component + ": ERROR (not running)")) {
            // Journal EIO/EROFS can legitimately terminate the bookie before recovery starts.
            return;
        }
        assertThat(result.getExitCode()).as("supervisorctl %s %s: %s %s", action, component,
                result.getStdout(), result.getStderr()).isZero();
    }

    private static String exec(GenericContainer<?> container, String script) throws Exception {
        Container.ExecResult result = container.execInContainer("sh", "-ec", script);
        assertThat(result.getExitCode()).as("%s: %s %s", script, result.getStdout(), result.getStderr()).isZero();
        return result.getStdout();
    }
}
