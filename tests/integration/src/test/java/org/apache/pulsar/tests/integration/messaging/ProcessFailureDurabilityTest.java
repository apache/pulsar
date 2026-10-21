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
package org.apache.pulsar.tests.integration.messaging;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import com.google.common.util.concurrent.RateLimiter;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.apache.pulsar.tests.integration.suites.PulsarTestSuite;
import org.apache.pulsar.tests.integration.topologies.PulsarClusterSpec;
import org.apache.pulsar.tests.integration.utils.DockerUtils;
import org.testng.ITestResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Process failures with the original container filesystem intact, not power-loss simulations.
 */
public class ProcessFailureDurabilityTest extends PulsarTestSuite {
    private static final String SUBSCRIPTION = "durable-sub";
    private static final String FROZEN_SUBSCRIPTION = "frozen-audit-sub";
    private static final int END_MARKER = -1;
    private static final Duration RECOVERY_TIMEOUT = Duration.ofSeconds(120);
    // Longer than both the broker and bookie metadata sessions in this dedicated configuration.
    private static final Duration PAUSE_DURATION = Duration.ofSeconds(35);

    @Override
    protected PulsarClusterSpec.PulsarClusterSpecBuilder beforeSetupCluster(
            String clusterName, PulsarClusterSpec.PulsarClusterSpecBuilder builder) {
        // No surviving replica can mask data loss. Do not change the fast defaults of other integration tests.
        return builder.numBrokers(1).numBookies(1)
                // No replica exists to re-replicate to. Embedded AutoRecovery terminates its hosting bookie when
                // its metadata session expires, which would turn the pause case into a graceful shutdown instead.
                .bookkeeperEnvs(Map.of("journalSyncData", "true", "journalWriteData", "true",
                        "zkTimeout", "10000", "autoRecoveryDaemonEnabled", "false"))
                .brokerEnvs(Map.of(
                        "managedLedgerDefaultEnsembleSize", "1",
                        "managedLedgerDefaultWriteQuorum", "1",
                        "managedLedgerDefaultAckQuorum", "1",
                        "managedLedgerCacheSizeMB", "0",
                        "managedLedgerDefaultMarkDeleteRateLimit", "0",
                        "metadataStoreSessionTimeoutMillis", "10000"));
    }

    @Override
    public void tearDownCluster() throws Exception {
        try {
            if (pulsarAdmin != null) {
                pulsarAdmin.close();
            }
        } finally {
            super.tearDownCluster();
        }
    }

    @AfterMethod(alwaysRun = true)
    public void collectFailureLogs(ITestResult result) {
        if (!result.isSuccess() && pulsarCluster != null) {
            pulsarCluster.getBrokers().forEach(this::dumpServiceLogs);
            pulsarCluster.getBookies().forEach(this::dumpServiceLogs);
        }
    }

    private void dumpServiceLogs(PulsarContainer<?> container) {
        DockerUtils.dumpContainerLogToTarget(container.getDockerClient(), container.getContainerId());
        DockerUtils.dumpContainerDirToTargetCompressed(
                container.getDockerClient(), container.getContainerId(), "/var/log/pulsar");
    }

    @DataProvider
    public Object[][] components() {
        return new Object[][] {{"broker"}, {"bookie"}};
    }

    @Test(dataProvider = "components", timeOut = 300_000)
    public void testAbruptCrashWithPersistentRestart(String component) throws Exception {
        testDurability(component, false);
    }

    @Test(dataProvider = "components", timeOut = 300_000)
    public void testLongProcessPause(String component) throws Exception {
        testDurability(component, true);
    }

    private void testDurability(String component, boolean pause) throws Exception {
        String namespace = generateNamespaceName();
        pulsarCluster.createNamespace(namespace);
        // Keep even acknowledged messages so a new reader can verify every confirmed publish from storage.
        pulsarAdmin.namespaces().setRetention("public/" + namespace, new RetentionPolicies(60, 100));
        String topic = generateTopicName(namespace, "process-durability", true);
        Map<MessageId, Integer> confirmed = new ConcurrentHashMap<>();
        Set<MessageId> attemptedAcks = ConcurrentHashMap.newKeySet();
        Set<MessageId> confirmedAcks = ConcurrentHashMap.newKeySet();
        Set<MessageId> frozenAcks = new HashSet<>();
        MessageId end;
        try (PulsarClient client = newClient();
             Consumer<Integer> consumer = newConsumer(client, topic);
             Producer<Integer> producer = client.newProducer(Schema.INT32).topic(topic)
                     .enableBatching(false).maxPendingMessages(16).sendTimeout(10, TimeUnit.SECONDS).create()) {
            // Freeze a second cursor before the fault. Later acknowledgments on the live subscription must not
            // repair lost bookie cursor records from the broker's in-memory delete ranges and hide data loss.
            try (Consumer<Integer> audit = newConsumer(client, topic, FROZEN_SUBSCRIPTION)) {
                for (int i = 0; i < 32; i++) {
                    int value = 100_000 + i;
                    confirmed.put(producer.send(value), value);
                    Message<Integer> message = audit.receive(10, TimeUnit.SECONDS);
                    assertThat(message).as("audit seed message").isNotNull();
                    assertThat(message.getValue()).isEqualTo(value);
                    if (i % 2 == 0) {
                        audit.acknowledge(message);
                        frozenAcks.add(message.getMessageId());
                    }
                }
            }
            try (Workload workload = new Workload(producer, consumer, confirmed, attemptedAcks, confirmedAcks)) {
                await().atMost(RECOVERY_TIMEOUT).untilAsserted(() -> {
                    workload.assertRunning();
                    assertThat(confirmed.size()).as("seed and live publishes before fault")
                            .isGreaterThanOrEqualTo(64);
                    assertThat(confirmedAcks.size()).as("persisted acknowledgments before fault")
                            .isGreaterThanOrEqualTo(8);
                });
                PulsarContainer<?> victim = component.equals("broker") ? pulsarCluster.getBroker(0)
                        : pulsarCluster.getBookies().iterator().next();
                if (pause) {
                    pauseAndResume(victim);
                } else {
                    killAndRestart(victim);
                }
                int publishedAfterFault = confirmed.size();
                int acknowledgedAfterFault = confirmedAcks.size();
                // Keep the same clients connected: recovery must resume both publishing and durable ack processing.
                await().atMost(RECOVERY_TIMEOUT).untilAsserted(() -> {
                    workload.assertRunning();
                    assertThat(confirmed.size()).as("publishing resumes after fault")
                            .isGreaterThanOrEqualTo(publishedAfterFault + 32);
                    assertThat(confirmedAcks.size()).as("acknowledgments resume after fault")
                            .isGreaterThanOrEqualTo(acknowledgedAfterFault + 8);
                });
            }
            end = producer.send(END_MARKER);
            confirmed.put(end, END_MARKER);
        }

        // Discard broker caches and the in-memory cursor, including in the bookie-failure cases. No graceful unload
        // or broker shutdown is allowed to flush state and conceal a missing journal/cursor write.
        killAndRestart(pulsarCluster.getBroker(0));
        // Wait for this topic and its cursors, rather than writing to an unrelated healthcheck ledger.
        await().atMost(RECOVERY_TIMEOUT).ignoreExceptions().untilAsserted(() ->
                assertThat(pulsarAdmin.topics().getStats(topic).getSubscriptions())
                        .containsKeys(SUBSCRIPTION, FROZEN_SUBSCRIPTION));
        verifyStoredMessagesAndSubscription(topic, end, confirmed, attemptedAcks, confirmedAcks, frozenAcks);
    }

    private PulsarClient newClient() throws PulsarClientException {
        return PulsarClient.builder().serviceUrl(pulsarCluster.getPlainTextServiceUrl())
                .operationTimeout(10, TimeUnit.SECONDS).build();
    }

    private Consumer<Integer> newConsumer(PulsarClient client, String topic) throws PulsarClientException {
        return newConsumer(client, topic, SUBSCRIPTION);
    }

    private Consumer<Integer> newConsumer(PulsarClient client, String topic, String subscription)
            throws PulsarClientException {
        return client.newConsumer(Schema.INT32).topic(topic).subscriptionName(subscription)
                .receiverQueueSize(32).acknowledgmentGroupTime(0, TimeUnit.MILLISECONDS)
                .isAckReceiptEnabled(true).subscribe();
    }

    private void killAndRestart(PulsarContainer<?> victim) {
        String id = victim.getContainerId();
        // Container.stop() is graceful and removes the writable layer. Kill all processes without shutdown hooks,
        // then start the exact same container, retaining journal, ledgers, cookies, hostname and network aliases.
        victim.getDockerClient().killContainerCmd(id).withSignal("KILL").exec();
        try {
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                var state = victim.getDockerClient().inspectContainerCmd(id).exec().getState();
                assertThat(state.getRunning()).isFalse();
                assertThat(state.getExitCodeLong()).isEqualTo(137L);
                assertThat(state.getOOMKilled()).as("injected kill, not OOM").isFalse();
            });
        } finally {
            victim.getDockerClient().startContainerCmd(id).exec();
        }
        assertThat(victim.getContainerId()).as("restart must not replace the container or its data").isEqualTo(id);
    }

    private void pauseAndResume(PulsarContainer<?> victim) {
        String id = victim.getContainerId();
        victim.getDockerClient().pauseContainerCmd(id).exec();
        try {
            assertThat(victim.getDockerClient().inspectContainerCmd(id).exec().getState().getPaused()).isTrue();
            // Intentional fault duration, not a sleep used to guess when an asynchronous operation has completed.
            await().pollDelay(PAUSE_DURATION).atMost(PAUSE_DURATION.plusSeconds(5))
                    .until(() -> victim.getDockerClient().inspectContainerCmd(id).exec().getState().getPaused());
        } finally {
            victim.getDockerClient().unpauseContainerCmd(id).exec();
        }
    }

    private void verifyStoredMessagesAndSubscription(String topic, MessageId end,
            Map<MessageId, Integer> confirmed, Set<MessageId> attemptedAcks, Set<MessageId> confirmedAcks,
            Set<MessageId> frozenAcks) throws Exception {
        try (PulsarClient client = newClient();
             Reader<Integer> reader = client.newReader(Schema.INT32).topic(topic)
                     .startMessageId(MessageId.earliest).create()) {
            Map<MessageId, Integer> stored = new HashMap<>();
            long deadline = System.nanoTime() + RECOVERY_TIMEOUT.toNanos();
            while (!stored.containsKey(end) && System.nanoTime() < deadline) {
                Message<Integer> message = reader.readNext(1, TimeUnit.SECONDS);
                if (message != null) {
                    stored.put(message.getMessageId(), message.getValue());
                }
            }
            assertThat(stored).as("every successful send must remain readable with its original message ID and payload")
                    .containsAllEntriesOf(confirmed);

            Set<MessageId> required = new HashSet<>(confirmed.keySet());
            // An ack without a receipt has an uncertain outcome: it may or may not have persisted before the fault.
            required.removeAll(attemptedAcks);
            verifySubscription(client, topic, SUBSCRIPTION, end, stored, required, confirmedAcks);
            Set<MessageId> frozenRequired = new HashSet<>(confirmed.keySet());
            frozenRequired.removeAll(frozenAcks);
            verifySubscription(client, topic, FROZEN_SUBSCRIPTION, end, stored, frozenRequired, frozenAcks);
        }
    }

    private void verifySubscription(PulsarClient client, String topic, String subscription, MessageId end,
            Map<MessageId, Integer> stored, Set<MessageId> required, Set<MessageId> confirmedAcks) throws Exception {
        assertThat(required).as("deliberately unacknowledged backlog").hasSizeGreaterThan(1);
        try (Consumer<Integer> recovered = newConsumer(client, topic, subscription)) {
            Set<MessageId> delivered = new HashSet<>();
            long deadline = System.nanoTime() + RECOVERY_TIMEOUT.toNanos();
            while (!delivered.contains(end) && System.nanoTime() < deadline) {
                Message<Integer> message = recovered.receive(1, TimeUnit.SECONDS);
                if (message != null) {
                    assertThat(confirmedAcks).as("receipt-confirmed cursor state survives the failure")
                            .doesNotContain(message.getMessageId());
                    assertThat(message.getValue()).isEqualTo(stored.get(message.getMessageId()));
                    delivered.add(message.getMessageId());
                    recovered.acknowledgeAsync(message).get(10, TimeUnit.SECONDS);
                }
            }
            assertThat(delivered).as("all confirmed, unacknowledged messages recover on " + subscription)
                    .containsAll(required);
        }
        try (Consumer<Integer> drained = newConsumer(client, topic, subscription)) {
            assertThat(drained.receive(3, TimeUnit.SECONDS)).as("recovered subscription can drain and reopen")
                    .isNull();
        }
    }

    private static final class Workload implements AutoCloseable {
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final ExecutorService executor = Executors.newFixedThreadPool(2,
                new DefaultThreadFactory("process-durability"));
        private final Future<?> publishing;
        private final Future<?> acknowledging;

        private Workload(Producer<Integer> producer, Consumer<Integer> consumer, Map<MessageId, Integer> confirmed,
                Set<MessageId> attemptedAcks, Set<MessageId> confirmedAcks) {
            publishing = executor.submit(() -> {
                RateLimiter rate = RateLimiter.create(50);
                // Bound memory and traffic even if fault injection or recovery fails.
                for (int value = 0; running.get() && value < 8192; value++) {
                    rate.acquire();
                    try {
                        confirmed.put(producer.send(value), value);
                    } catch (PulsarClientException expectedDuringFault) {
                        // Only successful sends enter the durability oracle. A failed send may still be stored.
                    }
                }
            });
            acknowledging = executor.submit(() -> {
                while (running.get()) {
                    try {
                        Message<Integer> message = consumer.receive(1, TimeUnit.SECONDS);
                        if (message != null) {
                            // A duplicate may have been queued before its ack receipt arrived. Do not fail on it or
                            // re-ack it: only cold cursor replay can distinguish that race from lost durable state.
                            if (confirmedAcks.contains(message.getMessageId())) {
                                continue;
                            }
                            // Leave holes in the cursor: recovery must restore acked entries and pending backlog.
                            if (message.getValue() % 2 == 0) {
                                attemptedAcks.add(message.getMessageId());
                                consumer.acknowledge(message);
                                confirmedAcks.add(message.getMessageId());
                            }
                        }
                    } catch (PulsarClientException expectedDuringFault) {
                        // Ack receipts that never arrive have an uncertain outcome and are not asserted as persisted.
                    }
                }
            });
        }

        private void assertRunning() throws Exception {
            if (publishing.isDone()) {
                publishing.get();
                throw new AssertionError("Publisher stopped before fault recovery completed");
            }
            if (acknowledging.isDone()) {
                acknowledging.get();
                throw new AssertionError("Acknowledgment worker stopped before fault recovery completed");
            }
        }

        @Override
        public void close() throws Exception {
            running.set(false);
            try {
                publishing.get(30, TimeUnit.SECONDS);
                acknowledging.get(30, TimeUnit.SECONDS);
            } finally {
                executor.shutdownNow();
                assertThat(executor.awaitTermination(30, TimeUnit.SECONDS)).as("workload threads terminate").isTrue();
            }
        }
    }
}
