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
package org.apache.pulsar.broker.service;

import static org.assertj.core.api.Assertions.assertThat;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.opentelemetry.api.OpenTelemetry;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.namespace.NamespaceEphemeralData;
import org.apache.pulsar.broker.namespace.ServiceUnitUtils;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreException.BadVersionException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.Option;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Uses independent real metadata sessions and bookies: recovery must fence an existing writer, which the mock
 * BookKeeper open-with-recovery operation does not implement. This test therefore cannot use the shared mock cluster.
 */
@Test(groups = "broker")
public class NamespaceReadmissionAfterFencingTest {
    private static final int TIMEOUT_SECONDS = 30;

    @Test
    public void testFencedTopicCannotReadmitWhileOwnershipNotificationsAreDelayed() throws Exception {
        LocalBookkeeperEnsemble ensemble = new LocalBookkeeperEnsemble(3, 0);
        NotificationGatedPulsarService brokerA = null;
        PulsarService brokerB = null;
        PulsarAdmin admin = null;
        PulsarClient clientA = null;
        PulsarClient clientB = null;
        MetadataStoreExtended authority = null;
        Reader<byte[]> readerB = null;
        Producer<byte[]> reconnectedProducer = null;
        try {
            ensemble.start();
            String metadataUrl = "zk:127.0.0.1:" + ensemble.getZookeeperPort();
            brokerA = new NotificationGatedPulsarService(configuration(metadataUrl));
            brokerA.start();
            brokerB = new PulsarService(configuration(metadataUrl));
            brokerB.start();
            admin = PulsarAdmin.builder().serviceHttpUrl(brokerA.getWebServiceAddress()).build();
            admin.clusters().createCluster("readmission", ClusterData.builder()
                    .serviceUrl(brokerA.getWebServiceAddress()).build());
            admin.tenants().createTenant("readmission", TenantInfo.builder()
                    .allowedClusters(Set.of("readmission")).build());
            admin.namespaces().createNamespace("readmission/delayed-notifications", 1);
            String topicName = "persistent://readmission/delayed-notifications/topic";
            NamespaceBundle bundle = brokerA.getNamespaceService().getBundle(TopicName.get(topicName));
            String ownershipPath = ServiceUnitUtils.path(bundle);
            authority = MetadataStoreExtended.create(metadataUrl, MetadataStoreConfig.builder().build());

            brokerA.getNamespaceService().getOwnershipCache().tryAcquiringOwnership(bundle)
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertOwner(authority, ownershipPath, brokerA);
            clientA = PulsarClient.builder().serviceUrl(brokerA.getBrokerServiceUrl())
                    .operationTimeout(5, TimeUnit.SECONDS).build();
            byte[] originalMessage = "before-takeover".getBytes(StandardCharsets.UTF_8);
            try (Producer<byte[]> producer = clientA.newProducer().topic(topicName).enableBatching(false).create()) {
                producer.send(originalMessage);
            }
            admin.topics().createSubscription(topicName, "retained-history", MessageId.earliest);
            PersistentTopic oldTopicA = (PersistentTopic) brokerA.getBrokerService()
                    .getTopicReference(topicName).orElseThrow();
            ManagedLedgerImpl ledgerA = (ManagedLedgerImpl) oldTopicA.getManagedLedger();
            long originalLedger = ledgerA.getLastConfirmedEntry().getLedgerId();
            String managedLedgerPath = "/managed-ledgers/" + ledgerA.getName();
            NotificationGatedMetadataStore storeA = brokerA.store;
            storeA.observeCasFailure(managedLedgerPath);

            // Delay the complete real notification stream, including cache invalidation and lock listeners.
            // No event is discarded or delivered ahead of the deletion when the FIFO is finally released.
            storeA.pauseNotifications().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            GetResult originalOwner = assertOwner(authority, ownershipPath, brokerA);
            authority.delete(ownershipPath, Optional.of(originalOwner.getStat().getVersion()))
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            Awaitility.await().atMost(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .until(() -> storeA.hasDelayedDeletion(ownershipPath));
            brokerB.getNamespaceService().getOwnershipCache().tryAcquiringOwnership(bundle)
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            GetResult ownerB = assertOwner(authority, ownershipPath, brokerB);
            PersistentTopic topicB = (PersistentTopic) brokerB.getBrokerService().getTopic(topicName, true)
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS).orElseThrow();
            ManagedLedgerImpl ledgerB = (ManagedLedgerImpl) topicB.getManagedLedger();
            long ledgerCreatedByB = ledgerB.getLedgersInfo().lastKey();
            assertThat(ledgerCreatedByB).as("B recovers L0 and creates its own current ledger")
                    .isNotEqualTo(originalLedger);
            assertThat(ledgerB.getLedgersInfo().get(originalLedger).getEntries()).isEqualTo(1);
            clientB = PulsarClient.builder().serviceUrl(brokerB.getBrokerServiceUrl()).build();
            readerB = clientB.newReader().topic(topicName).startMessageId(MessageId.earliest).create();
            assertThat(readerB.readNext(TIMEOUT_SECONDS, TimeUnit.SECONDS).getData()).isEqualTo(originalMessage);
            assertThat(brokerA.getNamespaceService().getOwnershipCache().isNamespaceBundleOwned(bundle))
                    .as("A still has its normally acquired, cached active bundle before the write fails").isTrue();

            // Publish through A's real topic without an automatic producer retry racing the explicit readmission.
            // B's recovery causes a real bookie fence; A's rollover then loses the real managed-ledger metadata CAS.
            CompletableFuture<Exception> failedPublish = new CompletableFuture<>();
            ByteBuf entry = Commands.serializeMetadataAndPayload(Commands.ChecksumType.None,
                    new MessageMetadata().setProducerName("old-writer").setSequenceId(1).setPublishTime(0),
                    Unpooled.EMPTY_BUFFER);
            try {
                oldTopicA.publishMessage(entry, (error, ledgerId, entryId) -> failedPublish.complete(error));
                assertThat(failedPublish.get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                        .isInstanceOf(BrokerServiceException.TopicFencedException.class);
                assertThat(storeA.casFailure.get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                        .as("the actual store rejected A's stale managed-ledger CAS")
                        .isInstanceOf(BadVersionException.class);
            } finally {
                entry.release();
            }
            PulsarService oldBroker = brokerA;
            Awaitility.await().atMost(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .until(() -> oldBroker.getBrokerService().getTopicReference(topicName).isEmpty());
            assertOwner(authority, ownershipPath, brokerB);

            // A client entering through A may be rejected or redirected to B. A successful send is legitimate
            // when B owns the producer; it must never require A to open another local topic on its cached bundle.
            reconnectedProducer = clientA.newProducer().topic(topicName).enableBatching(false)
                    .sendTimeout(5, TimeUnit.SECONDS).createAsync().handle((producer, error) -> producer)
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (reconnectedProducer != null) {
                reconnectedProducer.sendAsync("after-takeover".getBytes(StandardCharsets.UTF_8))
                        .handle((messageId, error) -> null).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            Optional<Topic> readmittedOnA;
            try {
                readmittedOnA = brokerA.getBrokerService().getTopic(topicName, true)
                        .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (ExecutionException error) {
                assertThat(FutureUtil.unwrapCompletionException(error))
                        .as("readmission must be refused by the namespace ownership check")
                        .isInstanceOf(BrokerServiceException.ServiceUnitNotReadyException.class);
                readmittedOnA = Optional.empty();
            }
            GetResult stillOwnerB = assertOwner(authority, ownershipPath, brokerB);
            assertThat(stillOwnerB.getValue()).as("the authoritative owner remains B").isEqualTo(ownerB.getValue());
            assertThat(stillOwnerB.getStat().getVersion()).isEqualTo(ownerB.getStat().getVersion());
            storeA.assertNotificationsRemainDelayed(ownershipPath);
            assertThat(readmittedOnA).as("a fenced broker must revalidate ownership before opening a new topic")
                    .isEmpty();
            assertThat(brokerA.getBrokerService().getTopicReference(topicName)).isEmpty();
            assertThat(brokerB.getBrokerService().getTopicReference(topicName).orElseThrow()).isSameAs(topicB);
            assertThat(ledgerB.getLedgersInfo().lastKey()).isEqualTo(ledgerCreatedByB);
        } finally {
            // Release every queued real callback in arrival order before closing clients, topics, or brokers.
            try {
                if (brokerA != null && brokerA.store != null) {
                    brokerA.store.resumeNotifications().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    brokerA.store.assertNotificationsReplayedInOrder();
                }
            } finally {
                closeResources(reconnectedProducer, readerB, clientA, clientB, admin, brokerA, brokerB, authority,
                        ensemble::stop);
            }
        }
    }

    private static void closeResources(AutoCloseable... resources) throws Exception {
        Exception failure = null;
        for (AutoCloseable resource : resources) {
            if (resource != null) {
                try {
                    resource.close();
                } catch (Exception error) {
                    if (failure == null) {
                        failure = error;
                    } else {
                        failure.addSuppressed(error);
                    }
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static ServiceConfiguration configuration(String metadataUrl) {
        ServiceConfiguration config = new ServiceConfiguration();
        config.setClusterName("readmission");
        config.setMetadataStoreUrl(metadataUrl);
        config.setAdvertisedAddress("127.0.0.1");
        config.setBrokerServicePort(Optional.of(0));
        config.setWebServicePort(Optional.of(0));
        config.setBrokerShutdownTimeoutMs(0L);
        config.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0));
        config.setLoadBalancerEnabled(false);
        config.setBrokerDeleteInactiveTopicsEnabled(false);
        config.setSystemTopicEnabled(false);
        config.setTopicLevelPoliciesEnabled(false);
        config.setDefaultNumberOfNamespaceBundles(1);
        config.setNumIOThreads(1);
        Properties properties = new Properties();
        properties.put("bookkeeper_numWorkerThreads", "1");
        config.setProperties(properties);
        return config;
    }

    private static GetResult assertOwner(MetadataStoreExtended authority, String path, PulsarService broker)
            throws Exception {
        authority.sync(path).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        GetResult result = authority.get(path).get(TIMEOUT_SECONDS, TimeUnit.SECONDS).orElseThrow();
        NamespaceEphemeralData owner = ObjectMapperFactory.getMapper().reader()
                .readValue(result.getValue(), NamespaceEphemeralData.class);
        assertThat(owner.getNativeUrl()).as("authoritative bundle lock at %s", path)
                .isEqualTo(broker.getBrokerServiceUrl());
        assertThat(result.getStat().isEphemeral()).isTrue();
        return result;
    }

    private static final class NotificationGatedPulsarService extends PulsarService {
        private NotificationGatedMetadataStore store;

        private NotificationGatedPulsarService(ServiceConfiguration config) {
            super(config);
        }

        @Override
        public MetadataStoreExtended createLocalMetadataStore(PulsarMetadataEventSynchronizer synchronizer,
                                                              OpenTelemetry openTelemetry)
                throws MetadataStoreException {
            store = new NotificationGatedMetadataStore(getConfiguration().getMetadataStoreUrl(),
                    MetadataStoreConfig.builder().metadataStoreName(MetadataStoreConfig.METADATA_STORE)
                            .synchronizer(synchronizer).openTelemetry(openTelemetry).build());
            return store;
        }
    }

    private static final class NotificationGatedMetadataStore extends ZKMetadataStore {
        private CompletableFuture<Void> notificationTail = CompletableFuture.completedFuture(null);
        private CompletableFuture<Void> notificationGate = CompletableFuture.completedFuture(null);
        private final List<Notification> delayed = new ArrayList<>();
        private final List<Notification> replayed = new ArrayList<>();
        private final CompletableFuture<BadVersionException> casFailure = new CompletableFuture<>();
        private volatile String observedCasPath;

        private NotificationGatedMetadataStore(String url, MetadataStoreConfig config) throws MetadataStoreException {
            super(url, config, true);
        }

        private void observeCasFailure(String path) {
            observedCasPath = path;
        }

        @Override
        protected CompletableFuture<Stat> storePut(String path, byte[] data, Optional<Long> expectedVersion,
                                                   Set<Option> options) {
            return super.storePut(path, data, expectedVersion, options).whenComplete((stat, error) -> {
                if (path.equals(observedCasPath)
                        && FutureUtil.unwrapCompletionException(error) instanceof BadVersionException badVersion) {
                    casFailure.complete(badVersion);
                }
            });
        }

        private synchronized CompletableFuture<Void> pauseNotifications() {
            notificationGate = new CompletableFuture<>();
            return notificationTail;
        }

        @Override
        protected synchronized CompletableFuture<Void> receivedNotification(Notification notification) {
            // ZooKeeper can notify while its superclass constructor is installing the persistent watch.
            if (notificationTail == null) {
                return super.receivedNotification(notification);
            }
            CompletableFuture<Void> gate = notificationGate;
            boolean held = !gate.isDone();
            if (held) {
                delayed.add(notification);
            }
            notificationTail = notificationTail.thenCompose(ignore -> gate)
                    .thenCompose(ignore -> super.receivedNotification(notification)).thenRun(() -> {
                        if (held) {
                            synchronized (this) {
                                replayed.add(notification);
                            }
                        }
                    });
            return notificationTail;
        }

        private CompletableFuture<Void> resumeNotifications() {
            CompletableFuture<Void> gate;
            CompletableFuture<Void> tail;
            synchronized (this) {
                gate = notificationGate;
                notificationGate = CompletableFuture.completedFuture(null);
                tail = notificationTail;
            }
            gate.complete(null);
            return tail;
        }

        private synchronized void assertNotificationsRemainDelayed(String ownershipPath) {
            assertThat(notificationGate).isNotDone();
            assertThat(replayed).isEmpty();
            assertThat(delayed).contains(new Notification(NotificationType.Deleted, ownershipPath));
        }

        private synchronized boolean hasDelayedDeletion(String path) {
            return delayed.contains(new Notification(NotificationType.Deleted, path));
        }

        private synchronized void assertNotificationsReplayedInOrder() {
            assertThat(replayed).containsExactlyElementsOf(delayed);
        }
    }
}
