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
package org.apache.pulsar.client.api.v5;

import static org.assertj.core.api.Assertions.assertThat;
import com.fasterxml.jackson.databind.JsonNode;
import io.opentelemetry.api.OpenTelemetry;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.impl.MetaStoreImpl;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.resources.ScalableTopicMetadata;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.broker.service.PulsarMetadataEventSynchronizer;
import org.apache.pulsar.broker.service.SharedMultiBrokerPulsarCluster;
import org.apache.pulsar.broker.service.scalable.ScalableTopicController;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.AutoScalePolicyOverride;
import org.apache.pulsar.common.scalable.HashRange;
import org.apache.pulsar.common.scalable.ScalableTopicHashing;
import org.apache.pulsar.common.scalable.SegmentInfo;
import org.apache.pulsar.common.scalable.SegmentTopicName;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Option;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.FaultInjectionMetadataStore;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/** Controller failures at real storage boundaries, retaining the original logical client resources. */
public class V5MultiBrokerScalableLayoutRecoveryTest extends V5MultiBrokerClientBaseTest {

    private enum Operation {
        SPLIT, MERGE, REBUCKET
    }

    private enum Boundary {
        BEFORE_COMMIT, AFTER_FIRST_SEAL, AFTER_RESERVATION, AFTER_COMMIT
    }

    @Test
    public void testProducerAndConsumersRecoverAfterSplitBeforeCommit() throws Exception {
        verifyRecovery(Operation.SPLIT, Boundary.BEFORE_COMMIT);
    }

    @Test
    public void testProducerAndConsumersRecoverAfterRebucketBeforeCommit() throws Exception {
        verifyRecovery(Operation.REBUCKET, Boundary.BEFORE_COMMIT);
    }

    @Test
    public void testProducerAndConsumersRecoverAfterFirstMergeParentIsDurablySealed() throws Exception {
        verifyRecovery(Operation.MERGE, Boundary.AFTER_FIRST_SEAL);
    }

    @Test
    public void testProducerAndConsumersRecoverAfterReservationBeforeCreatingChildren() throws Exception {
        verifyRecovery(Operation.SPLIT, Boundary.AFTER_RESERVATION);
    }

    @Test
    public void testProducerAndConsumersRecoverAfterCommitResponseIsLost() throws Exception {
        verifyRecovery(Operation.SPLIT, Boundary.AFTER_COMMIT);
    }

    private void verifyRecovery(Operation operation, Boundary boundary) throws Exception {
        @Cleanup
        FaultablePulsarService leader = (FaultablePulsarService) SharedMultiBrokerPulsarCluster.get()
                .startExtraBroker(FaultablePulsarService::new);
        // Own the namespace before any lookup. The merge test can then interrupt the first
        // parent's actual managed-ledger metadata write, while both parents contain real data.
        for (var bundle : leader.getNamespaceService().getNamespaceBundleFactory()
                .getBundles(NamespaceName.get(getNamespace())).getBundles()) {
            leader.getNamespaceService().getOwnershipCache().tryAcquiringOwnership(bundle)
                    .get(10, TimeUnit.SECONDS);
        }
        TopicName topic = TopicName.get("topic://" + getNamespace() + "/recovery-" + UUID.randomUUID());
        int initialSegments = operation == Operation.MERGE ? 2 : 1;
        admin.scalableTopics().createScalableTopic(topic.toString(), initialSegments,
                Map.of("original", "retained"));
        var service = leader.getBrokerService().getScalableTopicService();
        ScalableTopicController controller = service.getOrCreateController(topic).get(10, TimeUnit.SECONDS);
        assertThat(controller.isLeader()).as("dedicated broker leads before clients connect").isTrue();
        ScalableTopicResources resources = brokers.get(0).getPulsarResources().getScalableTopicResources();
        ScalableTopicMetadata original = resources.getScalableTopicMetadataAsync(topic, true)
                .get(10, TimeUnit.SECONDS).orElseThrow();
        String metadataPath = resources.topicPath(topic);
        List<SegmentInfo> sources = original.getSegments().values().stream()
                .filter(SegmentInfo::isActive).toList();
        List<String> keys = keysForSegments(sources);
        @Cleanup
        Producer<String> producer = v5Client.newProducer(Schema.string()).topic(topic.toString())
                .sendTimeout(Duration.ofSeconds(20)).create();
        @Cleanup
        QueueConsumer<String> queue = v5Client.newQueueConsumer(Schema.string()).topic(topic.toString())
                .subscriptionName("queue").subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();
        @Cleanup
        StreamConsumer<String> stream = v5Client.newStreamConsumer(Schema.string()).topic(topic.toString())
                .subscriptionName("stream").subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST)
                .subscribe();
        @Cleanup
        CheckpointConsumer<String> checkpoint = v5Client.newCheckpointConsumer(Schema.string())
                .topic(topic.toString()).consumerGroup("checkpoint").startPosition(Checkpoint.earliest()).create();
        Set<String> sent = new HashSet<>();
        send(producer, keys, 0, 4, sent);

        // Materialize genuine followers before interrupting the leader. They retain their
        // own metadata-store sessions; only the dedicated leader has the injected fault.
        List<ScalableTopicController> followers = new ArrayList<>();
        for (PulsarService broker : brokers) {
            followers.add(broker.getBrokerService().getScalableTopicService()
                    .getOrCreateController(topic).get(10, TimeUnit.SECONDS));
        }
        String firstParentPath = managedLedgerPath(topic, sources.get(0));
        WriteGate gate = gate(boundary, metadataPath, firstParentPath, original.getEpoch());
        leader.metadataStore.gate = gate;
        CompletableFuture<?> attempt = switch (operation) {
            case SPLIT -> controller.splitSegment(sources.get(0).segmentId());
            case MERGE -> controller.mergeSegments(sources.get(0).segmentId(), sources.get(1).segmentId());
            case REBUCKET -> controller.rebucketSegment(sources.get(0).segmentId(), sources.get(0).bucketCount() + 1);
        };
        try {
            gate.entered.get(30, TimeUnit.SECONDS);
            if (boundary == Boundary.BEFORE_COMMIT || boundary == Boundary.AFTER_FIRST_SEAL) {
                assertThat(durablyTerminated(resources, firstParentPath))
                        .as("parent termination is persisted independently of the unpublished DAG").isTrue();
                assertThat(readMetadata(resources, metadataPath).path("epoch").asLong())
                        .as("published DAG is still the original generation").isEqualTo(original.getEpoch());
            }
            if (boundary == Boundary.AFTER_FIRST_SEAL) {
                assertThat(durablyTerminated(resources, managedLedgerPath(topic, sources.get(1))))
                        .as("merge was interrupted before the second parent was sealed").isFalse();
            }
            if (boundary == Boundary.AFTER_RESERVATION) {
                String segmentDirectory = firstParentPath.substring(0, firstParentPath.lastIndexOf('/'));
                assertThat(resources.getStore().getChildren(segmentDirectory).get(10, TimeUnit.SECONDS))
                        .as("reservation precedes physical child creation").hasSize(initialSegments);
                assertThat(durablyTerminated(resources, firstParentPath)).isFalse();
            }
            // An unrelated writer changes mutable topic configuration while the operation
            // is in flight. Publishing/replaying the fixed DAG must preserve these changes.
            AutoScalePolicyOverride policy = AutoScalePolicyOverride.builder().enabled(false).build();
            resources.updateScalableTopicAsync(topic, md -> {
                md.setProperties(Map.of("original", "retained", "concurrent", "retained"));
                md.setAutoScalePolicy(policy);
                return md;
            }).get(10, TimeUnit.SECONDS);

            service.releaseController(topic).get(10, TimeUnit.SECONDS);
            gate.failResponse();
            // Closing the dedicated broker forces the original consumer sessions to reconnect.
            // No producer, consumer, subscription or topic is manually repaired or recreated.
            leader.close();
            Awaitility.await().atMost(Duration.ofSeconds(30))
                    .until(() -> followers.stream().anyMatch(ScalableTopicController::isLeader));

            // Keep this data-path assertion before assertions about the new recovery marker:
            // on unpatched brokers, the same producer remains routed to a terminated parent.
            send(producer, keys, 4, 12, sent);
            receiveQueue(queue, sent);
            receiveOrdered(stream::receive, stream::acknowledgeCumulative, sent);
            receiveOrdered(checkpoint::receive, ignored -> { }, sent);
            assertThat(checkpoint.checkpoint().toByteArray()).isNotEmpty();

            ScalableTopicMetadata recovered = resources.getScalableTopicMetadataAsync(topic, true)
                    .get(10, TimeUnit.SECONDS).orElseThrow();
            assertThat(recovered.getEpoch()).as("exactly one DAG generation is committed")
                    .isEqualTo(original.getEpoch() + 1);
            int replacements = operation == Operation.SPLIT ? 2 : 1;
            assertThat(recovered.getNextSegmentId()).isEqualTo(original.getNextSegmentId() + replacements);
            Set<Long> expectedIds = new HashSet<>(original.getSegments().keySet());
            for (int i = 0; i < replacements; i++) {
                expectedIds.add(original.getNextSegmentId() + i);
            }
            assertThat(recovered.getSegments().keySet()).containsExactlyInAnyOrderElementsOf(expectedIds);
            for (SegmentInfo source : sources) {
                assertThat(recovered.getSegments().get(source.segmentId()).isSealed()).isTrue();
            }
            assertThat(recovered.getSegments().values().stream().filter(SegmentInfo::isActive).count())
                    .isEqualTo(replacements);
            assertThat(recovered.getProperties())
                    .containsExactlyInAnyOrderEntriesOf(Map.of("original", "retained", "concurrent", "retained"));
            assertThat(recovered.getAutoScalePolicy()).isEqualTo(policy);
            JsonNode pending = readMetadata(resources, metadataPath).path("pendingLayoutTransition");
            assertThat(pending.isMissingNode() || pending.isNull())
                    .as("the recovery intent is cleared atomically with the published DAG").isTrue();
        } finally {
            gate.failResponse();
            attempt.exceptionally(ignored -> null);
        }
    }

    private static WriteGate gate(Boundary boundary, String topicPath, String firstParentPath, long baseEpoch) {
        if (boundary == Boundary.AFTER_FIRST_SEAL) {
            return new WriteGate((path, value) -> path.equals(firstParentPath) && terminated(value), true);
        }
        return new WriteGate((path, value) -> {
            if (!path.equals(topicPath)) {
                return false;
            }
            JsonNode metadata = json(value);
            if (boundary == Boundary.AFTER_RESERVATION) {
                return metadata.hasNonNull("pendingLayoutTransition") && metadata.path("epoch").asLong() == baseEpoch;
            }
            return metadata.path("epoch").asLong() == baseEpoch + 1;
        }, boundary != Boundary.BEFORE_COMMIT);
    }

    private static String managedLedgerPath(TopicName topic, SegmentInfo segment) {
        return "/managed-ledgers/" + TopicName.get(SegmentTopicName.backingTopicName(topic, segment))
                .getPersistenceNamingEncoding();
    }

    private static boolean durablyTerminated(ScalableTopicResources resources, String path) throws Exception {
        var result = resources.getStore().get(path).get(10, TimeUnit.SECONDS);
        return result.isPresent() && terminated(result.orElseThrow().getValue());
    }

    private static boolean terminated(byte[] value) {
        try {
            return new MetaStoreImpl(null, null).parseManagedLedgerInfo(value).hasTerminatedPosition();
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot read actual managed-ledger metadata", e);
        }
    }

    private static JsonNode readMetadata(ScalableTopicResources resources, String path) throws Exception {
        return json(resources.getStore().get(path).get(10, TimeUnit.SECONDS).orElseThrow().getValue());
    }

    private static JsonNode json(byte[] value) {
        try {
            return ObjectMapperFactory.getMapper().reader().readTree(value);
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot read scalable-topic metadata", e);
        }
    }

    private static List<String> keysForSegments(List<SegmentInfo> segments) {
        List<String> result = new ArrayList<>();
        for (SegmentInfo segment : segments) {
            // Both halves already carry data before a split, so recovery must preserve
            // ordered reads and provision subscriptions on both successor segments.
            for (HashRange range : segment.hashRange().split()) {
                for (int i = 0; ; i++) {
                    String key = "key-" + i;
                    int hash = ScalableTopicHashing.segmentHash(
                            ScalableTopicHashing.murmur(key.getBytes(StandardCharsets.UTF_8)));
                    if (range.contains(hash)) {
                        result.add(key);
                        break;
                    }
                }
            }
        }
        return result;
    }

    private static void send(Producer<String> producer, List<String> keys, int first, int end, Set<String> sent)
            throws Exception {
        for (int i = first; i < end; i++) {
            for (String key : keys) {
                String value = key + ":" + i;
                producer.newMessage().key(key).value(value).send();
                sent.add(value);
            }
        }
    }

    private static void receiveQueue(QueueConsumer<String> consumer, Set<String> sent) {
        Set<String> received = new HashSet<>();
        Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> {
            Message<String> message = consumer.receive(Duration.ofSeconds(1));
            if (message != null) {
                assertThat(sent).contains(message.value());
                received.add(message.value());
                consumer.acknowledge(message.id());
            }
            return received.equals(sent);
        });
    }

    private static void receiveOrdered(Receiver receiver, Consumer<MessageId> acknowledge,
                                       Set<String> sent) {
        Set<String> received = new HashSet<>();
        Map<String, Integer> next = new HashMap<>();
        Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> {
            Message<String> message = receiver.receive(Duration.ofSeconds(1));
            if (message != null) {
                String value = message.value();
                assertThat(sent).contains(value);
                if (received.add(value)) {
                    String key = message.key().orElseThrow();
                    int sequence = Integer.parseInt(value.substring(value.lastIndexOf(':') + 1));
                    assertThat(sequence).as("first delivery order for key %s", key)
                            .isEqualTo(next.getOrDefault(key, 0));
                    next.put(key, sequence + 1);
                }
                acknowledge.accept(message.id());
            }
            return received.equals(sent);
        });
    }

    @FunctionalInterface
    private interface Receiver {
        Message<String> receive(Duration timeout) throws PulsarClientException;
    }

    private static class WriteGate {
        private final BiPredicate<String, byte[]> predicate;
        private final boolean persistFirst;
        private final AtomicBoolean intercepted = new AtomicBoolean();
        private final CompletableFuture<Void> entered = new CompletableFuture<>();
        private final CompletableFuture<Stat> response = new CompletableFuture<>();

        WriteGate(BiPredicate<String, byte[]> predicate, boolean persistFirst) {
            this.predicate = predicate;
            this.persistFirst = persistFirst;
        }

        void failResponse() {
            response.completeExceptionally(new MetadataStoreException("Injected lost response from former leader"));
        }
    }

    private static class HoldingMetadataStore extends FaultInjectionMetadataStore {
        private volatile WriteGate gate;

        HoldingMetadataStore(MetadataStoreExtended store) {
            super(store);
        }

        @Override
        public CompletableFuture<Stat> put(String path, byte[] value, Optional<Long> version, Set<Option> options) {
            WriteGate current = gate;
            if (current == null || !current.predicate.test(path, value)
                    || !current.intercepted.compareAndSet(false, true)) {
                return super.put(path, value, version, options);
            }
            CompletableFuture<?> persisted = current.persistFirst
                    ? super.put(path, value, version, options) : CompletableFuture.completedFuture(null);
            return persisted.thenCompose(ignored -> {
                current.entered.complete(null);
                return current.response;
            });
        }
    }

    private static class FaultablePulsarService extends PulsarService {
        private HoldingMetadataStore metadataStore;

        FaultablePulsarService(ServiceConfiguration configuration) {
            super(configuration);
        }

        @Override
        public MetadataStoreExtended createLocalMetadataStore(PulsarMetadataEventSynchronizer synchronizer,
                                                               OpenTelemetry telemetry)
                throws MetadataStoreException, PulsarServerException {
            metadataStore = new HoldingMetadataStore(super.createLocalMetadataStore(synchronizer, telemetry));
            return metadataStore;
        }
    }
}
