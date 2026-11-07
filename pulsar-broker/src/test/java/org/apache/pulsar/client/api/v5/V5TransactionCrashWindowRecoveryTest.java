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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.google.common.collect.Sets;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.PulsarMockBookKeeper;
import org.apache.bookkeeper.client.PulsarMockLedgerHandle;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.MetaStoreImpl;
import org.apache.bookkeeper.mledger.proto.ManagedCursorInfo;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.metadata.TxnHeader;
import org.apache.pulsar.broker.transaction.metadata.TxnIds;
import org.apache.pulsar.broker.transaction.metadata.TxnMetadataStore;
import org.apache.pulsar.broker.transaction.metadata.TxnOp;
import org.apache.pulsar.broker.transaction.metadata.TxnPaths;
import org.apache.pulsar.broker.transaction.metadata.TxnState;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.api.v5.config.BatchingPolicy;
import org.apache.pulsar.client.api.v5.config.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.v5.config.TransactionPolicy;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.common.api.proto.TxnAction;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.scalable.HashRange;
import org.apache.pulsar.common.scalable.SegmentTopicName;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.ScanConsumer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.MockZooKeeper;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Uses an isolated restartable broker to reload OPEN write/ACK state before its outcome materializes. */
public class V5TransactionCrashWindowRecoveryTest extends MockedPulsarServiceBaseTest {
    private final String namespace = "pulsar/txn-crash-window";
    private PulsarClient client;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        ServiceConfiguration configuration = getDefaultConf();
        configuration.setTransactionCoordinatorEnabled(true);
        configuration.setTopicLevelPoliciesEnabled(false);
        // Keep the configured background checkpoint outside the bounded volatile-state-loss test.
        configuration.setManagedLedgerCursorPositionFlushSeconds(3600);
        super.internalSetup(configuration);
        admin.clusters().createCluster("test",
                ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant(NamespaceName.SYSTEM_NAMESPACE.getTenant(),
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet("test")));
        admin.namespaces().createNamespace(namespace, Sets.newHashSet("test"));
        admin.namespaces().createNamespace(NamespaceName.SYSTEM_NAMESPACE.toString());
        client = newClient();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        try {
            if (client != null) {
                client.close();
                client = null;
            }
        } finally {
            super.internalCleanup();
        }
    }

    @Test
    public void firstOpenTransactionRemainsInvisibleAfterOwnerRestart() throws Exception {
        String topic = newTopic();
        String segment = onlySegment(topic);
        TxnID transactionId;
        try (Producer<String> producer = producer(topic)) {
            Transaction transaction = client.newTransaction();
            producer.newMessage().transaction(transaction).value("still-open").send();
            transactionId = onlyOpenTransaction();
            assertThat(transactionStore().getSegmentWatermark(segment).get(10, TimeUnit.SECONDS))
                    .as("the first transaction has no earlier resolved visibility watermark").isEmpty();
        }
        restartAndReconnect();
        try (QueueConsumer<String> consumer = subscribe(topic, "first-open")) {
            assertThat(consumer.receive(Duration.ofSeconds(1)))
                    .as("recovery must not expose OPEN data when no durable watermark existed").isNull();
            pulsar.getTransactionCoordinatorV5().endTransaction(transactionId, TxnAction.ABORT_VALUE)
                    .get(10, TimeUnit.SECONDS);
            try (Producer<String> producer = producer(topic)) {
                producer.newMessage().value("after-abort").send();
            }
            Message<String> sentinel = consumer.receive(Duration.ofSeconds(10));
            assertThat(sentinel).isNotNull();
            assertThat(sentinel.value()).isEqualTo("after-abort");
            assertThat(consumer.receive(Duration.ofMillis(300))).isNull();
        }
    }

    @Test
    public void persistedOpenTransactionalAcksRecoverBeforeCommit() throws Exception {
        String topic = newTopic();
        String segment = onlySegment(topic);
        String subscription = "open-acks";
        TxnID transactionId;
        try (Producer<String> producer = producer(topic);
                QueueConsumer<String> consumer = subscribe(topic, subscription)) {
            for (int i = 0; i < 3; i++) {
                producer.newMessage().value("input-" + i).send();
            }
            Transaction transaction = client.newTransaction();
            transactionId = onlyOpenTransaction();
            for (int i = 0; i < 3; i++) {
                Message<String> message = consumer.receive(Duration.ofSeconds(10));
                assertThat(message).as("seed delivery %s", i).isNotNull();
                consumer.acknowledge(message.id(), transaction);
            }
            // The public V5 ACK returns void. Prove actual metadata persistence before owner restart.
            Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(ackOps(segment, subscription)).hasSize(3));
            assertThat(transactionStore().getHeader(TxnIds.toKey(transactionId)).get(10, TimeUnit.SECONDS))
                    .hasValueSatisfying(header -> assertThat(header.value().getState()).isEqualTo(TxnState.OPEN));
        }
        restartAndReconnect();
        try (QueueConsumer<String> recovered = subscribe(topic, subscription)) {
            pulsar.getTransactionCoordinatorV5().endTransaction(transactionId, TxnAction.COMMIT_VALUE)
                    .get(10, TimeUnit.SECONDS);
            Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(ackOps(segment, subscription)).isEmpty());
            assertThat(recovered.receive(Duration.ofSeconds(2)))
                    .as("committed ACK positions recovered from metadata must advance the durable cursor").isNull();
        }
    }

    @Test
    public void partialBatchTransactionalAckRecoversWithoutAcknowledgingOtherMessages() throws Exception {
        String topic = newTopic();
        String segment = onlySegment(topic);
        String subscription = "partial-batch-ack";
        TxnID transactionId;
        MessageIdAdv first;
        try (Producer<String> producer = client.newProducer(Schema.string()).topic(topic)
                .batchingPolicy(BatchingPolicy.builder().maxMessages(4)
                        .maxPublishDelay(Duration.ofSeconds(5)).build()).create()) {
            List<CompletableFuture<MessageId>> sends = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                sends.add(producer.async().newMessage().key("same-batch").value("input-" + i).send());
            }
            first = physical(sends.get(0).get(10, TimeUnit.SECONDS));
            assertThat(first.getBatchIndex()).as("actual first batched message index").isZero();
            for (int i = 0; i < sends.size(); i++) {
                MessageIdAdv message = physical(sends.get(i).get(10, TimeUnit.SECONDS));
                assertThat(message.getLedgerId()).isEqualTo(first.getLedgerId());
                assertThat(message.getEntryId()).isEqualTo(first.getEntryId());
                assertThat(message.getBatchIndex()).as("actual batch index %s", i).isEqualTo(i);
            }
            try (QueueConsumer<String> consumer = subscribe(topic, subscription)) {
                Message<String> message = consumer.receive(Duration.ofSeconds(10));
                assertThat(message).isNotNull();
                assertThat(message.value()).isEqualTo("input-0");
                assertThat(physical(message.id()).getBatchSize()).isEqualTo(4);
                Transaction transaction = client.newTransaction();
                transactionId = onlyOpenTransaction();
                consumer.acknowledge(message.id(), transaction);
                Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                        assertThat(ackOps(segment, subscription)).hasSize(1));
                assertThat(transactionStore().getHeader(TxnIds.toKey(transactionId)).get(10, TimeUnit.SECONDS))
                        .hasValueSatisfying(header -> assertThat(header.value().getState()).isEqualTo(TxnState.OPEN));
            }
        }
        restartAndReconnect();
        try (QueueConsumer<String> recovered = subscribe(topic, subscription)) {
            pulsar.getTransactionCoordinatorV5().endTransaction(transactionId, TxnAction.COMMIT_VALUE)
                    .get(10, TimeUnit.SECONDS);
            Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(cursorBatchMask(segment, subscription, first)).containsExactly(14L));
            List<String> values = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                Message<String> message = recovered.receive(Duration.ofSeconds(5));
                assertThat(message).as("unacknowledged batch member %s must remain readable", i).isNotNull();
                values.add(message.value());
            }
            assertThat(values).containsExactlyInAnyOrder("input-1", "input-2", "input-3");
            assertThat(recovered.receive(Duration.ofMillis(500)))
                    .as("only the first batch member was transactionally acknowledged").isNull();
        }
    }

    @Test
    public void committedPartialBatchAckSurvivesLossOfVolatileCursorState() throws Exception {
        String topic = newTopic();
        String segment = onlySegment(topic);
        String subscription = "committed-partial-batch";
        TxnID transactionId;
        CursorImage image;
        try (Producer<String> producer = client.newProducer(Schema.string()).topic(topic)
                .batchingPolicy(BatchingPolicy.builder().maxMessages(4)
                        .maxPublishDelay(Duration.ofSeconds(5)).build()).create();
                QueueConsumer<String> consumer = subscribe(topic, subscription)) {
            List<CompletableFuture<MessageId>> sends = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                sends.add(producer.async().newMessage().key("same-batch").value("input-" + i).send());
            }
            MessageIdAdv first = physical(sends.get(0).get(10, TimeUnit.SECONDS));
            for (int i = 0; i < sends.size(); i++) {
                MessageIdAdv actual = physical(sends.get(i).get(10, TimeUnit.SECONDS));
                assertThat(actual.getLedgerId()).isEqualTo(first.getLedgerId());
                assertThat(actual.getEntryId()).isEqualTo(first.getEntryId());
                assertThat(actual.getBatchIndex()).isEqualTo(i);
            }
            Message<String> selected = consumer.receive(Duration.ofSeconds(10));
            assertThat(selected).isNotNull();
            assertThat(selected.value()).isEqualTo("input-0");
            assertThat(physical(selected.id()).getBatchSize()).isEqualTo(4);
            Transaction transaction = client.newTransaction();
            transactionId = onlyOpenTransaction();
            consumer.acknowledge(selected.id(), transaction);
            Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(ackOps(segment, subscription)).hasSize(1));
            transaction.commit();
            assertThat(transactionStore().getHeader(TxnIds.toKey(transactionId)).get(10, TimeUnit.SECONDS))
                    .hasValueSatisfying(header -> assertThat(header.value().getState()).isEqualTo(TxnState.COMMITTED));
            Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(cursorBatchMask(segment, subscription, first)).containsExactly(14L));
            long[] memoryMask = cursorBatchMask(segment, subscription, first);
            assertThat(memoryMask).as("committed partial ACK was applied to the live cursor").containsExactly(14L);
            // Capture actual persisted state after the committed ACK is locally applied. Recovery
            // must have either a durable cursor checkpoint or the original ACK operation records.
            image = captureCursorImage(segment, subscription);
        }

        client.close();
        client = null;
        stopBroker();
        // Closing the in-JVM broker is only resource cleanup. Restore the exact already captured
        // durable cursor image so its graceful checkpoint cannot stand in for volatile-state loss.
        restoreCursorImage(image);
        startBroker();
        client = newClient();
        assertThat(transactionStore().getHeader(TxnIds.toKey(transactionId)).get(10, TimeUnit.SECONDS))
                .hasValueSatisfying(header -> assertThat(header.value().getState()).isEqualTo(TxnState.COMMITTED));
        try (QueueConsumer<String> recovered = subscribe(topic, subscription)) {
            List<String> values = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                Message<String> message = recovered.receive(Duration.ofSeconds(2));
                if (message == null) {
                    break;
                }
                values.add(message.value());
            }
            assertThat(values).as("a committed ACK must survive loss of volatile cursor state")
                    .containsExactlyInAnyOrder("input-1", "input-2", "input-3");
        }
    }

    @Test
    public void seekToEarliestSurvivesRestartAfterCommittedPartialBatchAck() throws Exception {
        String topic = newTopic();
        String segment = onlySegment(topic);
        String subscription = "seek-after-partial-batch";
        MessageIdAdv first;
        try (Producer<String> producer = client.newProducer(Schema.string()).topic(topic)
                .batchingPolicy(BatchingPolicy.builder().maxMessages(4)
                        .maxPublishDelay(Duration.ofSeconds(5)).build()).create();
                QueueConsumer<String> consumer = subscribe(topic, subscription)) {
            List<CompletableFuture<MessageId>> sends = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                sends.add(producer.async().newMessage().key("same-batch").value("input-" + i).send());
            }
            first = physical(sends.get(0).get(10, TimeUnit.SECONDS));
            for (int i = 0; i < sends.size(); i++) {
                MessageIdAdv actual = physical(sends.get(i).get(10, TimeUnit.SECONDS));
                assertThat(actual.getEntryId()).isEqualTo(first.getEntryId());
                assertThat(actual.getBatchIndex()).isEqualTo(i);
            }
            Message<String> selected = consumer.receive(Duration.ofSeconds(10));
            assertThat(selected).isNotNull();
            assertThat(selected.value()).isEqualTo("input-0");
            Transaction transaction = client.newTransaction();
            consumer.acknowledge(selected.id(), transaction);
            Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(ackOps(segment, subscription)).hasSize(1));
            transaction.commit();
            Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(cursorBatchMask(segment, subscription, first)).containsExactly(14L));
        }
        assertThat(ackOps(segment, subscription))
                .as("the committed partial-batch ACK is retained above the durable prefix").hasSize(1);

        admin.scalableTopics().seekSubscription(topic, subscription, 0L);
        assertThat(cursorBatchMask(segment, subscription, first))
                .as("the successful seek cleared the live batch ACK").isNull();

        restartAndReconnect();
        try (QueueConsumer<String> recovered = subscribe(topic, subscription)) {
            List<String> values = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                Message<String> message = recovered.receive(Duration.ofSeconds(3));
                if (message == null) {
                    break;
                }
                values.add(message.value());
            }
            assertThat(values).as("a seek to earliest must survive owner restart")
                    .containsExactlyInAnyOrder("input-0", "input-1", "input-2", "input-3");
        }
    }

    @Test
    public void seekToEarliestSurvivesRestartAfterCommittedOutOfOrderAck() throws Exception {
        String topic = newTopic();
        String segment = onlySegment(topic);
        String subscription = "seek-after-out-of-order";
        try (Producer<String> producer = producer(topic);
                QueueConsumer<String> consumer = subscribe(topic, subscription)) {
            for (int i = 0; i < 3; i++) {
                producer.newMessage().value("input-" + i).send();
            }
            Message<String> m0 = consumer.receive(Duration.ofSeconds(10));
            Message<String> m1 = consumer.receive(Duration.ofSeconds(10));
            assertThat(m0).isNotNull();
            assertThat(m1).isNotNull();
            assertThat(m1.value()).isEqualTo("input-1");
            Transaction transaction = client.newTransaction();
            consumer.acknowledge(m1.id(), transaction);
            Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(ackOps(segment, subscription)).hasSize(1));
            transaction.commit();
            Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
                PersistentTopic t = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(segment)
                        .get(10, TimeUnit.SECONDS).orElseThrow();
                MessageIdAdv id = physical(m1.id());
                assertThat(t.getSubscription(subscription).getCursor()
                        .isMessageDeleted(PositionFactory.create(id.getLedgerId(), id.getEntryId()))).isTrue();
            });
        }
        assertThat(ackOps(segment, subscription))
                .as("the committed ACK above the unacknowledged input-0 is retained").hasSize(1);

        admin.scalableTopics().seekSubscription(topic, subscription, 0L);

        restartAndReconnect();
        try (QueueConsumer<String> recovered = subscribe(topic, subscription)) {
            List<String> values = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                Message<String> message = recovered.receive(Duration.ofSeconds(3));
                if (message == null) {
                    break;
                }
                values.add(message.value());
            }
            assertThat(values).as("a seek to earliest must survive owner restart")
                    .containsExactlyInAnyOrder("input-0", "input-1", "input-2");
        }
    }

    @Test
    public void seekToEarliestKeepsOpenTransactionalAckAcrossRestart() throws Exception {
        String topic = newTopic();
        String segment = onlySegment(topic);
        String subscription = "seek-with-open-ack";
        TxnID transactionId;
        MessageIdAdv acked;
        try (Producer<String> producer = producer(topic);
                QueueConsumer<String> consumer = subscribe(topic, subscription)) {
            for (int i = 0; i < 3; i++) {
                producer.newMessage().value("input-" + i).send();
            }
            assertThat(consumer.receive(Duration.ofSeconds(10))).isNotNull();
            Message<String> m1 = consumer.receive(Duration.ofSeconds(10));
            assertThat(m1).isNotNull();
            assertThat(m1.value()).isEqualTo("input-1");
            acked = physical(m1.id());
            Transaction transaction = client.newTransaction();
            transactionId = onlyOpenTransaction();
            consumer.acknowledge(m1.id(), transaction);
            Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(ackOps(segment, subscription)).hasSize(1));
        }

        admin.scalableTopics().seekSubscription(topic, subscription, 0L);
        assertThat(ackOps(segment, subscription)).as("the reset keeps the ACK of an OPEN transaction").hasSize(1);

        restartAndReconnect();
        try (QueueConsumer<String> recovered = subscribe(topic, subscription)) {
            pulsar.getTransactionCoordinatorV5().endTransaction(transactionId, TxnAction.COMMIT_VALUE)
                    .get(10, TimeUnit.SECONDS);
            Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
                PersistentTopic t = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(segment)
                        .get(10, TimeUnit.SECONDS).orElseThrow();
                assertThat(t.getSubscription(subscription).getCursor().isMessageDeleted(
                        PositionFactory.create(acked.getLedgerId(), acked.getEntryId())))
                        .as("the OPEN transaction's ACK applies when it commits after the reset").isTrue();
            });
            List<String> values = new ArrayList<>();
            for (int i = 0; i < 3; i++) {
                Message<String> message = recovered.receive(Duration.ofSeconds(3));
                if (message == null) {
                    break;
                }
                values.add(message.value());
            }
            assertThat(values).containsExactlyInAnyOrder("input-0", "input-2");
        }
    }

    @Test
    public void failedSeekKeepsCommittedPartialBatchAckAcrossVolatileCursorLoss() throws Exception {
        String topic = newTopic();
        String segment = onlySegment(topic);
        String subscription = "failed-seek-partial-batch";
        MessageIdAdv first;
        CursorImage image;
        try (Producer<String> producer = client.newProducer(Schema.string()).topic(topic)
                .batchingPolicy(BatchingPolicy.builder().maxMessages(4)
                        .maxPublishDelay(Duration.ofSeconds(5)).build()).create();
                QueueConsumer<String> consumer = subscribe(topic, subscription)) {
            List<CompletableFuture<MessageId>> sends = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                sends.add(producer.async().newMessage().key("same-batch").value("input-" + i).send());
            }
            first = physical(sends.get(0).get(10, TimeUnit.SECONDS));
            for (int i = 0; i < sends.size(); i++) {
                MessageIdAdv actual = physical(sends.get(i).get(10, TimeUnit.SECONDS));
                assertThat(actual.getEntryId()).isEqualTo(first.getEntryId());
                assertThat(actual.getBatchIndex()).isEqualTo(i);
            }
            Message<String> selected = consumer.receive(Duration.ofSeconds(10));
            assertThat(selected).isNotNull();
            assertThat(selected.value()).isEqualTo("input-0");
            Transaction transaction = client.newTransaction();
            consumer.acknowledge(selected.id(), transaction);
            Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(ackOps(segment, subscription)).hasSize(1));
            transaction.commit();
            Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                    assertThat(cursorBatchMask(segment, subscription, first)).containsExactly(14L));
            image = captureCursorImage(segment, subscription);
        }
        assertThat(ackOps(segment, subscription)).hasSize(1);

        // Fail the persistence of the reset: fence the cursor ledger, and fail the metadata-store fallback once.
        PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(segment)
                .get(10, TimeUnit.SECONDS).orElseThrow();
        PersistentSubscription persistentSubscription = persistentTopic.getSubscription(subscription);
        long cursorLedger = ((ManagedCursorImpl) persistentSubscription.getCursor()).getCursorLedger();
        if (cursorLedger >= 0) {
            pulsarTestContext.getMockBookKeeper().getLedgerMap().get(cursorLedger).close();
        }
        AtomicBoolean armed = new AtomicBoolean(true);
        mockZooKeeper.failConditional(KeeperException.Code.CONNECTIONLOSS, (op, path) ->
                armed.get() && op == MockZooKeeper.Op.SET && path.equals(image.metadataPath()));
        try {
            assertThatThrownBy(() -> persistentSubscription.resetCursor(PositionFactory.EARLIEST)
                    .get(10, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class);
        } finally {
            armed.set(false);
        }
        assertThat(ackOps(segment, subscription)).as("the failed reset recreates the record it dropped").hasSize(1);
        assertThat(cursorBatchMask(segment, subscription, first))
                .as("the reset was not persisted, so the live cursor keeps the committed ACK").containsExactly(14L);

        client.close();
        client = null;
        stopBroker();
        restoreCursorImage(image);
        startBroker();
        client = newClient();
        try (QueueConsumer<String> recovered = subscribe(topic, subscription)) {
            List<String> values = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                Message<String> message = recovered.receive(Duration.ofSeconds(2));
                if (message == null) {
                    break;
                }
                values.add(message.value());
            }
            assertThat(values).as("a committed ACK must survive a failed seek and the loss of volatile cursor state")
                    .containsExactlyInAnyOrder("input-1", "input-2", "input-3");
        }
    }

    private long[] cursorBatchMask(String segment, String subscription, MessageIdAdv position) throws Exception {
        PersistentTopic topic = (PersistentTopic) pulsar.getBrokerService().getTopicIfExists(segment)
                .get(10, TimeUnit.SECONDS).orElseThrow();
        return topic.getSubscription(subscription).getCursor().getBatchPositionAckSet(
                PositionFactory.create(position.getLedgerId(), position.getEntryId()));
    }

    private record CursorImage(String metadataPath, byte[] metadataBytes, long ledgerId,
                               BookKeeper.DigestType digest, byte[] password,
                               Map<String, byte[]> customMetadata, boolean closed, List<byte[]> entries) {
    }

    private CursorImage captureCursorImage(String segment, String subscription) throws Exception {
        String path = "/managed-ledgers/" + TopicName.get(segment).getPersistenceNamingEncoding() + "/" + subscription;
        byte[] bytes = pulsar.getLocalMetadataStore().get(path).get(10, TimeUnit.SECONDS)
                .orElseThrow().getValue().clone();
        assertThat(mockZooKeeper.getData(path, false, null)).containsExactly(bytes);
        ManagedCursorInfo info;
        MetaStoreImpl parser = new MetaStoreImpl(null, null);
        try {
            info = parser.parseManagedCursorInfo(bytes);
        } finally {
            parser.close();
        }
        long ledgerId = info.getCursorsLedgerId();
        if (ledgerId < 0) {
            return new CursorImage(path, bytes, ledgerId, null, null, Map.of(), false, List.of());
        }
        PulsarMockLedgerHandle ledger = pulsarTestContext.getMockBookKeeper().getLedgerMap().get(ledgerId);
        assertThat(ledger).as("actual durable cursor ledger").isNotNull();
        List<byte[]> entries = new ArrayList<>();
        long confirmed = ledger.getLastAddConfirmed();
        if (confirmed >= 0) {
            Enumeration<LedgerEntry> stored = ledger.readEntries(0, confirmed);
            while (stored.hasMoreElements()) {
                entries.add(stored.nextElement().getEntry());
            }
        }
        assertThat(ledger.getLastAddConfirmed()).as("cursor ledger stayed stable during capture").isEqualTo(confirmed);
        assertThat(mockZooKeeper.getData(path, false, null)).as("cursor metadata stayed stable during capture")
                .containsExactly(bytes);
        var metadata = ledger.getLedgerMetadata();
        return new CursorImage(path, bytes, ledgerId,
                BookKeeper.DigestType.fromApiDigestType(metadata.getDigestType()), metadata.getPassword(),
                metadata.getCustomMetadata(), metadata.isClosed(), entries);
    }

    private void restoreCursorImage(CursorImage image) throws Exception {
        PulsarMockBookKeeper bookkeeper = pulsarTestContext.getMockBookKeeper();
        ManagedCursorInfo afterClose;
        MetaStoreImpl parser = new MetaStoreImpl(null, null);
        try {
            afterClose = parser.parseManagedCursorInfo(mockZooKeeper.getData(image.metadataPath(), false, null));
        } finally {
            parser.close();
        }
        if (image.ledgerId() >= 0) {
            if (afterClose.getCursorsLedgerId() != image.ledgerId()) {
                // Graceful cursor close completes before its asynchronous old-ledger deletion.
                Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                        assertThat(bookkeeper.getLedgerMap()).doesNotContainKey(image.ledgerId()));
            }
            PulsarMockLedgerHandle restored = new PulsarMockLedgerHandle(bookkeeper, image.ledgerId(),
                    image.digest(), image.password(), image.customMetadata());
            for (byte[] entry : image.entries()) {
                restored.addEntry(entry);
            }
            if (image.closed()) {
                restored.close();
            }
            bookkeeper.getLedgerMap().put(image.ledgerId(), restored);
        }
        mockZooKeeper.setData(image.metadataPath(), image.metadataBytes(), -1);
        assertThat(mockZooKeeper.getData(image.metadataPath(), false, null)).containsExactly(image.metadataBytes());
    }

    private MessageIdAdv physical(MessageId messageId) throws Exception {
        // The pinned V5 encoding embeds the V4 identity after its segment id and length.
        ByteBuffer buffer = ByteBuffer.wrap(messageId.toByteArray());
        buffer.getLong();
        int length = buffer.getInt();
        assertThat(length).isPositive().isLessThanOrEqualTo(buffer.remaining());
        byte[] encoded = new byte[length];
        buffer.get(encoded);
        // V4 and V5 MessageId share a simple name, so only this V4 reference is qualified.
        return (MessageIdAdv) org.apache.pulsar.client.api.MessageId.fromByteArray(encoded);
    }

    private PulsarClient newClient() throws Exception {
        return PulsarClient.builder().serviceUrl(pulsar.getBrokerServiceUrl())
                .transactionPolicy(TransactionPolicy.builder().timeout(Duration.ofMinutes(5)).build()).build();
    }

    private void restartAndReconnect() throws Exception {
        client.close();
        client = null;
        restartBroker();
        client = newClient();
    }

    private String newTopic() throws Exception {
        String topic = "topic://" + namespace + "/" + UUID.randomUUID();
        admin.scalableTopics().createScalableTopic(topic, 1);
        return topic;
    }

    private String onlySegment(String topic) throws Exception {
        var metadata = admin.scalableTopics().getMetadata(topic);
        assertThat(metadata.getSegments()).hasSize(1);
        var info = metadata.getSegments().values().iterator().next();
        return SegmentTopicName.fromParent(TopicName.get(topic),
                new HashRange(info.getHashRange().getStart(), info.getHashRange().getEnd()), info.getSegmentId())
                .toString();
    }

    private Producer<String> producer(String topic) throws Exception {
        return client.newProducer(Schema.string()).topic(topic).batchingPolicy(BatchingPolicy.ofDisabled()).create();
    }

    private QueueConsumer<String> subscribe(String topic, String subscription) throws Exception {
        return client.newQueueConsumer(Schema.string()).topic(topic).subscriptionName(subscription)
                .subscriptionInitialPosition(SubscriptionInitialPosition.EARLIEST).subscribe();
    }

    private TxnMetadataStore transactionStore() {
        return new TxnMetadataStore(pulsar.getLocalMetadataStore());
    }

    private TxnID onlyOpenTransaction() throws Exception {
        ConcurrentLinkedQueue<TxnID> open = new ConcurrentLinkedQueue<>();
        transactionStore().listOpenByDeadlineRange(null, null, new ScanConsumer() {
            @Override
            public void onNext(GetResult record) {
                TxnHeader header = TxnMetadataStore.fromJson(record.getValue(), TxnHeader.class);
                if (header.getState() == TxnState.OPEN) {
                    open.add(TxnIds.fromKey(TxnPaths.txnIdFromHeaderPath(record.getStat().getPath())));
                }
            }

            @Override
            public void onError(Throwable error) {
                // The returned scan future propagates the failure.
            }

            @Override
            public void onCompleted() {
            }
        }).get(10, TimeUnit.SECONDS);
        assertThat(open).hasSize(1);
        return open.element();
    }

    private List<TxnOp> ackOps(String segment, String subscription) throws Exception {
        ConcurrentLinkedQueue<TxnOp> records = new ConcurrentLinkedQueue<>();
        transactionStore().listAcksBySegmentSubscription(segment, subscription, new ScanConsumer() {
            @Override
            public void onNext(GetResult record) {
                records.add(TxnMetadataStore.fromJson(record.getValue(), TxnOp.class));
            }

            @Override
            public void onError(Throwable error) {
                // The returned scan future propagates the failure.
            }

            @Override
            public void onCompleted() {
            }
        }).get(10, TimeUnit.SECONDS);
        return new ArrayList<>(records);
    }
}
