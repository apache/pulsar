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
package org.apache.pulsar.broker.service.persistent;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.pulsar.common.protocol.Commands.serializeMetadataAndPayload;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.ImmediateEventExecutor;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.test.MockedBookKeeperTestCase;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.EntryBatchIndexesAcks;
import org.apache.pulsar.broker.service.EntryBatchSizes;
import org.apache.pulsar.broker.service.TransportCnx;
import org.apache.pulsar.broker.service.persistent.PersistentDispatcherMultipleConsumers.ReadType;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.policies.data.stats.ConsumerStatsImpl;
import org.apache.pulsar.common.protocol.Commands;
import org.awaitility.Awaitility;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Regression tests for apache/pulsar#26454: a Shared subscription stalls forever when a read failure
 * callback is interrupted before it reaches the {@code havePendingRead} reset. The read operation is
 * already gone from the managed cursor, so nothing would ever clear the flag: {@code readMoreEntries}
 * refuses to run while it is set and {@code checkAndUnblockIfStuck} requires {@code !havePendingRead},
 * leaving a topic unload as the only recovery.
 */
public class PersistentDispatcherMultipleConsumersStuckReadTest extends MockedBookKeeperTestCase {

    private static final String TOPIC = "persistent://prop/ns/shared-stuck-read";
    private static final long TIMEOUT_SECONDS = 10;

    private PulsarTestContext pulsarTestContext;
    private ManagedLedgerImpl ledger;
    private ManagedCursorImpl cursor;
    private PersistentTopic topic;
    private PersistentSubscription subscription;
    private PersistentDispatcherMultipleConsumers dispatcher;
    private Consumer consumer;

    private final List<Delivery> deliveries = Collections.synchronizedList(new ArrayList<>());
    private final AtomicLong msgCounter = new AtomicLong();

    @Override
    protected ManagedLedgerConfig initManagedLedgerConfig(ManagedLedgerConfig config) {
        super.initManagedLedgerConfig(config);
        // Inline checkForNewEntries for determinism.
        config.setNewEntriesCheckDelayInMillis(0);
        config.setMaxEntriesPerLedger(1_000_000);
        config.setRetentionTime(1, TimeUnit.HOURS);
        config.setRetentionSizeInMB(-1);
        return config;
    }

    private void buildFixture(String ledgerName) throws Exception {
        ServiceConfiguration svcConfig = new ServiceConfiguration();
        svcConfig.setBrokerShutdownTimeoutMs(0L);
        svcConfig.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
        svcConfig.setClusterName("test");
        svcConfig.setSystemTopicEnabled(false);
        svcConfig.setTopicLevelPoliciesEnabled(false);
        // Default is 15s; keep the read-failure retry backoff tiny.
        svcConfig.setDispatcherReadFailureBackoffInitialTimeInMs(1);

        pulsarTestContext = PulsarTestContext.builderForNonStartableContext()
                .config(svcConfig)
                .managedLedgerClients(bkc, factory)
                .build();

        ledger = (ManagedLedgerImpl) factory.open(ledgerName, initManagedLedgerConfig(new ManagedLedgerConfig()));
        topic = new PersistentTopic(TOPIC, ledger, pulsarTestContext.getBrokerService());
        // Open the cursor after topic construction so the topic does not auto-create a second
        // subscription sharing this cursor.
        cursor = (ManagedCursorImpl) ledger.openCursor("sub");
        // The dispatcher field is assigned after the subscription: the override below is only
        // evaluated when the first consumer is added, so it hands the spied dispatcher to the
        // real subscription lifecycle (see the seam's javadoc on PersistentSubscription).
        subscription = new PersistentSubscription(topic, "sub", cursor, true) {
            @Override
            protected Dispatcher reuseOrCreateDispatcher(Dispatcher existingDispatcher, Consumer newConsumer) {
                return PersistentDispatcherMultipleConsumersStuckReadTest.this.dispatcher;
            }
        };

        dispatcher = spy(new PersistentDispatcherMultipleConsumers(topic, cursor, subscription));

        deliveries.clear();
        consumer = newMockConsumer("c1");
    }

    /** A mock consumer that records deliveries, releases the entries and completes immediately. */
    private Consumer newMockConsumer(String consumerName) {
        Consumer mockConsumer = mock(Consumer.class);
        TransportCnx cnx = mock(TransportCnx.class);
        doReturn(true).when(cnx).isActive();
        doReturn(cnx).when(mockConsumer).cnx();
        doReturn(1000).when(mockConsumer).getAvailablePermits();
        doReturn(1).when(mockConsumer).getAvgMessagesPerEntry();
        doReturn(true).when(mockConsumer).isWritable();
        doReturn(false).when(mockConsumer).readCompacted();
        doReturn(false).when(mockConsumer).isPreciseDispatcherFlowControl();
        doReturn(false).when(mockConsumer).isBlocked();
        doReturn(consumerName).when(mockConsumer).consumerName();
        doReturn(0).when(mockConsumer).getPriorityLevel();
        doReturn(0L).when(mockConsumer).getConsumerEpoch();
        doReturn(SubType.Shared).when(mockConsumer).subType();
        doReturn(new ConsumerStatsImpl()).when(mockConsumer).getStats();
        doAnswer(inv -> {
            List<Entry> entries = inv.getArgument(0);
            for (Entry entry : entries) {
                deliveries.add(Delivery.of(consumerName, entry.getPosition()));
                entry.release();
            }
            EntryBatchSizes batchSizes = inv.getArgument(1);
            if (batchSizes != null) {
                batchSizes.recyle();
            }
            EntryBatchIndexesAcks batchIndexesAcks = inv.getArgument(2);
            if (batchIndexesAcks != null) {
                batchIndexesAcks.recycle();
            }
            return ImmediateEventExecutor.INSTANCE.newSucceededFuture(null);
        }).when(mockConsumer).sendMessages(any(), any(), any(), anyInt(), anyLong(), anyLong(), any());
        return mockConsumer;
    }

    @AfterMethod(alwaysRun = true)
    public void afterMethod() {
        try {
            if (cursor != null && !cursor.isClosed()) {
                cursor.close();
            }
            if (ledger != null) {
                ledger.close();
            }
            if (pulsarTestContext != null) {
                pulsarTestContext.close();
            }
        } catch (Exception ignore) {
            // best effort
        }
        cursor = null;
        ledger = null;
        pulsarTestContext = null;
    }

    /**
     * A read failure callback that is interrupted midway must still leave {@code havePendingRead}
     * cleared, since no other completion will ever arrive for that read.
     */
    @Test
    public void testReadFailureClearsPendingReadFlagEvenWhenCallbackThrows() throws Exception {
        buildFixture("shared-stuck-read-callback-throws");
        subscription.addConsumer(consumer).get();

        dispatcher.havePendingRead = true;
        doThrow(new RuntimeException("callback interrupted")).when(dispatcher).onReadEnded(any());

        Assert.assertThrows(RuntimeException.class, () -> dispatcher.readEntriesFailed(
                new ManagedLedgerException("read failed"), ReadType.Normal));
        Assert.assertFalse(dispatcher.havePendingRead);
    }

    /**
     * The flag reset being moved to the top must not change the normal failure handling: the
     * scheduled retry registers the cursor to wait for new entries again and later messages are
     * still delivered.
     */
    @Test
    public void testReadFailureStillWaitsForNewEntriesAndDelivers() throws Exception {
        buildFixture("shared-stuck-read-retry");
        subscription.addConsumer(consumer).get();

        dispatcher.havePendingRead = true;
        dispatcher.readEntriesFailed(new ManagedLedgerException("read failed"), ReadType.Normal);
        Assert.assertFalse(dispatcher.havePendingRead);

        Awaitility.await("cursor should wait for new entries again")
                .atMost(Duration.ofSeconds(TIMEOUT_SECONDS))
                .until(() -> cursor.hasPendingReadRequest() && dispatcher.havePendingRead);

        Position p2 = publish();
        Awaitility.await("entry published after the failure should be delivered")
                .atMost(Duration.ofSeconds(TIMEOUT_SECONDS))
                .until(() -> deliveries.contains(Delivery.of("c1", p2)));
    }

    /**
     * End-to-end reproduction of the #26454 symptom. The failure callback is interrupted before the
     * flag reset and the throw also prevents the scheduled retry, so the built-in
     * {@code checkAndUnblockIfStuck} detector is the only possible recovery: it is blind to a stuck
     * {@code havePendingRead} before the fix and unblocks the dispatcher after it.
     */
    @Test
    public void testInterruptedReadFailureIsRecoveredByUnblockStuckSubscription() throws Exception {
        buildFixture("shared-stuck-read-e2e");
        subscription.addConsumer(consumer).get();

        // Two entries so the read position stays behind the last confirmed entry.
        Position p1 = publish();
        Position p2 = publish();

        dispatcher.havePendingRead = true;

        // First detector invocation establishes the read-position baseline and must not unblock.
        Assert.assertFalse(subscription.checkAndUnblockIfStuck());

        doThrow(new RuntimeException("callback interrupted")).doNothing()
                .when(dispatcher).onReadEnded(any());
        Assert.assertThrows(RuntimeException.class, () -> dispatcher.readEntriesFailed(
                new ManagedLedgerException("read failed"), ReadType.Normal));

        Assert.assertTrue(subscription.checkAndUnblockIfStuck(),
                "detector should unblock the stuck dispatcher");

        Awaitility.await("backlog should be delivered after unblocking")
                .atMost(Duration.ofSeconds(TIMEOUT_SECONDS))
                .until(() -> deliveries.contains(Delivery.of("c1", p1))
                        && deliveries.contains(Delivery.of("c1", p2)));
    }

    /**
     * An exception thrown while scheduling the read - after {@code havePendingRead} is set but
     * before the read is issued to the managed cursor - used to leave the flag set forever with
     * nothing able to clear it. The failure must be converted into a flag reset plus a scheduled
     * retry, after which the backlog is delivered.
     */
    @Test
    public void testReadSchedulingFailureIsRetriedAndDelivers() throws Exception {
        buildFixture("shared-stuck-read-schedule-failure");
        subscription.addConsumer(consumer).get();
        Position p1 = publish();

        // Fail the first read scheduling attempt (skip-condition construction), then behave normally.
        doThrow(new RuntimeException("read scheduling failed")).doCallRealMethod()
                .when(dispatcher).createReadEntriesSkipConditionForNormalRead();

        dispatcher.readMoreEntriesAsync();

        Awaitility.await("entry should be delivered after the scheduling failure is retried")
                .atMost(Duration.ofSeconds(TIMEOUT_SECONDS))
                .until(() -> deliveries.contains(Delivery.of("c1", p1)));
    }

    private Position publish() throws Exception {
        long sequenceId = msgCounter.incrementAndGet();
        MessageMetadata messageMetadata = new MessageMetadata()
                .setSequenceId(sequenceId)
                .setProducerName("testProducer")
                .setPartitionKeyB64Encoded(false)
                .setPublishTime(System.currentTimeMillis());
        ByteBuf message = serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, messageMetadata,
                Unpooled.copiedBuffer(("m" + sequenceId).getBytes(UTF_8)));
        byte[] bytes = new byte[message.readableBytes()];
        message.readBytes(bytes);
        message.release();
        return ledger.addEntry(bytes);
    }

    private record Delivery(String consumerName, long ledgerId, long entryId) {

        static Delivery of(String consumerName, Position position) {
            return new Delivery(consumerName, position.getLedgerId(), position.getEntryId());
        }

        @Override
        public String toString() {
            return consumerName + "@" + ledgerId + ":" + entryId;
        }
    }
}
