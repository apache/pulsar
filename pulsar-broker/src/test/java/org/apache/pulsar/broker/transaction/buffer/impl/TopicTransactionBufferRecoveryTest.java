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
package org.apache.pulsar.broker.transaction.buffer.impl;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.PositionFactory;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.buffer.AbortedTxnProcessor;
import org.apache.pulsar.broker.transaction.buffer.TransactionBufferProvider;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests for {@link TopicTransactionBuffer} recovery completion behavior.
 */
@Test(groups = "broker")
public class TopicTransactionBufferRecoveryTest extends ProducerConsumerBase {

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        conf.setTransactionCoordinatorEnabled(true);
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "snapshotExists")
    public Object[][] snapshotExists() {
        return new Object[][] { { false }, { true } };
    }

    /**
     * While the transaction buffer is recovering, normal publishes don't move the max read position, so they
     * don't update the topic's lastMaxReadPositionMovedForwardTimestamp either. When recovery completes, the
     * transaction buffer must account for the messages published during recovery and trigger the
     * maxReadPositionMovedForward callback; otherwise ReplicatedSubscriptionsController would consider the topic
     * to have no new data and never start a subscription snapshot until further traffic arrives.
     */
    @Test(dataProvider = "snapshotExists")
    public void testMaxReadPositionMovedForwardForMessagesPublishedDuringRecovery(boolean snapshotExists)
            throws Exception {
        String tpName = BrokerTestUtil.newUniqueName("persistent://public/default/tp-tb-recovery");
        CompletableFuture<Position> recoverFuture = new CompletableFuture<>();
        TransactionBufferProvider originalProvider = pulsar.getTransactionBufferProvider();
        pulsar.setTransactionBufferProvider(originTopic -> {
            AbortedTxnProcessor processor = mock(AbortedTxnProcessor.class);
            when(processor.recoverFromSnapshot()).thenReturn(recoverFuture);
            when(processor.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
            return new TopicTransactionBuffer(
                    (PersistentTopic) originTopic, processor, AbortedTxnProcessor.SnapshotType.Single);
        });
        try {
            @Cleanup
            Producer<byte[]> producer = pulsarClient.newProducer().topic(tpName).create();
            // The transaction buffer is stuck in the recovering state until recoverFuture completes, so these
            // messages are published while it is still recovering.
            for (int i = 0; i < 3; i++) {
                producer.send(("msg-" + i).getBytes(StandardCharsets.UTF_8));
            }
            PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                    .getTopicIfExists(tpName).get().orElseThrow();
            assertEquals(persistentTopic.getLastMaxReadPositionMovedForwardTimestamp(), 0L,
                    "Publishes during recovery are not expected to move the max read position forward");

            // Let the recovery finish, either replaying from a snapshot position or without any snapshot.
            recoverFuture.complete(snapshotExists ? PositionFactory.EARLIEST : null);

            Awaitility.await().untilAsserted(() -> {
                assertTrue(persistentTopic.getLastMaxReadPositionMovedForwardTimestamp() > 0,
                        "Completed recovery should move the max read position forward for the messages"
                                + " published during recovery");
                assertEquals(persistentTopic.getTransactionBuffer().getMaxReadPosition(),
                        persistentTopic.getManagedLedger().getLastConfirmedEntry());
            });
        } finally {
            pulsar.setTransactionBufferProvider(originalProvider);
        }
    }

    /**
     * When nothing is published while the transaction buffer recovers, completing the recovery must not trigger
     * the maxReadPositionMovedForward callback, so an idle topic isn't mistaken for one with new data.
     */
    @Test
    public void testMaxReadPositionNotMovedForwardWhenNothingPublishedDuringRecovery() throws Exception {
        String tpName = BrokerTestUtil.newUniqueName("persistent://public/default/tp-tb-recovery-idle");
        CompletableFuture<Position> recoverFuture = new CompletableFuture<>();
        TransactionBufferProvider originalProvider = pulsar.getTransactionBufferProvider();
        pulsar.setTransactionBufferProvider(originTopic -> {
            AbortedTxnProcessor processor = mock(AbortedTxnProcessor.class);
            when(processor.recoverFromSnapshot()).thenReturn(recoverFuture);
            when(processor.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
            return new TopicTransactionBuffer(
                    (PersistentTopic) originTopic, processor, AbortedTxnProcessor.SnapshotType.Single);
        });
        try {
            PersistentTopic persistentTopic = (PersistentTopic) pulsar.getBrokerService()
                    .getTopic(tpName, true).get().orElseThrow();
            recoverFuture.complete(null);
            Awaitility.await().untilAsserted(() ->
                    assertEquals(persistentTopic.getTransactionBuffer().getMaxReadPosition(),
                            persistentTopic.getManagedLedger().getLastConfirmedEntry()));
            assertEquals(persistentTopic.getLastMaxReadPositionMovedForwardTimestamp(), 0L,
                    "Recovery of an idle topic should not move the max read position forward");
        } finally {
            pulsar.setTransactionBufferProvider(originalProvider);
        }
    }
}
