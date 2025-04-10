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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.compaction.Compactor;
import org.apache.zookeeper.MockZooKeeper;
import org.awaitility.Awaitility;
import org.awaitility.reflect.WhiteboxImpl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class CompactionConcurrencyTest extends ProducerConsumerBase {
    // don't make this over 2000ms, otherwise the test will be flaky due to ZKSessionWatcher
    static final int DELETE_OPERATION_DELAY_MS = 1900;

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        // Disable the scheduled task: compaction.
        conf.setBrokerServiceCompactionMonitorIntervalInSeconds(Integer.MAX_VALUE);
        // Disable the scheduled task: retention.
        conf.setRetentionCheckIntervalInSeconds(Integer.MAX_VALUE);
    }

    private void triggerCompactionAndWait(String topicName) throws Exception {
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topicName, false).get().get();
        persistentTopic.triggerCompaction();
        Awaitility.await().untilAsserted(() -> {
            Position lastConfirmPos = persistentTopic.getManagedLedger().getLastConfirmedEntry();
            Position markDeletePos = persistentTopic
                    .getSubscription(Compactor.COMPACTION_SUBSCRIPTION).getCursor().getMarkDeletedPosition();
            assertEquals(markDeletePos.getLedgerId(), lastConfirmPos.getLedgerId());
            assertEquals(markDeletePos.getEntryId(), lastConfirmPos.getEntryId());
        });
    }

    @Test
    public void testDisableCompactionConcurrently() throws Exception {
        String topicName = "persistent://public/default/" + BrokerTestUtil.newUniqueName("tp");
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topicPolicies().setCompactionThreshold(topicName, 1);
        admin.topics().createSubscription(topicName, "s1", MessageId.earliest);
        var producer = pulsarClient.newProducer(Schema.STRING).topic(topicName).enableBatching(false).create();
        producer.newMessage().key("k0").value("v0").send();
        triggerCompactionAndWait(topicName);
        admin.topics().deleteSubscription(topicName, "s1");
        PersistentTopic persistentTopic =
                (PersistentTopic) pulsar.getBrokerService().getTopic(topicName, false).get().get();
        AtomicBoolean disablingCompaction = persistentTopic.disablingCompaction;

        // Disable compaction.
        // Inject a delay when the first time of deleting cursor.
        AtomicInteger times = new AtomicInteger();
        String cursorPath = String.format("/managed-ledgers/%s/__compaction",
                TopicName.get(topicName).getPersistenceNamingEncoding());
        admin.topicPolicies().removeCompactionThreshold(topicName);
        mockZooKeeper.delay(DELETE_OPERATION_DELAY_MS, (op, path) -> {
            return op == MockZooKeeper.Op.DELETE && cursorPath.equals(path) && times.incrementAndGet() == 1;
        });
        mockZooKeeperGlobal.delay(DELETE_OPERATION_DELAY_MS, (op, path) -> {
            return op == MockZooKeeper.Op.DELETE && cursorPath.equals(path) && times.incrementAndGet() == 1;
        });
        AtomicReference<CompletableFuture<Void>> f1 = new AtomicReference<CompletableFuture<Void>>();
        AtomicReference<CompletableFuture<Void>> f2 = new AtomicReference<CompletableFuture<Void>>();
        new Thread(() -> {
            f1.set(admin.topics().deleteSubscriptionAsync(topicName, "__compaction"));
        }).start();
        new Thread(() -> {
            f2.set(admin.topics().deleteSubscriptionAsync(topicName, "__compaction"));
        }).start();

        // Verify: the next compaction will be skipped.
        Awaitility.await().untilAsserted(() -> {
            assertTrue(disablingCompaction.get());
        });
        producer.newMessage().key("k1").value("v1").send();
        producer.newMessage().key("k2").value("v2").send();
        CompletableFuture<Long> currentCompaction1 = persistentTopic.currentCompaction;
                WhiteboxImpl.getInternalState(persistentTopic, "currentCompaction");
        persistentTopic.triggerCompaction();
        CompletableFuture<Long> currentCompaction2 = persistentTopic.currentCompaction;
        assertTrue(currentCompaction1 == currentCompaction2);

        // Verify: one of the requests should fail.
        Awaitility.await().untilAsserted(() -> {
            assertTrue(f1.get() != null);
            assertTrue(f2.get() != null);
            assertTrue(f1.get().isDone());
            assertTrue(f2.get().isDone());
            assertTrue(f1.get().isCompletedExceptionally() || f2.get().isCompletedExceptionally());
            assertTrue(!f1.get().isCompletedExceptionally() || !f2.get().isCompletedExceptionally());
        });
        try {
            f1.get().join();
            f2.get().join();
            fail("Should fail");
        } catch (Exception ex) {
            Throwable actEx = FutureUtil.unwrapCompletionException(ex);
            assertTrue(actEx instanceof PulsarAdminException.PreconditionFailedException);
        }

        // cleanup.
        producer.close();
        admin.topics().delete(topicName, false);
    }
}
