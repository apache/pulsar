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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.compaction.Compactor;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class CompactionConcurrencyTest extends SharedPulsarBaseTest {

    private void triggerCompactionAndWait(String topicName) throws Exception {
        PersistentTopic persistentTopic =
                (PersistentTopic) getTopic(topicName, false).get().get();
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
        String topicName = newTopicName();
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topicPolicies().setCompactionThreshold(topicName, 1);
        admin.topics().createSubscription(topicName, "s1", MessageId.earliest);
        var producer = pulsarClient.newProducer(Schema.STRING).topic(topicName).enableBatching(false).create();
        producer.newMessage().key("k0").value("v0").send();
        triggerCompactionAndWait(topicName);
        admin.topics().deleteSubscription(topicName, "s1");

        // Disable compaction.
        admin.topicPolicies().removeCompactionThreshold(topicName);
        AtomicReference<CompletableFuture<Void>> f1 = new AtomicReference<CompletableFuture<Void>>();
        AtomicReference<CompletableFuture<Void>> f2 = new AtomicReference<CompletableFuture<Void>>();
        new Thread(() -> {
            f1.set(admin.topics().deleteSubscriptionAsync(topicName, "__compaction"));
        }).start();
        new Thread(() -> {
            f2.set(admin.topics().deleteSubscriptionAsync(topicName, "__compaction"));
        }).start();

        // Verify: at least one of the requests should fail (the other may succeed or also fail
        // with "not found" if the in-memory metadata store processes them sequentially).
        Awaitility.await().untilAsserted(() -> {
            assertTrue(f1.get() != null);
            assertTrue(f2.get() != null);
            assertTrue(f1.get().isDone());
            assertTrue(f2.get().isDone());
            assertTrue(f1.get().isCompletedExceptionally() || f2.get().isCompletedExceptionally());
        });

        // cleanup.
        producer.close();
    }
}
