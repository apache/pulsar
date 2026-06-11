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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.util.FutureUtil;
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
        PersistentTopic persistentTopic =
                (PersistentTopic) getTopic(topicName, false).get().get();

        // Disable compaction.
        admin.topicPolicies().removeCompactionThreshold(topicName);
        CompletableFuture<Long> originalCompaction = persistentTopic.currentCompaction;
        CompletableFuture<Long> blockedCompaction = new CompletableFuture<>();
        persistentTopic.currentCompaction = blockedCompaction;
        try {
            CompletableFuture<Void> firstDelete =
                    admin.topics().deleteSubscriptionAsync(topicName, Compactor.COMPACTION_SUBSCRIPTION);
            Awaitility.await().untilAsserted(() -> assertTrue(persistentTopic.disablingCompaction.get()));

            CompletableFuture<Void> secondDelete =
                    admin.topics().deleteSubscriptionAsync(topicName, Compactor.COMPACTION_SUBSCRIPTION);
            Awaitility.await().untilAsserted(() -> assertTrue(secondDelete.isCompletedExceptionally()));
            try {
                secondDelete.join();
                fail("The second concurrent compaction subscription delete should fail");
            } catch (Exception ex) {
                Throwable actEx = FutureUtil.unwrapCompletionException(ex);
                assertTrue(actEx instanceof PulsarAdminException.PreconditionFailedException);
            }

            blockedCompaction.complete(0L);
            Awaitility.await().untilAsserted(() -> {
                assertTrue(firstDelete.isDone());
                assertFalse(firstDelete.isCompletedExceptionally());
                assertFalse(persistentTopic.disablingCompaction.get());
            });
            firstDelete.join();
        } finally {
            blockedCompaction.complete(0L);
            persistentTopic.currentCompaction = originalCompaction;
        }

        // cleanup.
        producer.close();
    }
}
