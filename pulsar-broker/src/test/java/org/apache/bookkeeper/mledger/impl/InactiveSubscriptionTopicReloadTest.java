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
package org.apache.bookkeeper.mledger.impl;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.impl.MetaStore.MetaStoreCallback;
import org.apache.bookkeeper.mledger.proto.ManagedCursorInfo;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.metadata.api.Stat;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/** Verifies that subscriptions without consumers expire even if their topic is reloaded frequently. */
@Test(groups = "broker-impl")
public class InactiveSubscriptionTopicReloadTest extends SharedPulsarBaseTest {

    @Test(timeOut = 30000)
    public void testInactiveSubscriptionExpiresAcrossTopicReloads() throws Exception {
        String topicName = newTopicName();
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer().topic(topicName)
                .subscriptionName("in-use").subscribe()) {
            pulsarClient.newConsumer().topic(topicName).subscriptionName("idle").subscribe().close();
            // Let the idle subscription be inactive for longer than the expiration time checked below
            Thread.sleep(1500);

            // Reload the topic, as frequent bundle unloads would
            for (int i = 0; i < 2; i++) {
                PersistentTopic topic = (PersistentTopic) getTopicReference(topicName).orElseThrow();
                ManagedLedgerImpl ledger = (ManagedLedgerImpl) topic.getManagedLedger();
                admin.topics().unload(topicName);
                assertThat(storedCursorInfo(ledger, "idle").hasIdleSince()).isTrue();
                // The unload disconnects the consumer of the other subscription, which is not idle for that
                assertThat(storedCursorInfo(ledger, "in-use").hasIdleSince()).isFalse();

                Awaitility.await().untilAsserted(() -> {
                    Optional<Topic> reloaded = getTopicReference(topicName);
                    assertThat(reloaded).isPresent();
                    assertThat(reloaded.get()).isNotSameAs(topic);
                    assertThat(((PersistentTopic) reloaded.get()).getSubscription("in-use").getConsumers())
                            .hasSize(1);
                });
            }

            PersistentTopic topic = (PersistentTopic) getTopicReference(topicName).orElseThrow();
            // The consumer came back to a subscription that was not persisted as idle: there is nothing to persist
            ManagedCursorImpl cursorInUse = (ManagedCursorImpl) topic.getSubscription("in-use").getCursor();
            assertThat(cursorInUse.isDirty()).isFalse();

            topic.checkInactiveSubscriptions(TimeUnit.SECONDS.toMillis(1));
            Awaitility.await().untilAsserted(() ->
                    assertThat(topic.getSubscriptions().keySet()).containsExactly("in-use"));
        }
    }

    private static ManagedCursorInfo storedCursorInfo(ManagedLedgerImpl ledger, String subscriptionName)
            throws Exception {
        CompletableFuture<ManagedCursorInfo> result = new CompletableFuture<>();
        ledger.getStore().asyncGetCursorInfo(ledger.getName(), subscriptionName, new MetaStoreCallback<>() {
            @Override
            public void operationComplete(ManagedCursorInfo info, Stat stat) {
                result.complete(info);
            }

            @Override
            public void operationFailed(MetaStoreException exception) {
                result.completeExceptionally(exception);
            }
        });
        return result.get(5, TimeUnit.SECONDS);
    }
}
