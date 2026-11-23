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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.MultiBrokerBaseTest;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.BrokerServiceException.TopicFencedException;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.testinterceptor.BrokerTestInterceptor;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.assertj.core.api.SoftAssertions;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * A topic delete that waits for the topic's close on a broker must not delete the topic once that broker has given
 * up the topic's namespace bundle and another broker serves the topic.
 */
@CustomLog
@Test(groups = "broker")
public class BrokerServiceDeleteAfterUnloadTest extends MultiBrokerBaseTest {

    private static final long UNLOAD_TIMEOUT_MS = 2000;

    @Override
    protected int numberOfAdditionalBrokers() {
        return 1;
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        configureBroker(conf);
    }

    @Override
    protected ServiceConfiguration createConfForAdditionalBroker(int additionalBrokerIndex) {
        ServiceConfiguration configuration = super.createConfForAdditionalBroker(additionalBrokerIndex);
        configureBroker(configuration);
        return configuration;
    }

    private static void configureBroker(ServiceConfiguration configuration) {
        BrokerTestInterceptor.INSTANCE.configure(configuration);
        // A topic close that takes longer than this lets the bundle unload go on and release the bundle.
        configuration.setNamespaceBundleUnloadingTimeoutMs(UNLOAD_TIMEOUT_MS);
        // Only the test moves the bundle.
        configuration.setLoadBalancerSheddingEnabled(false);
    }

    @Test(timeOut = 120_000)
    public void testDeleteOnFormerOwnerAfterBundleUnload() throws Exception {
        String topic = "persistent://public/default/delete-after-unload-" + UUID.randomUUID();
        TopicName topicName = TopicName.get(topic);
        String managedLedgerName = topicName.getPersistenceNamingEncoding();
        String schemaPath = "/schemas/" + topicName.getSchemaName();
        AtomicBoolean holdNextClose = new AtomicBoolean();
        CompletableFuture<Void> closeHeld = new CompletableFuture<>();
        CompletableFuture<Void> releaseClose = new CompletableFuture<>();
        // The managed ledger closes at once, but its close callback, which completes the topic close, waits for the
        // test: a slow ledger close as the topic sees it.
        BrokerTestInterceptor.INSTANCE.setManagedLedgerDecorator(created -> {
            if (!created.getName().equals(managedLedgerName)) {
                return created;
            }
            ManagedLedgerImpl decorated = BrokerTestUtil.spyWithoutRecordingInvocations(created);
            doAnswer(invocation -> {
                if (!holdNextClose.compareAndSet(true, false)) {
                    return invocation.callRealMethod();
                }
                CloseCallback callback = invocation.getArgument(0);
                decorated.asyncClose(new CloseCallback() {
                    @Override
                    public void closeComplete(Object ctx) {
                        releaseClose.thenRun(() -> callback.closeComplete(ctx));
                    }

                    @Override
                    public void closeFailed(ManagedLedgerException exception, Object ctx) {
                        releaseClose.thenRun(() -> callback.closeFailed(exception, ctx));
                    }
                }, invocation.getArgument(1));
                closeHeld.complete(null);
                return null;
            }).when(decorated).asyncClose(any(), any());
            return decorated;
        });
        Producer<String> producer = null;
        try {
            producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create();
            producer.send("before unload");

            PulsarService formerOwner = getAllBrokers().stream()
                    .filter(broker -> broker.getBrokerService().getTopicReference(topic).isPresent())
                    .findFirst().orElseThrow();
            PulsarService newOwner = getAllBrokers().stream()
                    .filter(broker -> broker != formerOwner).findFirst().orElseThrow();
            PulsarService leader = getAllBrokers().stream()
                    .filter(broker -> broker.getLeaderElectionService().isLeader()).findFirst().orElseThrow();
            PersistentTopic formerTopic =
                    (PersistentTopic) formerOwner.getBrokerService().getTopicReference(topic).orElseThrow();
            NamespaceBundle bundle = formerOwner.getNamespaceService().getBundle(topicName);
            log.info()
                    .attr("formerOwner", formerOwner.getBrokerId())
                    .attr("newOwner", newOwner.getBrokerId())
                    .attr("leader", leader.getBrokerId())
                    .attr("bundle", bundle)
                    .log("Delete-after-unload test brokers");

            // What an unload to a destination broker does on the leader: the next assignment goes to the new owner.
            leader.getLoadManager().get().setNamespaceBundleAffinity(bundle.toString(), newOwner.getBrokerId());
            holdNextClose.set(true);
            CompletableFuture<Void> unload = formerOwner.getNamespaceService().unloadNamespaceBundle(bundle);
            closeHeld.get(10, TimeUnit.SECONDS);
            assertThat(formerTopic.isFenced()).as("the unload fenced the topic to close it").isTrue();

            CompletableFuture<Void> deletion = formerOwner.getBrokerService().deleteTopic(topic, false);
            assertThat(deletion).as("the delete waits for the close in progress").isNotDone();

            // The close outlasts the unload timeout, so the unload drops the fenced topic and releases the bundle.
            unload.get(30, TimeUnit.SECONDS);
            assertThat(formerOwner.getNamespaceService().getOwnershipCache().getOwnedBundle(bundle))
                    .as("the former owner released the bundle").isNull();
            Producer<String> reconnected = producer;
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                assertThat(newOwner.getBrokerService().getTopicReference(topic))
                        .as("the new owner loaded the topic").isPresent();
                assertThat(newOwner.getBrokerService().getTopicReference(topic).orElseThrow().getProducers())
                        .as("the producer reconnected to the new owner").hasSize(1);
                assertThat(reconnected.isConnected()).isTrue();
            });
            producer.sendAsync("on the new owner").get(10, TimeUnit.SECONDS);
            PersistentTopic newTopic =
                    (PersistentTopic) newOwner.getBrokerService().getTopicReference(topic).orElseThrow();
            long newOwnerLedgerId = ((ManagedLedgerImpl) newTopic.getManagedLedger()).getLedgersInfo().lastKey();
            assertThat(deletion).as("the delete still waits for the close").isNotDone();
            assertThat(formerTopic.getCloseFuture()).as("the close is still held").isNotDone();
            assertThat(newOwner.getPulsarResources().getTopicResources().persistentTopicExists(topicName)
                    .get(10, TimeUnit.SECONDS)).as("the managed ledger exists before the close finishes").isTrue();
            assertThat(newOwner.getLocalMetadataStore().exists(schemaPath).get(10, TimeUnit.SECONDS))
                    .as("the schema exists before the close finishes").isTrue();
            assertThat(mockBookKeeper.getLedgers()).as("the new owner's ledger exists before the close finishes")
                    .contains(newOwnerLedgerId);

            // The slow close on the former owner finishes now.
            releaseClose.complete(null);
            Awaitility.await().atMost(Duration.ofSeconds(30)).until(deletion::isDone);
            Throwable deletionError = deletion.handle((ignored, error) -> error).join();
            boolean managedLedgerExists = newOwner.getPulsarResources().getTopicResources()
                    .persistentTopicExists(topicName).get(10, TimeUnit.SECONDS);
            boolean schemaExists = newOwner.getLocalMetadataStore().exists(schemaPath).get(10, TimeUnit.SECONDS);
            boolean newOwnerLedgerExists = mockBookKeeper.getLedgers().contains(newOwnerLedgerId);
            Throwable sendError = producer.sendAsync("after the delete on the former owner")
                    .handle((ignored, error) -> error).get(30, TimeUnit.SECONDS);
            log.info()
                    .attr("deletionError", deletionError)
                    .attr("managedLedgerExists", managedLedgerExists)
                    .attr("schemaExists", schemaExists)
                    .attr("newOwnerLedgerId", newOwnerLedgerId)
                    .attr("newOwnerLedgerExists", newOwnerLedgerExists)
                    .attr("sendError", sendError)
                    .log("State after the former owner's delete finished");

            SoftAssertions softly = new SoftAssertions();
            softly.assertThat(FutureUtil.unwrapCompletionException(deletionError))
                    .as("the delete on %s, which no longer owns the bundle, must fail", formerOwner.getBrokerId())
                    .isInstanceOf(TopicFencedException.class);
            softly.assertThat(managedLedgerExists)
                    .as("the managed ledger of the topic that %s serves must still exist", newOwner.getBrokerId())
                    .isTrue();
            softly.assertThat(schemaExists)
                    .as("the schema of the topic that %s serves must still exist", newOwner.getBrokerId())
                    .isTrue();
            softly.assertThat(newOwnerLedgerExists)
                    .as("the ledger %s that %s writes to must still exist", newOwnerLedgerId, newOwner.getBrokerId())
                    .isTrue();
            softly.assertThat(sendError)
                    .as("a send to the topic on %s must still succeed", newOwner.getBrokerId())
                    .isNull();
            softly.assertAll();
        } finally {
            releaseClose.complete(null);
            BrokerTestInterceptor.INSTANCE.reset();
            if (producer != null) {
                producer.closeAsync();
            }
        }
    }
}
