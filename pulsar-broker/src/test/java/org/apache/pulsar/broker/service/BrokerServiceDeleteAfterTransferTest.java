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
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.CustomLog;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.MultiBrokerBaseTest;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.ExtensibleLoadManagerImpl;
import org.apache.pulsar.broker.loadbalance.extensions.scheduler.TransferShedder;
import org.apache.pulsar.broker.service.BrokerServiceException.ServiceUnitNotReadyException;
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
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * A topic delete that waits for the topic's close on a broker must not delete the topic once the extensible load
 * manager has transferred the topic's namespace bundle to another broker that serves the topic.
 */
@CustomLog
@Test(groups = "broker")
public class BrokerServiceDeleteAfterTransferTest extends MultiBrokerBaseTest {

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
        configuration.setLoadManagerClassName(ExtensibleLoadManagerImpl.class.getName());
        configuration.setLoadBalancerLoadSheddingStrategy(TransferShedder.class.getName());
        // A transfer closes a topic twice: first without disconnecting its clients, then, once the destination
        // broker owns the bundle, disconnecting them to that broker.
        configuration.setLoadBalancerMultiPhaseBundleUnload(true);
        configuration.setNamespaceBundleUnloadingTimeoutMs(UNLOAD_TIMEOUT_MS);
        // Only the test moves the bundle.
        configuration.setLoadBalancerSheddingEnabled(false);
        configuration.setLoadBalancerAutoBundleSplitEnabled(false);
    }

    @DataProvider
    public Object[][] heldClose() {
        // 1: the first close of the transfer, released before the unload timeout; 2: the second close.
        return new Object[][] {{1}, {2}};
    }

    @Test(dataProvider = "heldClose", timeOut = 120_000)
    public void testDeleteOnFormerOwnerDuringTransfer(int heldClose) throws Exception {
        String topic = "persistent://public/default/delete-during-transfer-" + UUID.randomUUID();
        TopicName topicName = TopicName.get(topic);
        String managedLedgerName = topicName.getPersistenceNamingEncoding();
        String schemaPath = "/schemas/" + topicName.getSchemaName();
        AtomicInteger closesUntilHold = new AtomicInteger();
        CompletableFuture<Void> closeHeld = new CompletableFuture<>();
        CompletableFuture<Void> releaseClose = new CompletableFuture<>();
        // The managed ledger closes at once, but its close callback, which completes the topic close, waits for the
        // test: a slow close as the topic sees it.
        BrokerTestInterceptor.INSTANCE.setManagedLedgerDecorator(created -> {
            if (!created.getName().equals(managedLedgerName)) {
                return created;
            }
            ManagedLedgerImpl decorated = BrokerTestUtil.spyWithoutRecordingInvocations(created);
            doAnswer(invocation -> {
                if (closesUntilHold.get() <= 0 || closesUntilHold.decrementAndGet() != 0) {
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
            producer.send("before transfer");

            PulsarService formerOwner = getAllBrokers().stream()
                    .filter(broker -> broker.getBrokerService().getTopicReference(topic).isPresent())
                    .findFirst().orElseThrow();
            PulsarService newOwner = getAllBrokers().stream()
                    .filter(broker -> broker != formerOwner).findFirst().orElseThrow();
            PersistentTopic formerTopic =
                    (PersistentTopic) formerOwner.getBrokerService().getTopicReference(topic).orElseThrow();
            NamespaceBundle bundle = formerOwner.getNamespaceService().getBundle(topicName);
            log.info()
                    .attr("heldClose", heldClose)
                    .attr("formerOwner", formerOwner.getBrokerId())
                    .attr("newOwner", newOwner.getBrokerId())
                    .attr("bundle", bundle)
                    .log("Delete-during-transfer test brokers");

            closesUntilHold.set(heldClose);
            CompletableFuture<Void> transfer = formerOwner.getNamespaceService()
                    .unloadNamespaceBundle(bundle, Optional.of(newOwner.getBrokerId()));
            closeHeld.get(10, TimeUnit.SECONDS);
            Optional<String> ownerWhileHeld = ExtensibleLoadManagerImpl.get(formerOwner)
                    .getOwnershipAsync(Optional.empty(), bundle).get(10, TimeUnit.SECONDS);
            log.info()
                    .attr("heldClose", heldClose)
                    .attr("transferring", formerTopic.isTransferring())
                    .attr("ownerWhileHeld", ownerWhileHeld)
                    .log("Topic close held on the former owner");
            assertThat(formerTopic.isFenced()).as("the transfer fenced the topic to close it").isTrue();

            CompletableFuture<Void> deletion = formerOwner.getBrokerService().deleteTopic(topic, false);
            assertThat(deletion).as("the delete waits for the close in progress").isNotDone();
            if (heldClose == 1) {
                // Before the unload timeout, so that the transfer goes on only after this close.
                releaseClose.complete(null);
            }

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
            Optional<String> ownerBeforeRelease = ExtensibleLoadManagerImpl.get(newOwner)
                    .getOwnershipAsync(Optional.empty(), bundle).get(10, TimeUnit.SECONDS);
            boolean deletionDoneBeforeRelease = deletion.isDone();
            log.info()
                    .attr("heldClose", heldClose)
                    .attr("ownerBeforeRelease", ownerBeforeRelease)
                    .attr("deletionDoneBeforeRelease", deletionDoneBeforeRelease)
                    .attr("newOwnerLedgerId", newOwnerLedgerId)
                    .log("New owner serves the topic");

            releaseClose.complete(null);
            Awaitility.await().atMost(Duration.ofSeconds(30)).until(deletion::isDone);
            Throwable deletionError = deletion.handle((ignored, error) -> error).join();
            Throwable transferError = transfer.handle((ignored, error) -> error).get(30, TimeUnit.SECONDS);
            boolean managedLedgerExists = newOwner.getPulsarResources().getTopicResources()
                    .persistentTopicExists(topicName).get(10, TimeUnit.SECONDS);
            boolean schemaExists = newOwner.getLocalMetadataStore().exists(schemaPath).get(10, TimeUnit.SECONDS);
            boolean newOwnerLedgerExists = mockBookKeeper.getLedgers().contains(newOwnerLedgerId);
            Throwable sendError = producer.sendAsync("after the delete on the former owner")
                    .handle((ignored, error) -> error).get(30, TimeUnit.SECONDS);
            log.info()
                    .attr("heldClose", heldClose)
                    .attr("deletionError", deletionError)
                    .attr("transferError", transferError)
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

    @DataProvider
    public Object[][] forceDelete() {
        return new Object[][] {{false}, {true}};
    }

    // A delete can pass the admin API's ownership check and reach the broker only after a transfer closed the topic
    // and handed the bundle over. This is such a delete as it reaches the broker.
    @Test(dataProvider = "forceDelete", timeOut = 120_000)
    public void testDeleteReachingFormerOwnerAfterTransfer(boolean forceDelete) throws Exception {
        String topic = "persistent://public/default/delete-reaching-former-owner-" + UUID.randomUUID();
        TopicName topicName = TopicName.get(topic);
        String schemaPath = "/schemas/" + topicName.getSchemaName();
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(topic).create();
        try {
            producer.send("before transfer");
            PulsarService formerOwner = getAllBrokers().stream()
                    .filter(broker -> broker.getBrokerService().getTopicReference(topic).isPresent())
                    .findFirst().orElseThrow();
            PulsarService newOwner = getAllBrokers().stream()
                    .filter(broker -> broker != formerOwner).findFirst().orElseThrow();
            NamespaceBundle bundle = formerOwner.getNamespaceService().getBundle(topicName);

            formerOwner.getNamespaceService().unloadNamespaceBundle(bundle, Optional.of(newOwner.getBrokerId()))
                    .get(30, TimeUnit.SECONDS);
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                assertThat(newOwner.getBrokerService().getTopicReference(topic))
                        .as("the new owner loaded the topic").isPresent();
                assertThat(newOwner.getBrokerService().getTopicReference(topic).orElseThrow().getProducers())
                        .as("the producer reconnected to the new owner").hasSize(1);
                assertThat(producer.isConnected()).as("the producer is connected").isTrue();
                assertThat(formerOwner.getBrokerService().getTopics())
                        .as("the topic cache of the former owner after the transfer").doesNotContainKey(topic);
            });
            producer.sendAsync("on the new owner").get(10, TimeUnit.SECONDS);
            PersistentTopic newTopic =
                    (PersistentTopic) newOwner.getBrokerService().getTopicReference(topic).orElseThrow();
            long newOwnerLedgerId = ((ManagedLedgerImpl) newTopic.getManagedLedger()).getLedgersInfo().lastKey();

            Throwable deletionError = formerOwner.getBrokerService().deleteTopic(topic, forceDelete)
                    .handle((ignored, error) -> error).get(30, TimeUnit.SECONDS);
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
                    .log("State after a delete that reached the former owner after the transfer");

            SoftAssertions softly = new SoftAssertions();
            softly.assertThat(FutureUtil.unwrapCompletionException(deletionError))
                    .as("the delete on %s, which no longer owns the bundle, must fail", formerOwner.getBrokerId())
                    .isInstanceOf(ServiceUnitNotReadyException.class);
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
            softly.assertThat(formerOwner.getBrokerService().getTopics())
                    .as("the topic cache of %s after the delete", formerOwner.getBrokerId())
                    .doesNotContainKey(topic);
            softly.assertAll();
        } finally {
            producer.closeAsync();
        }
    }
}
