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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.api.proto.ProducerAccessMode;
import org.apache.pulsar.common.protocol.schema.SchemaVersion;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ExclusiveProducerOwnershipTest extends SharedPulsarBaseTest {

    @Test
    public void testLateRemovalKeepsNewExclusiveProducer() throws Exception {
        AbstractTopic topic = spyTopic();
        Producer fenced = mockProducer(topic, "fenced", ProducerAccessMode.ExclusiveWithFencing);
        Producer fencing = mockProducer(topic, "fencing", ProducerAccessMode.ExclusiveWithFencing);
        add(topic, fenced).get(10, TimeUnit.SECONDS);
        add(topic, fencing).get(10, TimeUnit.SECONDS);

        topic.removeProducer(fenced);
        assertTrue(isSharedProducerRejected(topic));

        topic.removeProducer(fencing);
        assertFalse(isSharedProducerRejected(topic));
    }

    @Test
    public void testFailedAddHandsOverExclusiveOwnership() throws Exception {
        AbstractTopic topic = spyTopic();
        Producer attached = mockProducer(topic, "same-name", ProducerAccessMode.ExclusiveWithFencing);
        Producer failed = mockProducer(topic, "same-name", ProducerAccessMode.ExclusiveWithFencing);
        Producer waiting = mockProducer(topic, "waiting", ProducerAccessMode.WaitForExclusive);
        add(topic, attached).get(10, TimeUnit.SECONDS);
        assertThrows(ExecutionException.class, () -> add(topic, failed).get(10, TimeUnit.SECONDS));
        assertTrue(isSharedProducerRejected(topic));

        CompletableFuture<Optional<Long>> waitingAdded = add(topic, waiting);
        awaitWaitingProducers(topic, 1);
        topic.removeProducer(attached);
        waitingAdded.get(10, TimeUnit.SECONDS);

        topic.removeProducer(waiting);
        assertFalse(isSharedProducerRejected(topic));
    }

    @Test
    public void testFailedAddWithoutRemainingProducerPromotesWaitingProducer() throws Exception {
        AbstractTopic topic = spyTopic();
        CompletableFuture<Long> epoch = new CompletableFuture<>();
        doReturn(epoch).when(topic).incrementTopicEpoch(any());
        Producer failed = mockProducer(topic, "failed", ProducerAccessMode.Exclusive);
        Producer waiting = mockProducer(topic, "waiting", ProducerAccessMode.WaitForExclusive);
        doReturn(failedFuture()).when(topic).internalAddProducer(failed);

        CompletableFuture<Optional<Long>> failedAdded = add(topic, failed);
        Awaitility.await().until(() -> isSharedProducerRejected(topic));
        CompletableFuture<Optional<Long>> waitingAdded = add(topic, waiting);
        awaitWaitingProducers(topic, 1);

        doReturn(CompletableFuture.completedFuture(1L)).when(topic).incrementTopicEpoch(any());
        epoch.complete(0L);
        assertThrows(ExecutionException.class, () -> failedAdded.get(10, TimeUnit.SECONDS));
        waitingAdded.get(10, TimeUnit.SECONDS);
        assertEquals(topic.getProducers().get("waiting"), waiting);

        topic.removeProducer(waiting);
        assertFalse(isSharedProducerRejected(topic));
    }

    @Test
    public void testFailedAddWithoutRemainingProducerReleasesOwnership() throws Exception {
        AbstractTopic topic = spyTopic();
        Producer failed = mockProducer(topic, "failed", ProducerAccessMode.Exclusive);
        doReturn(failedFuture()).when(topic).internalAddProducer(failed);
        long usageCount = topic.currentUsageCount();

        assertThrows(ExecutionException.class, () -> add(topic, failed).get(10, TimeUnit.SECONDS));

        assertFalse(isSharedProducerRejected(topic));
        assertEquals(topic.currentUsageCount(), usageCount);
    }

    @Test
    public void testWaitingProducerIsPromotedOnlyWhenNoProducerRemains() throws Exception {
        AbstractTopic topic = spyTopic();
        Producer fenced = mockProducer(topic, "fenced", ProducerAccessMode.ExclusiveWithFencing);
        Producer fencing = mockProducer(topic, "fencing", ProducerAccessMode.ExclusiveWithFencing);
        Producer waiting = mockProducer(topic, "waiting", ProducerAccessMode.WaitForExclusive);
        add(topic, fenced).get(10, TimeUnit.SECONDS);
        add(topic, fencing).get(10, TimeUnit.SECONDS);
        CompletableFuture<Optional<Long>> waitingAdded = add(topic, waiting);
        awaitWaitingProducers(topic, 1);

        topic.removeProducer(fencing);
        assertEquals(topic.getWaitingProducersCount(), 1);

        topic.removeProducer(fenced);
        waitingAdded.get(10, TimeUnit.SECONDS);
        topic.removeProducer(waiting);
    }

    @Test
    public void testEpochFailurePromotesWaitingProducer() throws Exception {
        AbstractTopic topic = spyTopic();
        CompletableFuture<Long> epoch = new CompletableFuture<>();
        doReturn(epoch).when(topic).incrementTopicEpoch(any());
        Producer failed = mockProducer(topic, "failed", ProducerAccessMode.Exclusive);
        Producer waiting = mockProducer(topic, "waiting", ProducerAccessMode.WaitForExclusive);

        CompletableFuture<Optional<Long>> failedAdded = add(topic, failed);
        Awaitility.await().until(() -> isSharedProducerRejected(topic));
        CompletableFuture<Optional<Long>> waitingAdded = add(topic, waiting);
        awaitWaitingProducers(topic, 1);

        doReturn(CompletableFuture.completedFuture(1L)).when(topic).incrementTopicEpoch(any());
        epoch.completeExceptionally(new RuntimeException("injected"));
        assertThrows(ExecutionException.class, () -> failedAdded.get(10, TimeUnit.SECONDS));
        waitingAdded.get(10, TimeUnit.SECONDS);

        topic.removeProducer(waiting);
        assertFalse(isSharedProducerRejected(topic));
    }

    @Test
    public void testLateEpochFailureKeepsNewExclusiveProducer() throws Exception {
        AbstractTopic topic = spyTopic();
        CompletableFuture<Long> oldEpoch = new CompletableFuture<>();
        CompletableFuture<Long> newEpoch = new CompletableFuture<>();
        doReturn(oldEpoch).doReturn(newEpoch).when(topic).incrementTopicEpoch(any());
        Producer oldProducer = mockProducer(topic, "old", ProducerAccessMode.ExclusiveWithFencing);
        Producer newProducer = mockProducer(topic, "new", ProducerAccessMode.ExclusiveWithFencing);

        CompletableFuture<Optional<Long>> oldAdded = add(topic, oldProducer);
        CompletableFuture<Optional<Long>> newAdded = add(topic, newProducer);
        Awaitility.await().untilAsserted(() -> verify(topic, times(2)).incrementTopicEpoch(any()));

        oldEpoch.completeExceptionally(new RuntimeException("injected"));
        assertThrows(ExecutionException.class, () -> oldAdded.get(10, TimeUnit.SECONDS));
        assertTrue(isSharedProducerRejected(topic));

        newEpoch.complete(1L);
        newAdded.get(10, TimeUnit.SECONDS);
        topic.removeProducer(newProducer);
        assertFalse(isSharedProducerRejected(topic));
    }

    @Test
    public void testPromotedProducerEpochFailurePromotesNextWaitingProducer() throws Exception {
        AbstractTopic topic = spyTopic();
        Producer owner = mockProducer(topic, "owner", ProducerAccessMode.Exclusive);
        Producer failed = mockProducer(topic, "failed", ProducerAccessMode.WaitForExclusive);
        Producer next = mockProducer(topic, "next", ProducerAccessMode.WaitForExclusive);
        add(topic, owner).get(10, TimeUnit.SECONDS);
        CompletableFuture<Optional<Long>> failedAdded = add(topic, failed);
        awaitWaitingProducers(topic, 1);
        CompletableFuture<Optional<Long>> nextAdded = add(topic, next);
        awaitWaitingProducers(topic, 2);

        doReturn(failedFuture()).doReturn(CompletableFuture.completedFuture(1L))
                .when(topic).incrementTopicEpoch(any());
        topic.removeProducer(owner);

        assertThrows(ExecutionException.class, () -> failedAdded.get(10, TimeUnit.SECONDS));
        nextAdded.get(10, TimeUnit.SECONDS);
        topic.removeProducer(next);
        assertFalse(isSharedProducerRejected(topic));
    }

    @Test
    public void testFencedReservationIsNotAdded() throws Exception {
        AbstractTopic topic = spyTopic();
        CompletableFuture<Long> fencedEpoch = new CompletableFuture<>();
        doReturn(fencedEpoch).doReturn(CompletableFuture.completedFuture(1L)).when(topic).incrementTopicEpoch(any());
        Producer fenced = mockProducer(topic, "fenced", ProducerAccessMode.ExclusiveWithFencing);
        Producer fencing = mockProducer(topic, "fencing", ProducerAccessMode.ExclusiveWithFencing);

        CompletableFuture<Optional<Long>> fencedAdded = add(topic, fenced);
        add(topic, fencing).get(10, TimeUnit.SECONDS);
        fencedEpoch.complete(0L);

        assertThrows(ExecutionException.class, () -> fencedAdded.get(10, TimeUnit.SECONDS));
        assertNull(topic.getProducers().get("fenced"));
        topic.removeProducer(fencing);
    }

    @Test
    public void testFencedReservationIsNotAddedAfterFencingProducerFailed() throws Exception {
        AbstractTopic topic = spyTopic();
        CompletableFuture<Long> fencedEpoch = new CompletableFuture<>();
        doReturn(fencedEpoch).doReturn(CompletableFuture.completedFuture(1L)).when(topic).incrementTopicEpoch(any());
        Producer fenced = mockProducer(topic, "fenced", ProducerAccessMode.ExclusiveWithFencing);
        Producer fencing = mockProducer(topic, "fencing", ProducerAccessMode.ExclusiveWithFencing);
        doReturn(failedFuture()).when(topic).internalAddProducer(fencing);

        CompletableFuture<Optional<Long>> fencedAdded = add(topic, fenced);
        assertThrows(ExecutionException.class, () -> add(topic, fencing).get(10, TimeUnit.SECONDS));
        fencedEpoch.complete(0L);

        assertThrows(ExecutionException.class, () -> fencedAdded.get(10, TimeUnit.SECONDS));
        assertTrue(topic.getProducers().isEmpty());
        assertFalse(isSharedProducerRejected(topic));
    }

    @Test
    public void testLastPublishCompletionKeepsProducerClosedWithoutRemoval() throws Exception {
        AbstractTopic topic = spyTopic();
        Producer producer = realProducer(topic);
        add(topic, producer).get(10, TimeUnit.SECONDS);
        Topic.PublishContext publish = publishOneMessage(topic, producer);

        CompletableFuture<Void> closed = producer.close(false);
        assertFalse(closed.isDone());
        publish.completed(null, 1L, 1L);

        assertTrue(closed.isDone());
        assertEquals(topic.getProducers().get(producer.getProducerName()), producer);
        topic.removeProducer(producer);
    }

    @Test
    public void testForcedCloseDoesNotWaitForPendingPublishAcks() throws Exception {
        AbstractTopic topic = spyTopic();
        long usageCount = topic.currentUsageCount();
        Producer producer = realProducer(topic);
        add(topic, producer).get(10, TimeUnit.SECONDS);
        Topic.PublishContext publish = publishOneMessage(topic, producer);

        producer.closeNow(true);

        assertNull(topic.getProducers().get(producer.getProducerName()));
        assertTrue(producer.close(true).isDone());
        publish.completed(null, 1L, 1L);
        assertEquals(topic.currentUsageCount(), usageCount);
    }

    private AbstractTopic spyTopic() throws Exception {
        String topic = "persistent://" + getNamespace() + "/topic-" + UUID.randomUUID().toString().substring(0, 8);
        pulsarClient.newProducer(Schema.STRING).topic(topic).create().close();
        return spy((AbstractTopic) getTopicReference(topic).orElseThrow());
    }

    private CompletableFuture<Optional<Long>> add(AbstractTopic topic, Producer producer) {
        return topic.addProducer(producer, new CompletableFuture<>());
    }

    // Closing the mock does not remove it from the topic, like a producer with pending publish acks
    private Producer mockProducer(AbstractTopic topic, String name, ProducerAccessMode accessMode) {
        Producer producer = mock(Producer.class);
        when(producer.getTopic()).thenReturn(topic);
        when(producer.getProducerName()).thenReturn(name);
        when(producer.getAccessMode()).thenReturn(accessMode);
        when(producer.getTopicEpoch()).thenReturn(Optional.empty());
        when(producer.disconnect()).thenReturn(CompletableFuture.completedFuture(null));
        when(producer.disconnect(any())).thenReturn(CompletableFuture.completedFuture(null));
        return producer;
    }

    private Producer realProducer(AbstractTopic topic) {
        ServerCnx cnx = mock(ServerCnx.class);
        when(cnx.getBrokerService()).thenReturn(getPulsar().getBrokerService());
        when(cnx.clientSourceAddressAndPort()).thenReturn("127.0.0.1:1");
        when(cnx.clientSourceAddress()).thenReturn("127.0.0.1");
        when(cnx.getClientVersion()).thenReturn("test");
        when(cnx.isActive()).thenReturn(true);
        when(cnx.getCommandSender()).thenReturn(mock(PulsarCommandSender.class));
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(cnx).execute(any(Runnable.class));
        return new Producer(topic, cnx, 1, "real-producer", "role", false, null, SchemaVersion.Latest, 0, true,
                ProducerAccessMode.Shared, Optional.empty(), true);
    }

    private Topic.PublishContext publishOneMessage(AbstractTopic topic, Producer producer) {
        AtomicReference<Topic.PublishContext> publish = new AtomicReference<>();
        doAnswer(invocation -> {
            publish.set(invocation.getArgument(1));
            return null;
        }).when(topic).publishMessage(any(ByteBuf.class), any(Topic.PublishContext.class));
        producer.publishMessage(producer.getProducerId(), 1L, Unpooled.wrappedBuffer(new byte[32]), 1, false, false,
                null);
        assertEquals(producer.getPendingPublishAcks(), 1L);
        return publish.get();
    }

    private void awaitWaitingProducers(AbstractTopic topic, int count) {
        Awaitility.await().until(() -> topic.getWaitingProducersCount() == count);
    }

    private boolean isSharedProducerRejected(AbstractTopic topic) throws Exception {
        Producer shared = mockProducer(topic, "shared", ProducerAccessMode.Shared);
        return topic.incrementTopicEpochIfNeeded(shared, new CompletableFuture<>()).isCompletedExceptionally();
    }

    private static <T> CompletableFuture<T> failedFuture() {
        return CompletableFuture.failedFuture(new BrokerServiceException.ProducerBusyException("injected"));
    }
}
