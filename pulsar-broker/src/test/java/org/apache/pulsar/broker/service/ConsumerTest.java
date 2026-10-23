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

import static java.util.Collections.emptyMap;
import static org.apache.pulsar.client.api.MessageId.latest;
import static org.apache.pulsar.common.api.proto.CommandSubscribe.SubType.Exclusive;
import static org.apache.pulsar.common.api.proto.CommandSubscribe.SubType.Shared;
import static org.apache.pulsar.common.api.proto.KeySharedMode.AUTO_SPLIT;
import static org.apache.pulsar.common.protocol.Commands.DEFAULT_CONSUMER_EPOCH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.policies.data.HierarchyTopicPolicies;
import org.apache.pulsar.common.policies.data.stats.ConsumerStatsImpl;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class ConsumerTest {
    private Consumer consumer;
    private Subscription subscription;
    private ServerCnx cnx;
    private final ConsumerStatsImpl stats = new ConsumerStatsImpl();

    @BeforeMethod
    public void beforeMethod() {
        subscription = mock(Subscription.class);
        cnx = mock(ServerCnx.class);
        SocketAddress address = mock(SocketAddress.class);
        Topic topic = mock(Topic.class);
        BrokerService brokerService = mock(BrokerService.class);
        PulsarService pulsarService = mock(PulsarService.class);
        ServiceConfiguration serviceConfiguration = mock(ServiceConfiguration.class);

        when(cnx.clientAddress()).thenReturn(address);
        when(subscription.getTopic()).thenReturn(topic);
        HierarchyTopicPolicies policies = new HierarchyTopicPolicies();
        policies.getMaxUnackedMessagesOnConsumer().updateBrokerValue(0);
        when(topic.getHierarchyTopicPolicies()).thenReturn(policies);
        when(topic.getBrokerService()).thenReturn(brokerService);
        when(brokerService.getPulsar()).thenReturn(pulsarService);
        when(pulsarService.getConfiguration()).thenReturn(serviceConfiguration);

        consumer =
                new Consumer(subscription, Exclusive, "topic", 1, 0, "Cons1", true, cnx, "myrole-1", emptyMap(), false,
                        new KeySharedMeta().setKeySharedMode(AUTO_SPLIT), latest, DEFAULT_CONSUMER_EPOCH);
    }

    @Test
    public void testGetMsgOutCounter() {
        stats.msgOutCounter = 1L;
        consumer.updateStats(stats);
        assertEquals(consumer.getMsgOutCounter(), 1L);
    }

    @Test
    public void testGetBytesOutCounter() {
        stats.bytesOutCounter = 1L;
        consumer.updateStats(stats);
        assertEquals(consumer.getBytesOutCounter(), 1L);
    }

    @DataProvider
    public Object[][] groupedAcknowledgements() {
        return new Object[][] {{0, false}, {1, false}, {1000, false}, {1, true}};
    }

    @Test(dataProvider = "groupedAcknowledgements")
    public void testGroupedAcknowledgementsPreservePositionsAndCompletion(int count, boolean validationError) {
        CompletableFuture<Void> persistence = new CompletableFuture<>();
        when(subscription.acknowledgeMessageAsync(any(), eq(CommandAck.AckType.Individual), any()))
                .thenReturn(persistence);
        CommandAck ack = new CommandAck().setConsumerId(1).setAckType(CommandAck.AckType.Individual);
        if (validationError) {
            ack.setValidationError(CommandAck.ValidationError.ChecksumMismatch);
        }
        for (int i = 0; i < count; i++) {
            ack.addMessageId().setLedgerId(7).setEntryId(i);
        }

        CompletableFuture<Void> result = consumer.messageAcked(ack, true);
        ArgumentCaptor<List<Position>> positions = ArgumentCaptor.captor();
        verify(subscription).acknowledgeMessageAsync(positions.capture(),
                eq(CommandAck.AckType.Individual), eq(emptyMap()));
        assertThat(positions.getValue()).hasSize(count);
        for (int i = 0; i < count; i++) {
            assertThat(positions.getValue().get(i).getLedgerId()).isEqualTo(7);
            assertThat(positions.getValue().get(i).getEntryId()).isEqualTo(i);
        }
        assertThat(result).isNotDone();
        persistence.complete(null);
        assertThat(result).isCompletedWithValue(null);
    }
    @Test
    public void testAckCommandsKeepIndependentCompletionState() {
        Consumer receiver = new Consumer(subscription, Shared, "topic", 2, 0, "receiver", true, cnx,
                "myrole-1", emptyMap(), false, null, latest, DEFAULT_CONSUMER_EPOCH);
        when(subscription.getConsumers()).thenReturn(List.of(receiver));
        receiver.getPendingAcks().addPendingAckIfAllowed(7, 1, 3, 0);
        receiver.getPendingAcks().addPendingAckIfAllowed(7, 2, 5, 0);
        CompletableFuture<Void> firstPersistence = new CompletableFuture<>();
        CompletableFuture<Void> secondPersistence = new CompletableFuture<>();
        when(subscription.acknowledgeMessageAsync(any(), eq(CommandAck.AckType.Individual), any()))
                .thenReturn(firstPersistence, secondPersistence);
        CommandAck ack = new CommandAck().setConsumerId(2).setAckType(CommandAck.AckType.Individual);
        ack.addMessageId().setLedgerId(7).setEntryId(1);
        try {
            CompletableFuture<Void> first = receiver.messageAcked(ack, true);
            ack.getMessageIdAt(0).setEntryId(2);
            CompletableFuture<Void> second = receiver.messageAcked(ack, true);
            // The parsed command can be reused while persistence callbacks are still pending.
            ack.clear();
            secondPersistence.complete(null);
            assertThat(second).isCompletedWithValue(null);
            assertThat(first).isNotDone();
            assertThat(receiver.getPendingAcks().contains(7, 2)).isFalse();
            assertThat(receiver.getPendingAcks().getRemainingUnacked(7, 1)).isEqualTo(3);
            assertThat(receiver.getMessageAckCounter()).isEqualTo(5);
            firstPersistence.complete(null);
            assertThat(first).isCompletedWithValue(null);
            assertThat(receiver.getPendingAcks().contains(7, 1)).isFalse();
            assertThat(receiver.getMessageAckCounter()).isEqualTo(8);
        } finally {
            receiver.getPendingAcks().forEachAndClose((ledgerId, entryId, remaining, stickyKeyHash) -> { });
        }
    }

    @DataProvider
    public Object[][] completionOutcomes() {
        return new Object[][] {
                {true, true, 12L, 2}, {true, false, 12L, 2},
                {false, true, 12L, 2}, {false, false, 12L, 2},
                {true, true, 15L, 0}, {true, false, 15L, 0},
                {false, true, 15L, 0}, {false, false, 15L, 0},
                {true, true, 31L, -1}, {true, false, 31L, -1},
                {false, true, 31L, -1}, {false, false, 31L, -1}
        };
    }

    @Test(dataProvider = "completionOutcomes")
    public void testMixedAckCompletionsKeepIndependentSnapshot(boolean requirePersistence, boolean success,
                                                             long ackSet, int ackedDelta) {
        when(subscription.getTopic().getBrokerService().getPulsar().getConfiguration()
                .isAcknowledgmentAtBatchIndexLevelEnabled()).thenReturn(true);
        Consumer receiver = new Consumer(subscription, Shared, "topic", 2, 0, "receiver", true, cnx,
                "myrole-1", emptyMap(), false, null, latest, DEFAULT_CONSUMER_EPOCH);
        Consumer other = new Consumer(subscription, Shared, "topic", 3, 0, "other", true, cnx,
                "myrole-1", emptyMap(), false, null, latest, DEFAULT_CONSUMER_EPOCH);
        when(subscription.getConsumers()).thenReturn(List.of(receiver, other));
        receiver.getPendingAcks().addPendingAckIfAllowed(7, 1, 1, 0);
        other.getPendingAcks().addPendingAckIfAllowed(7, 2, 4, 0);
        receiver.getPendingAcks().addPendingAckIfAllowed(7, 3, 2, 0);
        CompletableFuture<Void> persistence = new CompletableFuture<>();
        when(subscription.acknowledgeMessageAsync(any(), eq(CommandAck.AckType.Individual), any()))
                .thenAnswer(invocation -> {
                    List<Position> positions = invocation.getArgument(0);
                    positions.clear();
                    return persistence;
                });
        CommandAck ack = new CommandAck().setConsumerId(2).setAckType(CommandAck.AckType.Individual);
        ack.addMessageId().setLedgerId(7).setEntryId(1);
        ack.addMessageId().setLedgerId(7).setEntryId(2).addAckSet(ackSet);
        ack.addMessageId().setLedgerId(7).setEntryId(3);
        try {
            CompletableFuture<Void> result = receiver.messageAcked(ack, requirePersistence);
            assertThat(receiver.getPendingAcks().getRemainingUnacked(7, 1)).isEqualTo(1);
            assertThat(other.getPendingAcks().getRemainingUnacked(7, 2)).isEqualTo(4);
            if (requirePersistence) {
                assertThat(result).isNotDone();
            } else {
                assertThat(result).isCompletedWithValue(null);
            }
            if (success) {
                persistence.complete(null);
                assertThat(result).isCompletedWithValue(null);
                assertThat(receiver.getPendingAcks().contains(7, 1)).isFalse();
                assertThat(receiver.getPendingAcks().contains(7, 3)).isFalse();
                assertThat(other.getPendingAcks().getRemainingUnacked(7, 2)).isEqualTo(4 - Math.max(0, ackedDelta));
                assertThat(receiver.getMessageAckCounter()).isEqualTo(3 + ackedDelta);
            } else {
                persistence.completeExceptionally(new IllegalStateException("persistence failed"));
                if (requirePersistence) {
                    assertThat(result).isCompletedExceptionally();
                }
                assertThat(receiver.getPendingAcks().getRemainingUnacked(7, 1)).isEqualTo(1);
                assertThat(receiver.getPendingAcks().getRemainingUnacked(7, 3)).isEqualTo(2);
                assertThat(other.getPendingAcks().getRemainingUnacked(7, 2)).isEqualTo(4);
            }
        } finally {
            receiver.getPendingAcks().forEachAndClose((ledgerId, entryId, remaining, stickyKeyHash) -> { });
            other.getPendingAcks().forEachAndClose((ledgerId, entryId, remaining, stickyKeyHash) -> { });
        }
    }

}
