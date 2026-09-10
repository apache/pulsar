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
    private final ConsumerStatsImpl stats = new ConsumerStatsImpl();

    @BeforeMethod
    public void beforeMethod() {
        subscription = mock(Subscription.class);
        ServerCnx cnx = mock(ServerCnx.class);
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
}
