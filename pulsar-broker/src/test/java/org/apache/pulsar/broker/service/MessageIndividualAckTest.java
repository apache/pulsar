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
import static org.apache.pulsar.common.api.proto.CommandAck.AckType.Individual;
import static org.apache.pulsar.common.api.proto.CommandSubscribe.SubType.Key_Shared;
import static org.apache.pulsar.common.api.proto.CommandSubscribe.SubType.Shared;
import static org.apache.pulsar.common.protocol.Commands.DEFAULT_CONSUMER_EPOCH;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import io.netty.channel.ChannelHandlerContext;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.api.proto.KeySharedMeta;
import org.apache.pulsar.common.api.proto.KeySharedMode;
import org.apache.pulsar.common.api.proto.ProtocolVersion;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.semaphore.AsyncDualMemoryLimiter;
import org.apache.pulsar.common.util.Codec;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class MessageIndividualAckTest {
    private final int consumerId = 1;

    private ServerCnx serverCnx;
    private PersistentSubscription sub;
    private PulsarTestContext pulsarTestContext;

    @DataProvider(name = "individualAckModes")
    public static Object[][] individualAckModes() {
        return new Object[][]{
                {Shared},
                {Key_Shared},
        };
    }


    @BeforeMethod
    public void setup() throws Exception {
        pulsarTestContext = PulsarTestContext.builderForNonStartableContext()
                .build();
        serverCnx = pulsarTestContext.createServerCnxSpy();
        doReturn(true).when(serverCnx).isActive();
        doReturn(true).when(serverCnx).isWritable();
        doReturn(new InetSocketAddress("localhost", 1234)).when(serverCnx).clientAddress();
        when(serverCnx.getRemoteEndpointProtocolVersion()).thenReturn(ProtocolVersion.v12.getValue());
        when(serverCnx.ctx()).thenReturn(mock(ChannelHandlerContext.class));
        AsyncDualMemoryLimiter maxTopicListInFlightLimiter = mock(AsyncDualMemoryLimiter.class);
        doReturn(new PulsarCommandSenderImpl(null, serverCnx, maxTopicListInFlightLimiter))
                .when(serverCnx).getCommandSender();

        String topicName = TopicName.get("MessageIndividualAckTest").toString();
        var mockManagedLedger = mock(ManagedLedger.class);
        when(mockManagedLedger.getConfig()).thenReturn(new ManagedLedgerConfig());
        var persistentTopic = new PersistentTopic(topicName, mockManagedLedger, pulsarTestContext.getBrokerService());
        ManagedCursor cursor = mock(ManagedCursor.class);
        doReturn(Codec.encode("sub-1")).when(cursor).getName();

        sub = spy(new PersistentSubscription(persistentTopic, "sub-1",
                cursor, false));
        doNothing().when(sub).acknowledgeMessage(any(), any(), any());
    }

    @AfterMethod(alwaysRun = true)
    public void shutdown() throws Exception {
        if (pulsarTestContext != null) {
            pulsarTestContext.close();
            pulsarTestContext = null;
        }
        sub = null;
    }

    @Test(timeOut = 5000, dataProvider = "individualAckModes")
    public void testIndividualAckNormalWithMessageNotExist(CommandSubscribe.SubType subType) throws Exception {
        KeySharedMeta keySharedMeta =
                subType == Key_Shared ? new KeySharedMeta().setKeySharedMode(KeySharedMode.AUTO_SPLIT) : null;
        Consumer consumer = new Consumer(sub, subType, "testIndividualAckNormal", consumerId, 0,
                "Cons1", true, serverCnx, "myrole-1", emptyMap(), false, keySharedMeta,
                MessageId.latest, DEFAULT_CONSUMER_EPOCH);
        sub.addConsumer(consumer);
        // mock a not exist ledger id and entry id
        final long notExistLedgerId = 9999L;
        final long notExistEntryId = 9999L;
        // mock an exist ledger id and entry id
        final long existLedgerId = 99L;
        final long existEntryId = 99L;
        // ack one message that not exists in the ledger and individualAckNormal() should return 0
        CommandAck commandAck = new CommandAck();
        commandAck.setAckType(Individual);
        commandAck.setConsumerId(consumerId);
        commandAck.addMessageId().setEntryId(notExistEntryId).setLedgerId(notExistLedgerId);
        Long l1 = consumer.individualAckNormal(commandAck, null).get();
        Assert.assertEquals(0L, l1.longValue());

        // ack two messages that one exists and the other not and individualAckNormal() should return 1
        consumer.getPendingAcks().addPendingAckIfAllowed(existLedgerId, existEntryId, 1, 99);
        commandAck = new CommandAck();
        commandAck.setAckType(Individual);
        commandAck.setConsumerId(consumerId);
        commandAck.addMessageId().setEntryId(notExistEntryId).setLedgerId(notExistLedgerId);
        commandAck.addMessageId().setEntryId(existEntryId).setLedgerId(existLedgerId);
        Long l2 = consumer.individualAckNormal(commandAck, null).get();
        Assert.assertEquals(1L, l2.longValue());
    }


    @Test(timeOut = 5000, dataProvider = "individualAckModes")
    public void testIndividualAckWithTransactionWithMessageNotExist(CommandSubscribe.SubType subType) throws Exception {
        KeySharedMeta keySharedMeta =
                subType == Key_Shared ? new KeySharedMeta().setKeySharedMode(KeySharedMode.AUTO_SPLIT) : null;
        Consumer consumer = new Consumer(sub, subType, "testIndividualAck", consumerId, 0,
                "Cons1", true, serverCnx, "myrole-1", emptyMap(), false, keySharedMeta,
                MessageId.latest, DEFAULT_CONSUMER_EPOCH);
        pulsarTestContext.getPulsarService().getConfig().setTransactionCoordinatorEnabled(true);
        sub.addConsumer(consumer);
        doNothing().when(sub).addUnAckedMessages(anyInt());
        CompletableFuture<Void> completedFuture = CompletableFuture.completedFuture(null);
        when(sub.transactionIndividualAcknowledge(any(), any())).thenReturn(completedFuture);
        // A not exist ledger id and entry id
        final long notExistLedgerId = 9999L;
        final long notExistEntryId = 9999L;
        // Ack one message that not exists in the ledger and individualAckWithTransaction() should return 0
        CommandAck commandAck = new CommandAck();
        commandAck.setTxnidMostBits(1L);
        commandAck.setTxnidLeastBits(1L);
        commandAck.setAckType(Individual);
        commandAck.setConsumerId(consumerId);
        commandAck.addMessageId().setEntryId(notExistEntryId).setLedgerId(notExistLedgerId);
        Long l1 = consumer.individualAckWithTransaction(commandAck).get();
        Assert.assertEquals(l1.longValue(), 0L);
    }
}
