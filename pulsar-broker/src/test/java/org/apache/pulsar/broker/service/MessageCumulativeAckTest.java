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
import static org.apache.pulsar.common.api.proto.CommandAck.AckType.Cumulative;
import static org.apache.pulsar.common.api.proto.CommandSubscribe.SubType.Exclusive;
import static org.apache.pulsar.common.api.proto.CommandSubscribe.SubType.Failover;
import static org.apache.pulsar.common.api.proto.CommandSubscribe.SubType.Key_Shared;
import static org.apache.pulsar.common.api.proto.CommandSubscribe.SubType.Shared;
import static org.apache.pulsar.common.protocol.Commands.DEFAULT_CONSUMER_EPOCH;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import io.netty.channel.ChannelHandlerContext;
import java.net.InetSocketAddress;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.api.proto.ProtocolVersion;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class MessageCumulativeAckTest {
    private final int consumerId = 1;

    private ServerCnx serverCnx;
    private PersistentSubscription sub;
    private PulsarTestContext pulsarTestContext;

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
        doReturn(new PulsarCommandSenderImpl(null, serverCnx))
                .when(serverCnx).getCommandSender();

        String topicName = TopicName.get("MessageCumulativeAckTest").toString();
        PersistentTopic persistentTopic = new PersistentTopic(topicName, mock(ManagedLedger.class), pulsarTestContext.getBrokerService());
        sub = spy(new PersistentSubscription(persistentTopic, "sub-1",
            mock(ManagedCursorImpl.class), false));
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

    @DataProvider(name = "individualAckModes")
    public static Object[][] individualAckModes() {
        return new Object[][]{
            {Shared},
            {Key_Shared},
        };
    }

    @DataProvider(name = "notIndividualAckModes")
    public static Object[][] notIndividualAckModes() {
        return new Object[][]{
            {Exclusive},
            {Failover},
        };
    }

    @Test(timeOut = 5000, dataProvider = "individualAckModes")
    public void testAckWithIndividualAckMode(CommandSubscribe.SubType subType) throws Exception {
        Consumer consumer = new Consumer(sub, subType, "topic-1", consumerId, 0,
            "Cons1", true, serverCnx, "myrole-1", emptyMap(), false, null,
            MessageId.latest, DEFAULT_CONSUMER_EPOCH);

        CommandAck commandAck = new CommandAck();
        commandAck.setAckType(Cumulative);
        commandAck.setConsumerId(consumerId);
        commandAck.addMessageId().setEntryId(0L).setLedgerId(1L);

        consumer.messageAcked(commandAck).get();
        verify(sub, never()).acknowledgeMessage(any(), any(), any());
    }

    @Test(timeOut = 5000, dataProvider = "notIndividualAckModes")
    public void testAckWithNotIndividualAckMode(CommandSubscribe.SubType subType) throws Exception {
        Consumer consumer = new Consumer(sub, subType, "topic-1", consumerId, 0,
            "Cons1", true, serverCnx, "myrole-1", emptyMap(), false, null,
            MessageId.latest, DEFAULT_CONSUMER_EPOCH);

        CommandAck commandAck = new CommandAck();
        commandAck.setAckType(Cumulative);
        commandAck.setConsumerId(consumerId);
        commandAck.addMessageId().setEntryId(0L).setLedgerId(1L);

        consumer.messageAcked(commandAck).get();
        verify(sub, times(1)).acknowledgeMessage(any(), any(), any());
    }

    @Test(timeOut = 5000)
    public void testAckWithMoreThanNoneMessageIds() throws Exception {
        Consumer consumer = new Consumer(sub, Failover, "topic-1", consumerId, 0,
            "Cons1", true, serverCnx, "myrole-1", emptyMap(), false, null,
            MessageId.latest, DEFAULT_CONSUMER_EPOCH);

        CommandAck commandAck = new CommandAck();
        commandAck.setAckType(Cumulative);
        commandAck.setConsumerId(consumerId);
        commandAck.addMessageId().setEntryId(0L).setLedgerId(1L);
        commandAck.addMessageId().setEntryId(0L).setLedgerId(2L);

        consumer.messageAcked(commandAck).get();
        verify(sub, never()).acknowledgeMessage(any(), any(), any());
    }
}
