/**
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
import static org.apache.pulsar.broker.BrokerTestUtil.spyWithClassAndConstructorArgs;
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
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import java.net.InetSocketAddress;
import java.util.Optional;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.impl.ManagedCursorImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.PulsarServiceMockSupport;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.broker.service.persistent.PersistentSubscription;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.TransactionTestBase;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.CommandSubscribe;
import org.apache.pulsar.common.api.proto.ProtocolVersion;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class MessageCumulativeAckTest {
    private final int consumerId = 1;
    private BrokerService brokerService;
    private ServerCnx serverCnx;
    private MetadataStore store;
    protected PulsarService pulsar;
    private OrderedExecutor executor;
    private EventLoopGroup eventLoopGroup;
    private PersistentSubscription sub;

    @BeforeMethod
    public void setup() throws Exception {
        executor = OrderedExecutor.newBuilder().numThreads(1).name("persistent-dispatcher-cumulative-ack-test").build();
        ServiceConfiguration svcConfig = spy(ServiceConfiguration.class);
        svcConfig.setBrokerShutdownTimeoutMs(0L);
        svcConfig.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
        svcConfig.setClusterName("pulsar-cluster");
        pulsar = spyWithClassAndConstructorArgs(PulsarService.class, svcConfig);
        doReturn(svcConfig).when(pulsar).getConfiguration();

        ManagedLedgerFactory mlFactoryMock = mock(ManagedLedgerFactory.class);
        doReturn(mlFactoryMock).when(pulsar).getManagedLedgerFactory();
        doReturn(TransactionTestBase.createMockBookKeeper(executor))
            .when(pulsar).getBookKeeperClient();

        store = MetadataStoreFactory.create("memory:local", MetadataStoreConfig.builder().build());
        doReturn(store).when(pulsar).getLocalMetadataStore();
        doReturn(store).when(pulsar).getConfigurationMetadataStore();

        PulsarResources pulsarResources = new PulsarResources(store, store);
        PulsarServiceMockSupport.mockPulsarServiceProps(pulsar, () -> {
            doReturn(pulsarResources).when(pulsar).getPulsarResources();
        });

        eventLoopGroup = new NioEventLoopGroup();
        brokerService = spyWithClassAndConstructorArgs(BrokerService.class, pulsar, eventLoopGroup);
        PulsarServiceMockSupport.mockPulsarServiceProps(pulsar, () -> {
            doReturn(brokerService).when(pulsar).getBrokerService();
        });

        serverCnx = spyWithClassAndConstructorArgs(ServerCnx.class, pulsar);
        doReturn(true).when(serverCnx).isActive();
        doReturn(true).when(serverCnx).isWritable();
        doReturn(new InetSocketAddress("localhost", 1234)).when(serverCnx).clientAddress();
        when(serverCnx.getRemoteEndpointProtocolVersion()).thenReturn(ProtocolVersion.v12.getValue());
        when(serverCnx.ctx()).thenReturn(mock(ChannelHandlerContext.class));
        doReturn(new PulsarCommandSenderImpl(null, serverCnx))
                .when(serverCnx).getCommandSender();

        String topicName = TopicName.get("MessageCumulativeAckTest").toString();
        PersistentTopic persistentTopic = new PersistentTopic(topicName, mock(ManagedLedger.class), brokerService);
        sub = spy(new PersistentSubscription(persistentTopic, "sub-1",
            mock(ManagedCursorImpl.class), false));
        doNothing().when(sub).acknowledgeMessage(any(), any(), any());
    }

    @AfterMethod(alwaysRun = true)
    public void shutdown() throws Exception {
        if (brokerService != null) {
            brokerService.close();
            brokerService = null;
        }
        if (pulsar != null) {
            pulsar.close();
            pulsar = null;
        }

        executor.shutdown();
        if (eventLoopGroup != null) {
            eventLoopGroup.shutdownGracefully().get();
        }
        store.close();
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
                null, MessageId.latest, DEFAULT_CONSUMER_EPOCH);

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
            null, MessageId.latest, DEFAULT_CONSUMER_EPOCH);

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
            null, MessageId.latest, DEFAULT_CONSUMER_EPOCH);

        CommandAck commandAck = new CommandAck();
        commandAck.setAckType(Cumulative);
        commandAck.setConsumerId(consumerId);
        commandAck.addMessageId().setEntryId(0L).setLedgerId(1L);
        commandAck.addMessageId().setEntryId(0L).setLedgerId(2L);

        consumer.messageAcked(commandAck).get();
        verify(sub, never()).acknowledgeMessage(any(), any(), any());
    }
}
