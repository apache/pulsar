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
package org.apache.pulsar.broker.service.nonpersistent;

import static org.apache.pulsar.common.protocol.Commands.DEFAULT_CONSUMER_EPOCH;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.Consumer;
import org.apache.pulsar.broker.service.PulsarCommandSenderImpl;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandActiveConsumerChange;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.api.proto.ProtocolVersion;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.semaphore.AsyncDualMemoryLimiter;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Verifies non-persistent Failover subscriptions notify clients of active-consumer changes.
 */
@Test(groups = "broker")
public class NonPersistentDispatcherFailoverConsumerTest {

    private static final String TOPIC_NAME = "non-persistent://prop/ns-abc/failover-notify";

    private record ActiveChange(long consumerId, boolean isActive) {
    }

    private record TestDispatcher(NonPersistentTopic topic, NonPersistentSubscription sub,
                                  NonPersistentDispatcherSingleActiveConsumer dispatcher) {
    }

    private PulsarTestContext pulsarTestContext;
    private ServerCnx serverCnx;
    private LinkedBlockingQueue<ActiveChange> consumerChanges;

    @BeforeMethod
    public void setup() throws Exception {
        ServiceConfiguration svcConfig = new ServiceConfiguration();
        svcConfig.setBrokerShutdownTimeoutMs(0L);
        svcConfig.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
        svcConfig.setClusterName("pulsar-cluster");
        svcConfig.setSystemTopicEnabled(false);
        svcConfig.setTopicLevelPoliciesEnabled(false);
        svcConfig.setActiveConsumerFailoverDelayTimeMillis(0);
        pulsarTestContext = PulsarTestContext.builderForNonStartableContext()
                .config(svcConfig)
                .spyByDefault()
                .build();

        consumerChanges = new LinkedBlockingQueue<>();
        ChannelHandlerContext channelCtx = mock(ChannelHandlerContext.class);
        Channel channel = mock(Channel.class);
        when(channelCtx.channel()).thenReturn(channel);
        doAnswer(invocationOnMock -> {
            ByteBuf buf = invocationOnMock.getArgument(0);
            ByteBuf cmdBuf = buf.retainedSlice(4, buf.writerIndex() - 4);
            try {
                int cmdSize = (int) cmdBuf.readUnsignedInt();
                BaseCommand cmd = new BaseCommand();
                cmd.parseFrom(cmdBuf, cmdSize);
                if (cmd.hasActiveConsumerChange()) {
                    CommandActiveConsumerChange change = cmd.getActiveConsumerChange();
                    consumerChanges.put(new ActiveChange(change.getConsumerId(), change.isIsActive()));
                }
            } finally {
                cmdBuf.release();
                buf.release();
            }
            return null;
        }).when(channelCtx).writeAndFlush(any(), any());

        AsyncDualMemoryLimiter maxTopicListInFlightLimiter = mock(AsyncDualMemoryLimiter.class);
        serverCnx = createServerCnx(channelCtx, maxTopicListInFlightLimiter);

        NamespaceService nsSvc = pulsarTestContext.getPulsarService().getNamespaceService();
        doReturn(true).when(nsSvc).isServiceUnitOwned(any(NamespaceBundle.class));
        doReturn(CompletableFuture.completedFuture(mock(NamespaceBundle.class))).when(nsSvc).getBundleAsync(any());
        doReturn(CompletableFuture.completedFuture(true)).when(nsSvc).checkBundleOwnership(any(), any());
    }

    @AfterMethod(alwaysRun = true)
    public void shutdown() throws Exception {
        if (pulsarTestContext != null) {
            pulsarTestContext.close();
            pulsarTestContext = null;
        }
    }

    private ServerCnx createServerCnx(ChannelHandlerContext channelCtx,
                                      AsyncDualMemoryLimiter maxTopicListInFlightLimiter) {
        ServerCnx cnx = pulsarTestContext.createServerCnxSpy();
        doReturn(true).when(cnx).isActive();
        doReturn(true).when(cnx).isWritable();
        doReturn(new InetSocketAddress("localhost", 1234)).when(cnx).clientAddress();
        when(cnx.getRemoteEndpointProtocolVersion()).thenReturn(ProtocolVersion.v12.getValue());
        when(cnx.ctx()).thenReturn(channelCtx);
        doReturn(CompletableFuture.completedFuture(Optional.of(true))).when(cnx).checkConnectionLiveness();
        doReturn(new PulsarCommandSenderImpl(null, cnx, maxTopicListInFlightLimiter)).when(cnx).getCommandSender();
        return cnx;
    }

    private TestDispatcher newDispatcher(SubType subType) {
        NonPersistentTopic topic = new NonPersistentTopic(TOPIC_NAME, pulsarTestContext.getBrokerService());
        NonPersistentSubscription sub = new NonPersistentSubscription(topic, "sub-1", Collections.emptyMap());
        return new TestDispatcher(topic, sub,
                new NonPersistentDispatcherSingleActiveConsumer(subType, 0, topic, sub));
    }

    private Consumer newConsumer(TestDispatcher td, SubType subType, long consumerId, String consumerName) {
        return newConsumer(td, subType, consumerId, 0, consumerName, serverCnx);
    }

    private Consumer newConsumer(TestDispatcher td, SubType subType, long consumerId, int priorityLevel,
                                 String consumerName, ServerCnx cnx) {
        return new Consumer(td.sub(), subType, td.topic().getName(), consumerId, priorityLevel, consumerName,
                true, cnx, "myrole-1", Collections.emptyMap(), false, null, MessageId.latest, DEFAULT_CONSUMER_EPOCH);
    }

    private List<ActiveChange> takeExactChanges(int expected) {
        List<ActiveChange> actual = new ArrayList<>();
        consumerChanges.drainTo(actual);
        assertEquals(actual.size(), expected, "unexpected ActiveConsumerChange notifications: " + actual);
        return actual;
    }

    private void assertNoActiveConsumerChanges() {
        assertTrue(consumerChanges.isEmpty(),
                "unexpected ActiveConsumerChange notifications: " + consumerChanges);
    }

    @Test(timeOut = 10000)
    public void testActiveConsumerChangeNotifications() throws Exception {
        TestDispatcher td = newDispatcher(SubType.Failover);
        NonPersistentDispatcherSingleActiveConsumer dispatcher = td.dispatcher();
        assertFalse(dispatcher.isConsumerConnected());

        Consumer consumer1 = newConsumer(td, SubType.Failover, 1, "Cons1");
        dispatcher.addConsumer(consumer1).get();
        assertSame(dispatcher.getActiveConsumer(), consumer1);
        assertEquals(takeExactChanges(1), List.of(new ActiveChange(1, true)));

        Consumer consumer2 = newConsumer(td, SubType.Failover, 2, "Cons2");
        dispatcher.addConsumer(consumer2).get();
        assertSame(dispatcher.getActiveConsumer(), consumer1);
        assertEquals(takeExactChanges(1), List.of(new ActiveChange(2, false)));

        Consumer consumer0 = newConsumer(td, SubType.Failover, 0, "Cons0");
        dispatcher.addConsumer(consumer0).get();
        assertSame(dispatcher.getActiveConsumer(), consumer0);
        // consumers is sorted by name before notify, so Cons0, Cons1, Cons2.
        assertEquals(takeExactChanges(3), List.of(
                new ActiveChange(0, true),
                new ActiveChange(1, false),
                new ActiveChange(2, false)));

        dispatcher.removeConsumer(consumer0);
        assertSame(dispatcher.getActiveConsumer(), consumer1);
        assertEquals(takeExactChanges(2), List.of(
                new ActiveChange(1, true),
                new ActiveChange(2, false)));

        dispatcher.removeConsumer(consumer2);
        assertSame(dispatcher.getActiveConsumer(), consumer1);
        assertNoActiveConsumerChanges();

        dispatcher.removeConsumer(consumer1);
        assertFalse(dispatcher.isConsumerConnected());
        assertNoActiveConsumerChanges();
    }

    @Test(timeOut = 10000)
    public void testAddConsumerWhenClosed() throws Exception {
        NonPersistentDispatcherSingleActiveConsumer dispatcher = newDispatcher(SubType.Failover).dispatcher();
        dispatcher.close().get();

        Consumer consumer = mock(Consumer.class);
        dispatcher.addConsumer(consumer).get();
        verify(consumer, times(1)).disconnect();
        assertEquals(dispatcher.getConsumers().size(), 0);
        assertNoActiveConsumerChanges();
    }

    @Test(timeOut = 10000)
    public void testHigherPriorityConsumerBecomesActive() throws Exception {
        TestDispatcher td = newDispatcher(SubType.Failover);
        NonPersistentDispatcherSingleActiveConsumer dispatcher = td.dispatcher();

        Consumer consumer1 = newConsumer(td, SubType.Failover, 1, 1, "Cons1", serverCnx);
        dispatcher.addConsumer(consumer1).get();
        assertEquals(takeExactChanges(1), List.of(new ActiveChange(1, true)));

        Consumer consumer2 = newConsumer(td, SubType.Failover, 2, 1, "Cons2", serverCnx);
        dispatcher.addConsumer(consumer2).get();
        assertEquals(takeExactChanges(1), List.of(new ActiveChange(2, false)));

        Consumer consumer3 = newConsumer(td, SubType.Failover, 3, 0, "Cons3", serverCnx);
        dispatcher.addConsumer(consumer3).get();
        assertSame(dispatcher.getActiveConsumer(), consumer3);
        assertEquals(takeExactChanges(3), List.of(
                new ActiveChange(3, true),
                new ActiveChange(1, false),
                new ActiveChange(2, false)));
    }
}
