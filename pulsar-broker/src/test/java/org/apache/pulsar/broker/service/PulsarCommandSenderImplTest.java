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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.ImmediateEventExecutor;
import java.util.ArrayList;
import java.util.List;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.api.proto.ProtocolVersion;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PulsarCommandSenderImplTest {

    @Test
    public void testSendMessagesMapsFinalizedPerEntryPermitsToWireCommands() {
        ServerCnx cnx = mock(ServerCnx.class);
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        Channel channel = mock(Channel.class);
        EventLoop eventLoop = mock(EventLoop.class);
        ChannelPromise writePromise = new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE);
        ChannelPromise voidPromise = mock(ChannelPromise.class);
        BrokerService brokerService = mock(BrokerService.class);
        PulsarService pulsarService = mock(PulsarService.class);
        ServiceConfiguration configuration = mock(ServiceConfiguration.class);
        List<Integer> serializedPermits = new ArrayList<>();

        when(cnx.ctx()).thenReturn(ctx);
        when(ctx.channel()).thenReturn(channel);
        when(channel.eventLoop()).thenReturn(eventLoop);
        doAnswer(invocation -> {
            invocation.<Runnable>getArgument(0).run();
            return null;
        }).when(eventLoop).execute(any(Runnable.class));
        when(ctx.newPromise()).thenReturn(writePromise);
        when(ctx.voidPromise()).thenReturn(voidPromise);
        doAnswer(invocation -> {
            invocation.<ChannelPromise>getArgument(1).trySuccess();
            return invocation.getArgument(1);
        }).when(ctx).writeAndFlush(any(), eq(writePromise));
        when(cnx.isBatchMessageCompatibleVersion()).thenReturn(true);
        when(cnx.getRemoteEndpointProtocolVersion()).thenReturn(ProtocolVersion.v18.getValue());
        when(cnx.supportBrokerMetadata()).thenReturn(true);
        when(cnx.getBrokerService()).thenReturn(brokerService);
        when(brokerService.getPulsar()).thenReturn(pulsarService);
        when(pulsarService.getConfig()).thenReturn(configuration);
        when(configuration.isExposingBrokerEntryMetadataToClientEnabled()).thenReturn(true);
        when(cnx.newMessageAndIntercept(anyLong(), anyLong(), anyLong(), anyInt(), anyInt(), any(ByteBuf.class),
                any(), anyString(), anyLong(), anyInt())).thenAnswer(invocation -> {
                    serializedPermits.add(invocation.getArgument(9));
                    return mock(ByteBufPair.class);
                });

        Entry first = mock(Entry.class);
        Entry second = mock(Entry.class);
        ByteBuf firstPayload = Unpooled.buffer(0);
        ByteBuf secondPayload = Unpooled.buffer(0);
        when(first.getLedgerId()).thenReturn(1L);
        when(first.getEntryId()).thenReturn(2L);
        when(first.getDataBuffer()).thenReturn(firstPayload);
        when(second.getLedgerId()).thenReturn(3L);
        when(second.getEntryId()).thenReturn(4L);
        when(second.getDataBuffer()).thenReturn(secondPayload);
        List<Entry> entries = new ArrayList<>();
        entries.add(first);
        entries.add(null);
        entries.add(second);
        EntryBatchSizes batchSizes = EntryBatchSizes.get(entries.size());
        batchSizes.setBatchSize(0, 10);
        batchSizes.setBatchSize(2, 4);
        SendMessagesResult sendResult = new SendMessagesResult(entries.size());
        sendResult.setMessagePermits(0, 3);
        sendResult.setMessagePermits(2, 4);

        try {
            PulsarCommandSenderImpl sender = new PulsarCommandSenderImpl(null, cnx, null);
            sender.sendMessagesToConsumer(7, "topic", mock(Subscription.class), -1, entries, batchSizes, null,
                    sendResult, mock(RedeliveryTracker.class), 11);

            assertEquals(serializedPermits, List.of(3, 4));
            verify(first).release();
            verify(second).release();
        } finally {
            while (firstPayload.refCnt() > 0) {
                firstPayload.release();
            }
            while (secondPayload.refCnt() > 0) {
                secondPayload.release();
            }
        }
    }
}
