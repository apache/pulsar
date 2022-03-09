/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.service;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.MessageIdData;
import org.apache.pulsar.common.protocol.Commands;
import org.mockito.Mockito;
import org.powermock.reflect.Whitebox;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;

public class BrokerInterceptorTest {

    @Test
    public void testInterceptAck() throws Throwable {
        ServerCnx serverCnx = Mockito.mock(ServerCnx.class, CALLS_REAL_METHODS);
        Whitebox.setInternalState(serverCnx, "cmd", new BaseCommand());

        BrokerInterceptor interceptor = Mockito.mock(BrokerInterceptor.class);
        BrokerService brokerService = Mockito.mock(BrokerService.class);
        Mockito.doReturn(brokerService).when(serverCnx).getBrokerService();
        Mockito.doReturn(interceptor).when(brokerService).getInterceptor();

        long consumerId = -1;
        long ledgerId = -2;
        long entryId = -3;
        AtomicInteger counter = new AtomicInteger(0);
        Mockito.doAnswer(invocationOnMock -> {
            BaseCommand command = invocationOnMock.getArgument(0);
            if (command.getType().equals(BaseCommand.Type.ACK)) {
                CommandAck ack = command.getAck();

                Assert.assertEquals(ack.getConsumerId(), consumerId);
                Assert.assertEquals(ack.getMessageIdsCount(), 1);

                MessageIdData idData = ack.getMessageIdAt(0);
                Assert.assertEquals(idData.getLedgerId(), ledgerId);
                Assert.assertEquals(idData.getEntryId(), entryId);

                counter.getAndIncrement();
            }
            return null;
        }).when(interceptor).onPulsarCommand(any(BaseCommand.class), any(ServerCnx.class));

        Mockito.doNothing().when(serverCnx).handleAck(any());

        ByteBuf ack = Commands.newAck(consumerId, ledgerId, entryId, null, CommandAck.AckType.Individual,
                null, Collections.emptyMap(), -1);
        serverCnx.channelRead(Mockito.mock(ChannelHandlerContext.class), ack);

        Assert.assertEquals(counter.get(), 1);
    }
}
