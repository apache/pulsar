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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.ArrayList;
import java.util.List;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PulsarCommandSenderImplTest {
    private EmbeddedChannel channel;
    private PulsarCommandSenderImpl commandSender;
    private final List<BaseCommand.Type> interceptedTypes = new ArrayList<>();

    @BeforeMethod
    public void setup() throws Exception {
        interceptedTypes.clear();
        channel = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        ServerCnx cnx = mock(ServerCnx.class);
        when(cnx.ctx()).thenReturn(channel.pipeline().firstContext());
        // an interceptor that builds another command on the same thread, as one that sends a message with a
        // producer does: the commands built by Commands share a thread-local instance
        BrokerInterceptor interceptor = mock(BrokerInterceptor.class);
        doAnswer(invocation -> {
            BaseCommand command = invocation.getArgument(0);
            interceptedTypes.add(command.getType());
            Commands.newSuccess(Long.MAX_VALUE).release();
            return null;
        }).when(interceptor).onPulsarCommand(any(), any());
        commandSender = new PulsarCommandSenderImpl(interceptor, cnx, null);
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() {
        if (channel != null) {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void testSendReceiptIsNotOverwrittenByInterceptor() {
        commandSender.sendSendReceiptResponse(1, 2, 3, 4, 5);

        BaseCommand response = readResponse();
        assertThat(interceptedTypes).containsExactly(BaseCommand.Type.SEND_RECEIPT);
        assertThat(response.getType()).isEqualTo(BaseCommand.Type.SEND_RECEIPT);
        assertThat(response.getSendReceipt()).satisfies(receipt -> {
            assertThat(receipt.getProducerId()).isEqualTo(1);
            assertThat(receipt.getSequenceId()).isEqualTo(2);
            assertThat(receipt.getHighestSequenceId()).isEqualTo(3);
            assertThat(receipt.getMessageId().getLedgerId()).isEqualTo(4);
            assertThat(receipt.getMessageId().getEntryId()).isEqualTo(5);
        });
    }

    @Test
    public void testPartitionMetadataResponseIsNotOverwrittenByInterceptor() {
        commandSender.sendPartitionMetadataResponse(7, 42);

        BaseCommand response = readResponse();
        assertThat(interceptedTypes).containsExactly(BaseCommand.Type.PARTITIONED_METADATA_RESPONSE);
        assertThat(response.getType()).isEqualTo(BaseCommand.Type.PARTITIONED_METADATA_RESPONSE);
        assertThat(response.getPartitionMetadataResponse().getRequestId()).isEqualTo(42);
        assertThat(response.getPartitionMetadataResponse().getPartitions()).isEqualTo(7);
    }

    private BaseCommand readResponse() {
        channel.runPendingTasks();
        ByteBuf frame = channel.readOutbound();
        assertThat(frame).isNotNull();
        try {
            frame.readInt();
            int commandSize = frame.readInt();
            BaseCommand command = new BaseCommand();
            command.parseFrom(frame, commandSize);
            return command;
        } finally {
            frame.release();
        }
    }
}
