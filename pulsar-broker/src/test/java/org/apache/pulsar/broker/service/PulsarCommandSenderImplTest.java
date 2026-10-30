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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.intercept.BrokerInterceptorWithClassLoader;
import org.apache.pulsar.broker.intercept.BrokerInterceptors;
import org.apache.pulsar.broker.intercept.CounterBrokerInterceptor;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandPartitionedTopicMetadata;
import org.apache.pulsar.common.intercept.InterceptException;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.PulsarDecoder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PulsarCommandSenderImplTest {
    private EmbeddedChannel channel;
    private ServerCnx cnx;
    private PulsarCommandSenderImpl commandSender;
    private final List<BaseCommand.Type> interceptedTypes = new ArrayList<>();

    @BeforeMethod
    public void setup() throws Exception {
        interceptedTypes.clear();
        channel = new EmbeddedChannel(new ChannelInboundHandlerAdapter());
        cnx = mock(ServerCnx.class);
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

    @DataProvider
    public Object[][] interceptorCounts() {
        return new Object[][] {{2}, {3}};
    }

    @Test(dataProvider = "interceptorCounts")
    public void testSendReceiptIsPreservedAcrossInterceptors(int interceptorCount) {
        List<BaseCommand> interceptedCommands = installReentrantInterceptors(interceptorCount);

        commandSender.sendSendReceiptResponse(1, 2, 3, 4, 5);

        assertThat(interceptedCommands).hasSize(interceptorCount);
        interceptedCommands.add(readResponse());
        assertThat(interceptedCommands).allSatisfy(command -> {
            assertThat(command.getType()).isEqualTo(BaseCommand.Type.SEND_RECEIPT);
            assertThat(command.getSendReceipt()).satisfies(receipt -> {
                assertThat(receipt.getProducerId()).isEqualTo(1);
                assertThat(receipt.getSequenceId()).isEqualTo(2);
                assertThat(receipt.getHighestSequenceId()).isEqualTo(3);
                assertThat(receipt.getMessageId().getLedgerId()).isEqualTo(4);
                assertThat(receipt.getMessageId().getEntryId()).isEqualTo(5);
            });
        });
    }

    @Test(dataProvider = "interceptorCounts")
    public void testPartitionMetadataResponseIsPreservedAcrossInterceptors(int interceptorCount) {
        List<BaseCommand> interceptedCommands = installReentrantInterceptors(interceptorCount);

        commandSender.sendPartitionMetadataResponse(7, 42);

        assertThat(interceptedCommands).hasSize(interceptorCount);
        interceptedCommands.add(readResponse());
        assertThat(interceptedCommands).allSatisfy(command -> {
            assertThat(command.getType()).isEqualTo(BaseCommand.Type.PARTITIONED_METADATA_RESPONSE);
            assertThat(command.getPartitionMetadataResponse().getRequestId()).isEqualTo(42);
            assertThat(command.getPartitionMetadataResponse().getPartitions()).isEqualTo(7);
        });
    }

    @Test
    public void testInboundCommandMutationIsPreserved() {
        List<Long> requestIds = new ArrayList<>();
        BrokerInterceptor modifier = new CounterBrokerInterceptor() {
            @Override
            public void onPulsarCommand(BaseCommand command, ServerCnx cnx) {
                command.getPartitionMetadata().setRequestId(43);
                Commands.newSuccess(Long.MAX_VALUE).release();
            }
        };
        BrokerInterceptor observer = new CounterBrokerInterceptor() {
            @Override
            public void onPulsarCommand(BaseCommand command, ServerCnx cnx) {
                requestIds.add(command.getPartitionMetadata().getRequestId());
            }
        };
        BrokerInterceptors interceptors = createInterceptors(List.of(modifier, observer));
        EmbeddedChannel decoderChannel = new EmbeddedChannel(new PulsarDecoder() {
            @Override
            protected void messageReceived(BaseCommand command) {
            }

            @Override
            protected void interceptCommand(BaseCommand command) throws InterceptException {
                interceptors.onPulsarCommand(command, cnx);
            }

            @Override
            protected void handlePartitionMetadataRequest(CommandPartitionedTopicMetadata command) {
                requestIds.add(command.getRequestId());
            }
        });
        try {
            ByteBuf frame = Commands.newPartitionMetadataRequest("persistent://public/default/test", 42, true);
            frame.readInt();
            decoderChannel.writeInbound(frame);
            assertThat(requestIds).containsExactly(43L, 43L);
        } finally {
            decoderChannel.finishAndReleaseAll();
        }
    }

    private List<BaseCommand> installReentrantInterceptors(int interceptorCount) {
        List<BaseCommand> interceptedCommands = new ArrayList<>();
        List<BrokerInterceptor> interceptors = new ArrayList<>();
        for (int i = 0; i < interceptorCount; i++) {
            BrokerInterceptor interceptor = new CounterBrokerInterceptor() {
                @Override
                public void onPulsarCommand(BaseCommand command, ServerCnx cnx) {
                    interceptedCommands.add(new BaseCommand().copyFrom(command));
                    // Build another response on this thread, reusing both BaseCommand and its nested receipt.
                    Commands.newSendReceiptCommand(101, 102, 103, 104, 105);
                }
            };
            interceptors.add(interceptor);
        }
        commandSender = new PulsarCommandSenderImpl(createInterceptors(interceptors), cnx, null);
        return interceptedCommands;
    }

    private BrokerInterceptors createInterceptors(List<BrokerInterceptor> interceptors) {
        Map<String, BrokerInterceptorWithClassLoader> wrappers = new LinkedHashMap<>();
        for (int i = 0; i < interceptors.size(); i++) {
            wrappers.put("interceptor-" + i, new BrokerInterceptorWithClassLoader(interceptors.get(i), null));
        }
        return new BrokerInterceptors(wrappers);
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
