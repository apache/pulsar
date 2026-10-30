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
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.ProtocolVersion;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Commands.ChecksumType;
import org.mockito.MockedStatic;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class PulsarCommandSenderImplTest {
    private EmbeddedChannel channel;
    private PulsarCommandSenderImpl commandSender;
    private PulsarTestContext testContext;
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
    public void cleanup() throws Exception {
        try {
            if (channel != null) {
                channel.finishAndReleaseAll();
            }
        } finally {
            if (testContext != null) {
                testContext.close();
                testContext = null;
            }
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
    public Object[][] serializationFailureIndexes() {
        return new Object[][] {{0}, {2}};
    }

    @Test(dataProvider = "serializationFailureIndexes")
    public void testLargeMessageSerializationFailureReleasesEntries(int failureIndex) throws Exception {
        createDispatchCommandSender();
        List<EntryImpl> entries = createDispatchEntries();
        List<ByteBuf> buffers = entries.stream().map(EntryImpl::getDataBuffer).toList();
        ByteBuf failedPayload = buffers.get(failureIndex);
        AtomicReference<ByteBuf> failedHeader = new AtomicReference<>();
        OutOfMemoryError allocationFailure = new OutOfMemoryError("Cannot allocate a message buffer pair");
        // The serializer uses a static allocator and recycler, without a local allocation-failure hook.
        // Fail only the pair allocation; command serialization and all buffer/entry cleanup run normally.
        try (MockedStatic<ByteBufPair> ignored = mockStatic(ByteBufPair.class, invocation -> {
            if (invocation.getMethod().getName().equals("get") && invocation.getArgument(1) == failedPayload) {
                failedHeader.set(invocation.getArgument(0));
                throw allocationFailure;
            }
            return invocation.callRealMethod();
        })) {
            ChannelPromise writePromise = dispatch(entries);
            channel.runPendingTasks();

            assertThat(failedHeader.get()).as("real serializer reached pair allocation").isNotNull();
            assertThat(failedHeader.get().refCnt()).as("serializer releases its header").isZero();
            assertThat(writePromise.isDone()).isTrue();
            assertThat(writePromise.isSuccess()).isFalse();
            assertThat(channel.isActive()).isFalse();
            assertThat(entries).allSatisfy(entry -> assertThat(entry.refCnt()).isZero());
            // EmbeddedChannel keeps already-written messages in its outbound queue, even after close.
            channel.finishAndReleaseAll();
            assertThat(buffers).allSatisfy(buffer -> assertThat(buffer.refCnt()).isZero());
        } finally {
            releaseDispatchEntries(entries, buffers);
        }
    }

    @DataProvider
    public Object[][] closedChannel() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "closedChannel")
    public void testDispatchTransfersLargeBuffersToChannel(boolean closedChannel) throws Exception {
        createDispatchCommandSender();
        if (closedChannel) {
            channel.close();
        }
        List<EntryImpl> entries = createDispatchEntries();
        List<ByteBuf> buffers = entries.stream().map(EntryImpl::getDataBuffer).toList();
        try {
            ChannelPromise writePromise = dispatch(entries);
            channel.runPendingTasks();

            assertThat(writePromise.isDone()).isTrue();
            assertThat(writePromise.isSuccess()).isEqualTo(!closedChannel);
            assertThat(channel.isActive()).isEqualTo(!closedChannel);
            assertThat(entries).allSatisfy(entry -> assertThat(entry.refCnt()).isZero());
            for (int i = 0; i < buffers.size(); i++) {
                // Large buffers belong to the channel; small ones have already been copied and released.
                assertThat(buffers.get(i).refCnt()).isEqualTo(!closedChannel && i % 2 == 0 ? 1 : 0);
            }
            channel.finishAndReleaseAll();
            assertThat(buffers).allSatisfy(buffer -> assertThat(buffer.refCnt()).isZero());
        } finally {
            releaseDispatchEntries(entries, buffers);
        }
    }

    private void releaseDispatchEntries(List<EntryImpl> entries, List<ByteBuf> buffers) {
        channel.finishAndReleaseAll();
        // Keep a failing test from leaking its orphaned references into later tests.
        entries.forEach(entry -> {
            if (entry.refCnt() > 0) {
                entry.release(entry.refCnt());
            }
        });
        buffers.forEach(buffer -> {
            if (buffer.refCnt() > 0) {
                buffer.release(buffer.refCnt());
            }
        });
    }

    private void createDispatchCommandSender() throws Exception {
        channel.finishAndReleaseAll();
        testContext = PulsarTestContext.builderForNonStartableContext().build();
        ServerCnx cnx = new ServerCnx(testContext.getPulsarService());
        cnx.setRemoteEndpointProtocolVersion(ProtocolVersion.v21.getValue());
        channel = new EmbeddedChannel(ByteBufPair.getEncoder(false), cnx);
        commandSender = (PulsarCommandSenderImpl) cnx.getCommandSender();
    }

    private static List<EntryImpl> createDispatchEntries() {
        List<EntryImpl> entries = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            ByteBuf payload = Unpooled.buffer(i % 2 == 0 ? 4096 : 16);
            payload.writeZero(payload.capacity());
            MessageMetadata metadata = new MessageMetadata().setProducerName("producer").setSequenceId(i)
                    .setPublishTime(0);
            try {
                ByteBuf data = Commands.serializeMetadataAndPayload(ChecksumType.Crc32c, metadata, payload);
                try {
                    entries.add(EntryImpl.create(1, i, data));
                } finally {
                    data.release();
                }
            } finally {
                payload.release();
            }
        }
        return entries;
    }

    private ChannelPromise dispatch(List<EntryImpl> entries) {
        EntryBatchSizes batchSizes = EntryBatchSizes.get(entries.size());
        for (int i = 0; i < entries.size(); i++) {
            batchSizes.setBatchSize(i, 1);
        }
        return commandSender.sendMessagesToConsumer(1, "persistent://tenant/namespace/topic", null, 0,
                entries, batchSizes, null, new InMemoryRedeliveryTracker(), 0);
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
