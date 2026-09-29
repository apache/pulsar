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
package org.apache.pulsar.client.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultSelectStrategyFactory;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.resolver.AddressResolver;
import io.netty.resolver.dns.DnsAddressResolverGroup;
import io.netty.resolver.dns.DnsNameResolverBuilder;
import io.netty.util.concurrent.DefaultEventExecutorChooserFactory;
import io.netty.util.concurrent.RejectedExecutionHandlers;
import java.net.InetSocketAddress;
import java.nio.channels.spi.SelectorProvider;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import lombok.CustomLog;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.interceptor.ProducerInterceptor;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.policies.data.SchemaCompatibilityStrategy;
import org.apache.pulsar.common.protocol.ByteBufPair;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.netty.EventLoopUtil;
import org.awaitility.Awaitility;
import org.awaitility.reflect.WhiteboxImpl;
import org.mockito.MockedStatic;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@CustomLog
@Test(groups = "broker-api")
public class ProducerMemoryLeakTest extends SharedPulsarBaseTest {

    @DataProvider
    public Object[][] pendingMessageAfterSchemaFailure() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "pendingMessageAfterSchemaFailure")
    public void testSchemaFailureCallbackStopsRecovery(boolean enqueueFollowingMessage) throws Exception {
        admin.namespaces().setSchemaCompatibilityStrategy(getNamespace(),
                SchemaCompatibilityStrategy.ALWAYS_INCOMPATIBLE);
        try (PulsarClientImpl client = (PulsarClientImpl) newPulsarClient();
             ProducerImpl<String> producer = (ProducerImpl<String>) client.newProducer(Schema.STRING)
                     .topic(newTopicName()).enableBatching(false).maxPendingMessages(10).create()) {
            producer.send("initial");
            ClientCnx cnx = producer.getClientCnx();
            AtomicInteger writesAfterClose = new AtomicInteger();
            AtomicBoolean closeStarted = new AtomicBoolean();
            AtomicReference<CompletableFuture<Void>> closeFuture = new AtomicReference<>();
            cnx.channel().eventLoop().submit(() -> cnx.channel().pipeline().addBefore(cnx.ctx().name(),
                    "count-sends-after-close", new ChannelOutboundHandlerAdapter() {
                        @Override
                        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
                                throws Exception {
                            if (closeStarted.get() && msg instanceof ByteBufPair) {
                                writesAfterClose.incrementAndGet();
                            }
                            super.write(ctx, msg, promise);
                        }
                    })).get(10, TimeUnit.SECONDS);
            cnx.channel().config().setAutoRead(false);
            MsgPayloadTouchableMessageBuilder<Boolean> incompatible =
                    new MsgPayloadTouchableMessageBuilder<>(producer, Schema.BOOL);
            MsgPayloadTouchableMessageBuilder<String> following = newMessage(producer);
            try {
                CompletableFuture<MessageId> failedSend = incompatible.value(true).sendAsync();
                CompletableFuture<MessageId> completion = failedSend.whenComplete((id, error) -> {
                    closeStarted.set(true);
                    closeFuture.set(producer.closeAsync());
                });
                CompletableFuture<MessageId> followingSend = enqueueFollowingMessage
                        ? following.value("must not be sent after close").sendAsync() : null;
                assertEquals(producer.getPendingQueueSize(), enqueueFollowingMessage ? 2 : 1);
                cnx.channel().config().setAutoRead(true);
                try {
                    completion.get(10, TimeUnit.SECONDS);
                    fail("Expected the broker to reject the incompatible schema");
                } catch (ExecutionException error) {
                    assertTrue(error.getCause() instanceof PulsarClientException.IncompatibleSchemaException);
                }
                closeFuture.get().get(10, TimeUnit.SECONDS);
                cnx.channel().eventLoop().submit(() -> { }).get(10, TimeUnit.SECONDS);
                assertEquals(writesAfterClose.get(), 0, "Recovery must stop after the callback closes the producer");
                if (followingSend != null) {
                    assertTrue(followingSend.isCompletedExceptionally());
                }
                Awaitility.await().untilAsserted(() -> {
                    assertFalse(cnx.channel().isActive());
                    assertEquals(producer.getPendingQueueSize(), 0);
                    assertEquals(producer.availableSendPermitsForTesting(), 10);
                    assertEquals(client.getMemoryLimitController().currentUsage(), 0L);
                    assertEquals(incompatible.payload.refCnt(), 1);
                    if (enqueueFollowingMessage) {
                        assertEquals(following.payload.refCnt(), 1);
                    }
                });
            } finally {
                cnx.channel().config().setAutoRead(true);
                incompatible.release();
                if (following.payload != null) {
                    following.release();
                }
            }
        }
    }

    @Test
    public void testDeferredOversizedMessageClosesProducerInCallback() throws Exception {
        try (PulsarClientImpl client = (PulsarClientImpl) newPulsarClient()) {
            String topic = newTopicName();
            ProducerBuilderImpl<byte[]> builder = (ProducerBuilderImpl<byte[]>) client.newProducer()
                    .topic(topic).enableBatching(false).maxPendingMessages(10);
            CompletableFuture<Producer<byte[]>> created = new CompletableFuture<>();
            AtomicInteger callbacks = new AtomicInteger();
            try (ProducerImpl<byte[]> producer = new ProducerImpl<>(client, topic, builder.getConf(), created,
                    -1, Schema.BYTES, null, Optional.empty()) {
                @Override
                protected ByteBufPair sendMessage(long producerId, long sequenceId, int numMessages,
                                                 MessageId messageId, MessageMetadata metadata, ByteBuf payload) {
                    ByteBufPair cmd = super.sendMessage(producerId, sequenceId, numMessages,
                            messageId, metadata, payload);
                    // Put the real deferred-schema recovery in the reconnect window before its size check.
                    ClientCnx cnx = getClientCnx();
                    connectionClosed(cnx, Optional.of(60_000L), Optional.empty());
                    cnx.channel().close();
                    return cmd;
                }

                @Override
                protected void onSendAcknowledgement(Message<?> message, MessageId messageId, Throwable error) {
                    callbacks.incrementAndGet();
                    super.onSendAcknowledgement(message, messageId, error);
                }
            }) {
                created.get(10, TimeUnit.SECONDS);
                producer.getConnectionHandler().setMaxMessageSize(1024);
                ClientCnx cnx = producer.getClientCnx();
                cnx.channel().config().setAutoRead(false);
                MsgPayloadTouchableMessageBuilder<String> message =
                        new MsgPayloadTouchableMessageBuilder<>(producer, Schema.STRING);
                try {
                    CompletableFuture<MessageId> send = message.value("x".repeat(800))
                            .property("large-metadata", "y".repeat(400)).sendAsync();
                    CompletableFuture<MessageId> completion = send.whenComplete((id, error) -> producer.closeAsync());
                    assertEquals(producer.getPendingQueueSize(), 1);
                    cnx.channel().config().setAutoRead(true);
                    try {
                        completion.get(10, TimeUnit.SECONDS);
                        fail("Expected the deferred command to exceed the message size limit");
                    } catch (ExecutionException error) {
                        assertTrue(error.getCause() instanceof PulsarClientException.InvalidMessageException);
                    }
                    // A barrier also waits for recovery cleanup after the callback has completed the future.
                    cnx.channel().eventLoop().submit(() -> { }).get(10, TimeUnit.SECONDS);
                    assertEquals(callbacks.get(), 1);
                    assertEquals(producer.getPendingQueueSize(), 0);
                    assertEquals(producer.availableSendPermitsForTesting(), 10);
                    assertEquals(client.getMemoryLimitController().currentUsage(), 0L);
                    assertEquals(message.payload.refCnt(), 1);
                } finally {
                    cnx.channel().config().setAutoRead(true);
                    if (message.payload.refCnt() > 0) {
                        message.release();
                    }
                }
            }
        }
    }

    @DataProvider
    public Object[][] chunkWriteFailures() {
        return new Object[][] {{false, 0}, {false, 1}, {false, 2}, {true, 0}, {true, 1}, {true, 2}};
    }

    @Test(dataProvider = "chunkWriteFailures")
    public void testChunkedSendStopsAfterRejectedWrite(boolean blockIfQueueFull, int rejectedChunk) throws Exception {
        AtomicReference<Thread> rejectNextTaskFrom = new AtomicReference<>();
        // Use a real socket/event loop. Make its task queue reject one write submission from the sender;
        // rejecting by thread keeps broker responses and cleanup tasks running normally.
        NioEventLoopGroup eventLoops = new NioEventLoopGroup(1, (Executor) null,
                DefaultEventExecutorChooserFactory.INSTANCE, SelectorProvider.provider(),
                DefaultSelectStrategyFactory.INSTANCE, RejectedExecutionHandlers.reject(),
                capacity -> new ConcurrentLinkedQueue<>() {
                    @Override
                    public boolean offer(Runnable task) {
                        if (rejectNextTaskFrom.compareAndSet(Thread.currentThread(), null)) {
                            return false;
                        }
                        return super.offer(task);
                    }
                });
        ClientConfigurationData configuration = new ClientConfigurationData();
        configuration.setServiceUrl(getBrokerServiceUrl());
        // The default DNS resolver picks its channel type from the platform (epoll on Linux), which does not
        // match the NIO event loop above and breaks the client's shutdown. Use a resolver whose channels match.
        DnsResolverGroupImpl dnsResolverGroup = new DnsResolverGroupImpl(configuration) {
            private final DnsAddressResolverGroup nioResolverGroup = new DnsAddressResolverGroup(
                    new DnsNameResolverBuilder()
                            .traceEnabled(true)
                            .channelType(EventLoopUtil.getDatagramChannelClass(eventLoops))
                            .socketChannelType(EventLoopUtil.getClientSocketChannelClass(eventLoops), true));

            @Override
            public AddressResolver<InetSocketAddress> createAddressResolver(EventLoopGroup eventLoopGroup) {
                return nioResolverGroup.getResolver(eventLoopGroup.next());
            }

            @Override
            public void close() {
                nioResolverGroup.close();
            }
        };
        try (PulsarClientImpl client = PulsarClientImpl.builder()
                .conf(configuration)
                .eventLoopGroup(eventLoops)
                .dnsResolverGroup(dnsResolverGroup)
                .build()) {
            String topic = newTopicName();
            ProducerBuilderImpl<byte[]> builder = (ProducerBuilderImpl<byte[]>) client.newProducer()
                    .topic(topic).enableBatching(false).enableChunking(true).chunkMaxMessageSize(100)
                    .maxPendingMessages(10).blockIfQueueFull(blockIfQueueFull);
            CompletableFuture<Producer<byte[]>> created = new CompletableFuture<>();
            AtomicInteger builtChunks = new AtomicInteger();
            AtomicInteger callbacks = new AtomicInteger();
            AtomicReference<ProducerImpl.ChunkedMessageCtx> context = new AtomicReference<>();
            try (ProducerImpl<byte[]> producer = new ProducerImpl<>(client, topic, builder.getConf(), created,
                    -1, Schema.BYTES, null, Optional.empty()) {
                @Override
                protected void processOpSendMsg(OpSendMsg op) {
                    builtChunks.incrementAndGet();
                    context.set(op.chunkedMessageCtx);
                    if (op.chunkId == rejectedChunk) {
                        rejectNextTaskFrom.set(Thread.currentThread());
                    }
                    super.processOpSendMsg(op);
                }

                @Override
                protected void onSendAcknowledgement(Message<?> message, MessageId messageId, Throwable error) {
                    callbacks.incrementAndGet();
                    super.onSendAcknowledgement(message, messageId, error);
                }
            }) {
                created.get(10, TimeUnit.SECONDS);
                ClientCnx cnx = producer.getClientCnx();
                cnx.channel().config().setAutoRead(false);
                MsgPayloadTouchableMessageBuilder<byte[]> message = newMessage(producer);
                try {
                    CompletableFuture<MessageId> send = message.value(new byte[250]).sendAsync();
                    try {
                        send.get(10, TimeUnit.SECONDS);
                        fail("Expected the chunk write submission to fail");
                    } catch (ExecutionException expected) {
                        assertTrue(expected.getCause().getCause() instanceof RejectedExecutionException);
                    }
                    assertEquals(builtChunks.get(), rejectedChunk + 1);
                    assertEquals(callbacks.get(), 1);
                    assertEquals(producer.getPendingQueueSize(), rejectedChunk);
                    assertEquals(producer.availableSendPermitsForTesting(), 10 - rejectedChunk);
                    assertEquals(client.getMemoryLimitController().currentUsage(), 0L);
                    cnx.channel().config().setAutoRead(true);
                    producer.close();
                    Awaitility.await().untilAsserted(() -> {
                        assertEquals(context.get().refCnt(), 0);
                        assertEquals(message.payload.refCnt(), 1);
                    });
                    assertEquals(producer.getPendingQueueSize(), 0);
                    assertEquals(producer.availableSendPermitsForTesting(), 10);
                    assertEquals(client.getMemoryLimitController().currentUsage(), 0L);
                    assertEquals(callbacks.get(), 1);
                } finally {
                    rejectNextTaskFrom.set(null);
                    cnx.channel().config().setAutoRead(true);
                    message.release();
                }
            }
        } finally {
            dnsResolverGroup.close();
            eventLoops.shutdownGracefully(0, 0, TimeUnit.SECONDS).sync();
        }
    }

    @Test
    public void testInterruptedChunkedSendReleasesUnbuiltChunks() throws Exception {
        try (PulsarClientImpl client = (PulsarClientImpl) newPulsarClient()) {
            String topic = newTopicName();
            ProducerBuilderImpl<byte[]> builder = (ProducerBuilderImpl<byte[]>) client.newProducer()
                    .topic(topic).enableBatching(false).enableChunking(true).chunkMaxMessageSize(100)
                    .maxPendingMessages(1).blockIfQueueFull(true);
            CompletableFuture<Producer<byte[]>> created = new CompletableFuture<>();
            CountDownLatch firstChunkQueued = new CountDownLatch(1);
            AtomicReference<ProducerImpl.ChunkedMessageCtx> context = new AtomicReference<>();
            try (ProducerImpl<byte[]> producer = new ProducerImpl<>(client, topic, builder.getConf(), created,
                    -1, Schema.BYTES, null, Optional.empty()) {
                @Override
                protected void processOpSendMsg(OpSendMsg op) {
                    context.set(op.chunkedMessageCtx);
                    super.processOpSendMsg(op);
                    firstChunkQueued.countDown();
                }
            }) {
                created.get(10, TimeUnit.SECONDS);
                ClientCnx cnx = producer.getClientCnx();
                cnx.channel().config().setAutoRead(false);
                MsgPayloadTouchableMessageBuilder<byte[]> message = newMessage(producer);
                CompletableFuture<MessageId> sendResult = new CompletableFuture<>();
                AtomicBoolean interruptPreserved = new AtomicBoolean();
                Thread sender = new Thread(() -> {
                    try {
                        CompletableFuture<MessageId> send = message.value(new byte[250]).sendAsync();
                        interruptPreserved.set(Thread.currentThread().isInterrupted());
                        send.whenComplete((id, error) -> {
                            if (error != null) {
                                sendResult.completeExceptionally(error);
                            } else {
                                sendResult.complete(id);
                            }
                        });
                    } catch (Throwable error) {
                        sendResult.completeExceptionally(error);
                    }
                }, "interrupted-chunk-sender");
                try {
                    sender.start();
                    assertTrue(firstChunkQueued.await(10, TimeUnit.SECONDS));
                    Awaitility.await().until(() -> sender.getState() == Thread.State.WAITING);
                    sender.interrupt();
                    try {
                        sendResult.get(10, TimeUnit.SECONDS);
                        fail("Expected interruption while acquiring the second chunk's permit");
                    } catch (ExecutionException expected) {
                        assertTrue(expected.getCause().getCause() instanceof InterruptedException);
                    }
                    assertTrue(interruptPreserved.get());
                    cnx.channel().config().setAutoRead(true);
                    producer.close();
                    cnx.channel().eventLoop().submit(() -> { }).get(10, TimeUnit.SECONDS);
                    assertEquals(context.get().refCnt(), 0);
                    assertEquals(producer.availableSendPermitsForTesting(), 1);
                    assertEquals(client.getMemoryLimitController().currentUsage(), 0L);
                    assertEquals(message.payload.refCnt(), 1);
                } finally {
                    sender.interrupt();
                    sender.join(10_000);
                    assertFalse(sender.isAlive());
                    cnx.channel().config().setAutoRead(true);
                    if (message.payload != null) {
                        message.release();
                    }
                }
            }
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSendQueueIsFull() throws Exception {
        final String topicName = newTopicName();
        admin.topics().createNonPartitionedTopic(topicName);
        ProducerImpl<String> producer = (ProducerImpl<String>) pulsarClient.newProducer(Schema.STRING)
                .blockIfQueueFull(false).maxPendingMessages(1)
                .enableBatching(true).topic(topicName).create();
        List<MsgPayloadTouchableMessageBuilder<String>> msgBuilderList = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            msgBuilderList.add(newMessage(producer));
        }

        CompletableFuture latestSendFuture = null;
        for (MsgPayloadTouchableMessageBuilder<String> msgBuilder: msgBuilderList) {
            latestSendFuture = msgBuilder.value("msg-1").sendAsync();
        }
        try {
            latestSendFuture.join();
        } catch (Exception ex) {
            // Ignore the error PulsarClientException$ProducerQueueIsFullError.
            assertTrue(FutureUtil.unwrapCompletionException(ex)
                    instanceof PulsarClientException.ProducerQueueIsFullError);
        }

        // Verify: ref is expected.
        producer.close();
        for (int i = 0; i < msgBuilderList.size(); i++) {
            MsgPayloadTouchableMessageBuilder<String> msgBuilder = msgBuilderList.get(i);
            assertEquals(msgBuilder.payload.refCnt(), 1);
            msgBuilder.release();
            assertEquals(msgBuilder.payload.refCnt(), 0);
        }
        admin.topics().delete(topicName);
    }

    /**
     * The content size of msg(value is "msg-1") will be "5".
     * Then provides two param: 1 and 5.
     *   1: reach the limitation before adding the message metadata.
     *   2: reach the limitation after adding the message metadata.
     */
    @DataProvider(name = "maxMessageSizeAndCompressions")
    public Object[][] maxMessageSizeAndCompressions(){
        return new Object[][] {
                {1, CompressionType.NONE},
                {5, CompressionType.NONE},
                {1, CompressionType.LZ4},
                {6, CompressionType.LZ4}
        };
    }

    @Test(dataProvider = "maxMessageSizeAndCompressions")
    @SuppressWarnings("unchecked")
    public void testSendMessageSizeExceeded(int maxMessageSize, CompressionType compressionType) throws Exception {
        final String topicName = newTopicName();
        admin.topics().createNonPartitionedTopic(topicName);
        ProducerImpl<String> producer = (ProducerImpl<String>) pulsarClient.newProducer(Schema.STRING).topic(topicName)
                .compressionType(compressionType)
                .enableBatching(false)
                .create();
        producer.getConfiguration().setCompressMinMsgBodySize(1);
        producer.getConnectionHandler().setMaxMessageSize(maxMessageSize);
        MsgPayloadTouchableMessageBuilder<String> msgBuilder = newMessage(producer);
        /**
         * Mock an error: reached max message size, see more details {@link #maxMessageSizeAndCompressions()}.
         */
        try (MockedStatic<ByteBufPair> theMock = mockStatic(ByteBufPair.class)) {
            List<ByteBufPair> generatedByteBufPairs = Collections.synchronizedList(new ArrayList<>());
            theMock.when(() -> ByteBufPair.get(any(ByteBuf.class), any(ByteBuf.class))).then(invocation -> {
                ByteBufPair byteBufPair = (ByteBufPair) invocation.callRealMethod();
                generatedByteBufPairs.add(byteBufPair);
                byteBufPair.retain();
                return byteBufPair;
            });
            try {
                msgBuilder.value("msg-1").send();
                fail("expected an error that reached the max message size");
            } catch (Exception ex) {
                assertTrue(FutureUtil.unwrapCompletionException(ex)
                        instanceof PulsarClientException.InvalidMessageException);
            }

            // Verify: message payload has been released.
            // Since "MsgPayloadTouchableMessageBuilder" has called "buffer.retain" once, "refCnt()" should be "1".
            producer.close();
            Awaitility.await().untilAsserted(() -> {
                assertEquals(producer.getPendingQueueSize(), 0);
            });
            // Verify: ByteBufPair generated for Pulsar Command.
            if (maxMessageSize == 1) {
                assertEquals(generatedByteBufPairs.size(), 0);
            } else {
                assertEquals(generatedByteBufPairs.size(), 1);
                if (compressionType == CompressionType.NONE) {
                    assertEquals(msgBuilder.payload.refCnt(), 2);
                } else {
                    assertEquals(msgBuilder.payload.refCnt(), 1);
                }
                for (ByteBufPair byteBufPair : generatedByteBufPairs) {
                    assertEquals(byteBufPair.refCnt(), 1);
                    byteBufPair.release();
                    assertEquals(byteBufPair.refCnt(), 0);
                }
            }
            // Verify: message.payload
            assertEquals(msgBuilder.payload.refCnt(), 1);
            msgBuilder.release();
            assertEquals(msgBuilder.payload.refCnt(), 0);
        }

        // cleanup.
        assertEquals(msgBuilder.payload.refCnt(), 0);
        admin.topics().delete(topicName);
    }

    /**
     * The content size of msg(value is "msg-1") will be "5".
     * Then provides two param: 1 and 5.
     *   1: Less than the limitation when adding the message into the batch-container.
     *   3: Less than the limitation when building batched messages payload.
     *   2: Equals the limitation when building batched messages payload.
     */
    @DataProvider(name = "maxMessageSizes")
    public Object[][] maxMessageSizes(){
        return new Object[][] {
                {1},
                {3},
                {26}
        };
    }

    @Test(dataProvider = "maxMessageSizes")
    @SuppressWarnings("unchecked")
    public void testBatchedSendMessageSizeExceeded(int maxMessageSize) throws Exception {
        final String topicName = newTopicName();
        admin.topics().createNonPartitionedTopic(topicName);
        ProducerImpl<String> producer = (ProducerImpl<String>) pulsarClient.newProducer(Schema.STRING).topic(topicName)
                .enableBatching(true)
                .compressionType(CompressionType.NONE)
                .create();
        final ClientCnx cnx = producer.getClientCnx();
        producer.getConnectionHandler().setMaxMessageSize(maxMessageSize);
        MsgPayloadTouchableMessageBuilder<String> msgBuilder1 = newMessage(producer);
        MsgPayloadTouchableMessageBuilder<String> msgBuilder2 = newMessage(producer);
        /**
         * Mock an error: reached max message size. see more detail {@link #maxMessageSizes()}.
         */
        msgBuilder1.value("msg-1").sendAsync();
        try {
            msgBuilder2.value("msg-1").send();
            if (maxMessageSize != 26) {
                fail("expected an error that reached the max message size");
            }
        } catch (Exception ex) {
            assertTrue(FutureUtil.unwrapCompletionException(ex)
                    instanceof PulsarClientException.InvalidMessageException);
        }

        // Verify: message payload has been released.
        // Since "MsgPayloadTouchableMessageBuilder" has called "buffer.retain" once, "refCnt()" should be "1".
        producer.close();
        Awaitility.await().untilAsserted(() -> {
            assertEquals(producer.getPendingQueueSize(), 0);
        });
        assertEquals(msgBuilder1.payload.refCnt(), 1);
        assertEquals(msgBuilder2.payload.refCnt(), 1);

        // cleanup.
        cnx.ctx().close();
        msgBuilder1.release();
        msgBuilder2.release();
        assertEquals(msgBuilder1.payload.refCnt(), 0);
        assertEquals(msgBuilder2.payload.refCnt(), 0);
        admin.topics().delete(topicName);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSendAfterClosedProducer() throws Exception {
        final String topicName = newTopicName();
        admin.topics().createNonPartitionedTopic(topicName);
        ProducerImpl<String> producer =
                (ProducerImpl<String>) pulsarClient.newProducer(Schema.STRING).topic(topicName).create();
        // Publish after the producer was closed.
        MsgPayloadTouchableMessageBuilder<String> msgBuilder = newMessage(producer);
        producer.close();
        try {
            msgBuilder.value("msg-1").send();
            fail("expected an error that the producer has closed");
        } catch (Exception ex) {
            assertTrue(FutureUtil.unwrapCompletionException(ex)
                    instanceof PulsarClientException.AlreadyClosedException);
        }

        // Verify: message payload has been released.
        Awaitility.await().untilAsserted(() -> {
            assertEquals(producer.getPendingQueueSize(), 0);
        });
        assertEquals(msgBuilder.payload.refCnt(), 1);

        // cleanup.
        msgBuilder.release();
        assertEquals(msgBuilder.payload.refCnt(), 0);
        admin.topics().delete(topicName);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testBrokenSchema() throws Exception {
        admin.namespaces().setSchemaCompatibilityStrategy(getNamespace(),
                SchemaCompatibilityStrategy.ALWAYS_INCOMPATIBLE);
        final String topicName = newTopicName();
        admin.topics().createNonPartitionedTopic(topicName);
        ProducerImpl producer =
                (ProducerImpl) pulsarClient.newProducer(Schema.AUTO_PRODUCE_BYTES()).topic(topicName).create();
        // Publish after the producer was closed.
        MsgPayloadTouchableMessageBuilder<String> msgBuilder1 = newMessage(producer, Schema.STRING);
        msgBuilder1.value("msg-1").send();
        MsgPayloadTouchableMessageBuilder<Boolean> msgBuilder2 = newMessage(producer, Schema.BOOL);
        try {
            msgBuilder2.value(false).send();
            fail("expected schema broken error");
        } catch (Exception ex) {
            assertTrue(FutureUtil.unwrapCompletionException(ex)
                    instanceof PulsarClientException.IncompatibleSchemaException);
        }
        MsgPayloadTouchableMessageBuilder<String> msgBuilder3 = newMessage(producer, Schema.STRING);
        msgBuilder3.value("msg-3").send();

        // Verify: message payload has been released.
        Awaitility.await().untilAsserted(() -> {
            ProducerImpl.OpSendMsgQueue pendingMessages =
                    WhiteboxImpl.getInternalState(producer, "pendingMessages");
            Queue<ProducerImpl.OpSendMsg> pendingMessagesInternal =
                    WhiteboxImpl.getInternalState(pendingMessages, "delegate");
            assertEquals(pendingMessagesInternal.size(), 0);
        });
        assertEquals(msgBuilder1.payload.refCnt(), 1);
        assertEquals(msgBuilder2.payload.refCnt(), 1);
        assertEquals(msgBuilder3.payload.refCnt(), 1);

        // cleanup.
        msgBuilder1.release();
        msgBuilder2.release();
        msgBuilder3.release();
        assertEquals(msgBuilder1.payload.refCnt(), 0);
        assertEquals(msgBuilder2.payload.refCnt(), 0);
        assertEquals(msgBuilder3.payload.refCnt(), 0);
        producer.close();
        admin.topics().delete(topicName);
    }

    @DataProvider
    public Object[][] failedInterceptAt() {
        return new Object[][]{
            {"close"},
            {"eligible"},
            {"beforeSend"},
            {"onSendAcknowledgement"},
        };
    }

    @Test(dataProvider = "failedInterceptAt")
    @SuppressWarnings("unchecked")
    public void testInterceptorError(String method) throws Exception {
        final String topicName = newTopicName();
        admin.topics().createNonPartitionedTopic(topicName);
        ProducerImpl<String> producer = (ProducerImpl<String>) pulsarClient.newProducer(Schema.STRING).topic(topicName)
                .intercept(

                new ProducerInterceptor() {
                    @Override
                    public void close() {
                        if (method.equals("close")) {
                            throw new RuntimeException("Mocked error");
                        }
                    }

                    @Override
                    public boolean eligible(Message message) {
                        if (method.equals("eligible")) {
                            throw new RuntimeException("Mocked error");
                        }
                        return false;
                    }

                    @Override
                    public Message beforeSend(Producer producer, Message message) {
                        if (method.equals("beforeSend")) {
                            throw new RuntimeException("Mocked error");
                        }
                        return message;
                    }

                    @Override
                    @SuppressWarnings("unchecked")
                    public void onSendAcknowledgement(Producer producer, Message message, MessageId msgId,
                                                      Throwable exception) {
                        if (method.equals("onSendAcknowledgement")) {
                            throw new RuntimeException("Mocked error");
                        }

                    }
                }).create();

        MsgPayloadTouchableMessageBuilder<String> msgBuilder = newMessage(producer);
        try {
            msgBuilder.value("msg-1").sendAsync().get(3, TimeUnit.SECONDS);
            // It may throw error.
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("Mocked"));
        }

        // Verify: message payload has been released.
        producer.close();
        assertEquals(msgBuilder.payload.refCnt(), 1);

        // cleanup.
        msgBuilder.release();
        assertEquals(msgBuilder.payload.refCnt(), 0);
        admin.topics().delete(topicName);
    }

    private <T> MsgPayloadTouchableMessageBuilder<T> newMessage(ProducerImpl<T> producer){
        return newMessage(producer, producer.schema);
    }

    private <T> MsgPayloadTouchableMessageBuilder<T> newMessage(ProducerImpl<T> producer, Schema<T> schema){
        return new MsgPayloadTouchableMessageBuilder<T>(producer, schema);
    }

    private static class MsgPayloadTouchableMessageBuilder<T> extends TypedMessageBuilderImpl {

        public volatile ByteBuf payload;

        @SuppressWarnings("unchecked")
        public <T> MsgPayloadTouchableMessageBuilder(ProducerBase producer, Schema<T> schema) {
            super(producer, schema);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Message<T> getMessage() {
            MessageImpl<T> msg = (MessageImpl<T>) super.getMessage();
            payload = msg.getPayload();
            // Retain the msg to avoid it be reused by other task.
            payload.retain();
            return msg;
        }

        public void release() {
            payload.release();
        }
    }
}
