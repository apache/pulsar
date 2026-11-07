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
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.intercept.BrokerInterceptor;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * The broker copies dispatched messages up to {@link PulsarCommandSenderImpl#MAX_COPIED_MESSAGE_BYTES} into shared
 * buffers and writes larger ones with a reference to the entry: a consumer receives both, mixed, in order and intact,
 * also when a broker interceptor reuses the thread-local command while a message is dispatched.
 */
@Test(groups = "broker-api")
public class DispatchSmallMessagesCoalescingTest extends ProducerConsumerBase {

    private final ReentrantInterceptor interceptor = new ReentrantInterceptor();

    @Override
    @BeforeClass
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();
    }

    @Override
    @AfterClass(alwaysRun = true)
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected void customizeMainPulsarTestContextBuilder(PulsarTestContext.Builder pulsarTestContextBuilder) {
        pulsarTestContextBuilder.brokerInterceptor(interceptor);
    }

    @Test(timeOut = 60_000)
    public void testMixedSmallAndLargeMessagesArriveInOrder() throws Exception {
        // sizes around the copy limit, and a large message, so that a dispatch has copied and referenced messages
        verifyDelivery(false, new int[] {1, 100, PulsarCommandSenderImpl.MAX_COPIED_MESSAGE_BYTES - 200,
                PulsarCommandSenderImpl.MAX_COPIED_MESSAGE_BYTES + 1, 10, 50_000, 300, 1500}, 600);
    }

    @Test(timeOut = 60_000)
    public void testSmallAndLargeBatchesArriveInOrder() throws Exception {
        // batches of five: the small ones are copied, the large ones referenced
        verifyDelivery(true, new int[] {10, 100, 200, 50, 300, 5000, 20, 30, 40, 50}, 600);
    }

    @Test(timeOut = 60_000)
    public void testSmallMessagesFillMoreThanOneBufferInADispatch() throws Exception {
        // a backlog of copied messages only, which an exclusive subscription's consumer receives in reads of up to
        // 100 entries: each read is dispatched at once, and takes more than two buffers of copied messages
        int messageSize = 1500;
        int count = 300;
        String topicName = newTopicName();
        createSubscription(topicName, SubscriptionType.Exclusive);
        List<byte[]> sent = produce(topicName, false, new int[] {messageSize}, count);
        int dispatchedBefore = interceptor.dispatched.get();
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer()
                     .topic(topicName)
                     .subscriptionName("sub")
                     .subscriptionType(SubscriptionType.Exclusive)
                     .receiverQueueSize(1000)
                     .startPaused(true)
                     .subscribe()) {
            // record the writes to the consumer's connection before the consumer asks for messages
            ServerCnx cnx = (ServerCnx) pulsar.getBrokerService().getTopicReference(topicName).orElseThrow()
                    .getSubscription("sub").getConsumers().get(0).cnx();
            FlushedWrites writes = new FlushedWrites();
            cnx.ctx().pipeline().addBefore("handler", "flushedWrites", writes);
            consumer.resume();
            receiveAndVerify(consumer, sent);

            // a flush ends a dispatch: some dispatch wrote more than one buffer, and filled one before writing it
            assertThat(writes.flushes).anySatisfy(buffers -> assertThat(buffers).hasSizeGreaterThan(1));
            List<int[]> buffers = writes.flushes.stream().flatMap(List::stream).toList();
            assertThat(buffers).allSatisfy(buffer ->
                    assertThat(buffer[0]).isLessThanOrEqualTo(PulsarCommandSenderImpl.MAX_COALESCED_BYTES));
            assertThat(buffers).anySatisfy(buffer ->
                    assertThat(buffer[0]).isGreaterThan(PulsarCommandSenderImpl.MAX_COALESCED_BYTES - 2 * messageSize));
            assertThat(buffers.stream().mapToInt(buffer -> buffer[1]).sum()).isEqualTo(count);
        }
        assertThat(interceptor.dispatched.get() - dispatchedBefore).isGreaterThanOrEqualTo(count);
    }

    private void verifyDelivery(boolean batching, int[] sizes, int count) throws Exception {
        String topicName = newTopicName();
        createSubscription(topicName, SubscriptionType.Shared);
        List<byte[]> sent = produce(topicName, batching, sizes, count);
        int dispatchedBefore = interceptor.dispatched.get();
        try (Consumer<byte[]> consumer = pulsarClient.newConsumer()
                     .topic(topicName)
                     .subscriptionName("sub")
                     .subscriptionType(SubscriptionType.Shared)
                     .receiverQueueSize(1000)
                     .subscribe()) {
            receiveAndVerify(consumer, sent);
        }
        assertThat(interceptor.dispatched.get()).isGreaterThan(dispatchedBefore);
    }

    private void createSubscription(String topicName, SubscriptionType subscriptionType) throws Exception {
        // the subscription exists before the messages, and the consumer comes after them, so that the dispatches
        // read the backlog in full reads
        pulsarClient.newConsumer().topic(topicName).subscriptionName("sub")
                .subscriptionType(subscriptionType).subscribe().close();
    }

    private List<byte[]> produce(String topicName, boolean batching, int[] sizes, int count) throws Exception {
        List<byte[]> sent = new ArrayList<>(count);
        try (Producer<byte[]> producer = pulsarClient.newProducer()
                     .topic(topicName)
                     .enableBatching(batching)
                     .batchingMaxMessages(5)
                     .batchingMaxPublishDelay(1, TimeUnit.SECONDS)
                     .create()) {
            for (int i = 0; i < count; i++) {
                byte[] payload = new byte[sizes[i % sizes.length]];
                Arrays.fill(payload, (byte) i);
                payload[0] = (byte) (i >> 8);
                sent.add(payload);
                producer.sendAsync(payload);
            }
            producer.flush();
        }
        return sent;
    }

    private static void receiveAndVerify(Consumer<byte[]> consumer, List<byte[]> sent) throws Exception {
        for (int i = 0; i < sent.size(); i++) {
            Message<byte[]> message = consumer.receive(10, TimeUnit.SECONDS);
            assertThat(message).as("message %d", i).isNotNull();
            assertThat(message.getData()).as("message %d", i).isEqualTo(sent.get(i));
            consumer.acknowledge(message);
        }
    }

    /**
     * Reuses the thread-local command of {@link Commands} when a message is dispatched, as sending a message with a
     * producer of the broker's client would.
     */
    private static class ReentrantInterceptor implements BrokerInterceptor {
        final AtomicInteger dispatched = new AtomicInteger();

        @Override
        public void messageDispatched(ServerCnx cnx, org.apache.pulsar.broker.service.Consumer consumer,
                                      long ledgerId, long entryId, ByteBuf headersAndPayload) {
            Commands.newSuccessCommand(-1);
            dispatched.incrementAndGet();
        }

        @Override
        public void onPulsarCommand(BaseCommand command, ServerCnx cnx) {
        }

        @Override
        public void onConnectionClosed(ServerCnx cnx) {
        }

        @Override
        public void onWebserviceRequest(ServletRequest request) {
        }

        @Override
        public void onWebserviceResponse(ServletRequest request, ServletResponse response) {
        }

        @Override
        public void initialize(PulsarService pulsarService) {
        }

        @Override
        public void close() {
        }
    }

    /**
     * Records the size and the number of frames of each buffer of more than one frame written to a connection,
     * grouped by the flush that follows them.
     */
    private static class FlushedWrites extends ChannelOutboundHandlerAdapter {
        final List<List<int[]>> flushes = new CopyOnWriteArrayList<>();
        // accessed on the connection's event loop only
        private List<int[]> buffers = new ArrayList<>();

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
            if (msg instanceof ByteBuf buffer) {
                int frames = 0;
                for (int index = buffer.readerIndex(); index < buffer.writerIndex();
                     index += 4 + buffer.getInt(index)) {
                    frames++;
                }
                if (frames > 1) {
                    buffers.add(new int[] {buffer.readableBytes(), frames});
                }
            }
            super.write(ctx, msg, promise);
        }

        @Override
        public void flush(ChannelHandlerContext ctx) throws Exception {
            if (!buffers.isEmpty()) {
                flushes.add(buffers);
                buffers = new ArrayList<>();
            }
            super.flush(ctx);
        }
    }
}
