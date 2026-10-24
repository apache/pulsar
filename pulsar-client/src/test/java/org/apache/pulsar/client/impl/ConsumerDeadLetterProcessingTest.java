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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.channel.EventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.ImmediateEventExecutor;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.DeadLetterProducerBuilderCustomizer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.client.util.ScheduledExecutorProvider;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandMessage;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.ProtocolVersion;
import org.apache.pulsar.common.api.proto.SingleMessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ConsumerDeadLetterProcessingTest {
    private static final String TOPIC = "persistent://public/default/dlq-processing";
    private static final byte[] PAYLOAD = "same-original-payload".getBytes(StandardCharsets.UTF_8);
    private static final MessageIdImpl ORIGINAL_ID = new MessageIdImpl(1, 1, -1);
    private static final int MAX_REDELIVERIES = 2;

    private enum SendCompletion {
        SUCCESS,
        FAILURE,
        CANCELLATION
    }

    @DataProvider
    public Object[][] messageTypes() {
        return new Object[][] {{false}, {true}};
    }

    @DataProvider
    public Object[][] duplicateBatchMessages() {
        return new Object[][] {
                {MAX_REDELIVERIES, 1}, {MAX_REDELIVERIES, 2},
                {MAX_REDELIVERIES + 1, 1}, {MAX_REDELIVERIES + 1, 2}
        };
    }

    @DataProvider
    public Object[][] sendCompletions() {
        List<Object[]> cases = new ArrayList<>();
        for (boolean reverseOrder : new boolean[] {false, true}) {
            for (SendCompletion completion : SendCompletion.values()) {
                cases.add(new Object[] {reverseOrder, SendCompletion.SUCCESS, completion});
                if (completion != SendCompletion.SUCCESS) {
                    cases.add(new Object[] {reverseOrder, completion, SendCompletion.SUCCESS});
                }
            }
        }
        return cases.toArray(Object[][]::new);
    }

    @DataProvider
    public Object[][] partialBatchSendFailures() {
        return new Object[][] {
                {new IllegalStateException("Controlled second DLQ message construction failure")},
                {new AssertionError("Controlled second DLQ message construction error")}
        };
    }

    @DataProvider
    public Object[][] staleDeliveries() {
        return new Object[][] {{false, false}, {false, true}, {true, false}, {true, true}};
    }

    @DataProvider
    public Object[][] rejectedBatchNotifications() {
        return new Object[][] {{0}, {1}};
    }

    @Test(dataProvider = "duplicateBatchMessages", timeOut = 30000)
    public void testDuplicateBatchMessagesAreReleasedAndExcludedFromDeadLetter(int redeliveryCount,
                                                                             int acknowledgedMessages)
            throws Exception {
        try (Fixture fixture = new Fixture(true, true)) {
            ByteBuf original = fixture.deliver(0);
            for (int index = 0; index < 2; index++) {
                Message<byte[]> message = fixture.consumer.receive(1, TimeUnit.SECONDS);
                assertThat(message).isNotNull();
                try {
                    if (index < acknowledgedMessages) {
                        fixture.consumer.acknowledgeAsync(message).get(1, TimeUnit.SECONDS);
                    }
                } finally {
                    message.release();
                }
            }
            assertThat(original.refCnt()).isZero();

            ByteBuf redelivery = fixture.deliver(redeliveryCount);
            if (acknowledgedMessages == 2) {
                assertThat(fixture.consumer.getPossibleSendToDeadLetterTopicMessages()).isEmpty();
                assertThat(fixture.sends).isEmpty();
            } else {
                assertThat(fixture.consumer.getPossibleSendToDeadLetterTopicMessages().get(ORIGINAL_ID)).hasSize(1);
                if (redeliveryCount == MAX_REDELIVERIES) {
                    Message<byte[]> remaining = fixture.consumer.receive(1, TimeUnit.SECONDS);
                    assertThat(remaining).isNotNull();
                    remaining.release();
                    fixture.consumer.redeliverUnacknowledgedMessages(Set.of(ORIGINAL_ID));
                }
                assertThat(fixture.sends).hasSize(1);
                fixture.assertSentMetadata(1);
                fixture.completeSend(0, SendCompletion.SUCCESS);
                assertThat(fixture.consumer.getPossibleSendToDeadLetterTopicMessages()).isEmpty();
            }
            assertThat(fixture.consumer.numMessagesInQueue()).isZero();
            assertThat(redelivery.refCnt()).as("discarded duplicates have no application or DLQ buffer owner")
                    .isZero();
        }
    }

    @Test(dataProvider = "sendCompletions", timeOut = 30000)
    public void testBatchAcknowledgmentWaitsForEveryDeadLetterSend(boolean reverseOrder,
                                                                  SendCompletion firstCompletion,
                                                                  SendCompletion finalCompletion) throws Exception {
        try (Fixture fixture = new Fixture(true)) {
            ByteBuf frame = fixture.deliver();
            assertThat(fixture.sends).hasSize(2);
            assertThat(frame.refCnt()).as("DLQ candidates are heap copies of the released original messages")
                    .isZero();
            int first = reverseOrder ? 1 : 0;
            int last = reverseOrder ? 0 : 1;
            fixture.completeSend(first, firstCompletion);
            assertThat(fixture.sends.get(last)).as("the remaining DLQ send is still pending").isNotDone();
            assertThat(fixture.acknowledgments).as("a pending send prevents acknowledgment of the batch")
                    .isEmpty();
            assertThat(fixture.redeliveries).as("failure or cancellation must wait for the remaining submitted send")
                    .isZero();
            assertThat(fixture.consumer.getPossibleSendToDeadLetterTopicMessages()).containsKey(ORIGINAL_ID);
            fixture.assertSentMetadata(0, 1);

            fixture.completeSend(last, finalCompletion);
            if (firstCompletion == SendCompletion.SUCCESS && finalCompletion == SendCompletion.SUCCESS) {
                assertThat(fixture.acknowledgments).hasSize(1);
                assertThat(fixture.redeliveries).isZero();
                fixture.assertLastAcknowledgmentCoversEntry();
                assertThat(fixture.consumer.getPossibleSendToDeadLetterTopicMessages()).isEmpty();
            } else {
                assertThat(fixture.acknowledgments).as("failure or cancellation preserves the original batch")
                        .isEmpty();
                assertThat(fixture.redeliveries).as("retry is requested only after every submitted send completes")
                        .isEqualTo(1);
                assertThat(fixture.consumer.getPossibleSendToDeadLetterTopicMessages()).containsKey(ORIGINAL_ID);
            }
        }
    }

    @Test(timeOut = 30000)
    public void testPartialApplicationAcknowledgmentExcludesCachedBatchMessageFromDeadLetter() throws Exception {
        try (Fixture fixture = new Fixture(true)) {
            ByteBuf frame = fixture.deliver(MAX_REDELIVERIES);
            Message<byte[]> first = fixture.consumer.receive(1, TimeUnit.SECONDS);
            Message<byte[]> second = fixture.consumer.receive(1, TimeUnit.SECONDS);
            assertThat(first).isNotNull();
            assertThat(second).isNotNull();
            MessageId secondMessageId = second.getMessageId();
            try {
                fixture.consumer.acknowledgeAsync(first).get(1, TimeUnit.SECONDS);
            } finally {
                first.release();
                second.release();
            }
            assertThat(frame.refCnt()).isZero();
            assertThat(fixture.acknowledgments).hasSize(1);
            fixture.consumer.redeliverUnacknowledgedMessages(Set.of(secondMessageId));
            assertThat(fixture.sends).as("a message acknowledged after caching must not be republished to DLQ")
                    .hasSize(1);
            fixture.assertSentMetadata(1);
            fixture.completeSend(0, SendCompletion.SUCCESS);
            assertThat(fixture.acknowledgments).hasSize(2);
            fixture.assertLastAcknowledgmentCoversEntry();
            assertThat(fixture.consumer.getPossibleSendToDeadLetterTopicMessages()).isEmpty();
        }
    }

    @Test(dataProvider = "partialBatchSendFailures", timeOut = 30000)
    public void testPartialBatchConstructionFailureDoesNotAcknowledgeSource(Throwable failure) throws Exception {
        try (Fixture fixture = new Fixture(true)) {
            fixture.messageCreationFailure = failure;
            ByteBuf frame = fixture.deliver();
            assertThat(fixture.sends).as("the first send precedes failure constructing the second message")
                    .hasSize(1);
            assertThat(frame.refCnt()).isZero();
            assertThat(fixture.sends.get(0)).isNotDone();
            assertThat(fixture.acknowledgments).isEmpty();
            assertThat(fixture.redeliveries).as("construction failure must wait for the already submitted send")
                    .isZero();
            fixture.completeSend(0, SendCompletion.SUCCESS);
            assertThat(fixture.redeliveries).as("the failed batch is retried after its submitted send completes")
                    .isEqualTo(1);
            assertThat(fixture.acknowledgments).as("a partially submitted batch cannot acknowledge its source")
                    .isEmpty();
            assertThat(fixture.consumer.getPossibleSendToDeadLetterTopicMessages()).containsKey(ORIGINAL_ID);
        }
    }

    @Test(dataProvider = "messageTypes", timeOut = 30000)
    public void testAcknowledgmentFromReceiveCallbackRemovesPublishedCandidates(boolean batch) throws Exception {
        try (Fixture fixture = new Fixture(batch)) {
            List<CompletableFuture<Void>> callbacks = new ArrayList<>();
            for (int index = 0; index < (batch ? 2 : 1); index++) {
                callbacks.add(fixture.consumer.receiveAsync().thenCompose(message -> {
                    try {
                        assertThat(message.getData()).isEqualTo(PAYLOAD);
                        return fixture.consumer.acknowledgeAsync(message);
                    } finally {
                        message.release();
                    }
                }));
            }
            ByteBuf frame = fixture.deliver(MAX_REDELIVERIES);
            CompletableFuture.allOf(callbacks.toArray(CompletableFuture<?>[]::new)).get(1, TimeUnit.SECONDS);
            assertThat(fixture.sends).isEmpty();
            assertThat(fixture.acknowledgments).hasSize(batch ? 2 : 1);
            assertThat(fixture.consumer.getPossibleSendToDeadLetterTopicMessages()).isEmpty();
            assertThat(frame.refCnt()).isZero();
        }
    }

    @Test(dataProvider = "messageTypes", timeOut = 30000)
    public void testCloseFromFirstReceiveCallbackDoesNotRepublishCandidates(boolean batch) throws Exception {
        try (Fixture fixture = new Fixture(batch)) {
            CompletableFuture<Void> callback = fixture.consumer.receiveAsync().thenCompose(message -> {
                try {
                    CompletableFuture<Void> close = fixture.consumer.closeAsync();
                    assertThat(message.getData()).isEqualTo(PAYLOAD);
                    return close;
                } finally {
                    message.release();
                }
            });
            ByteBuf frame = fixture.deliver(MAX_REDELIVERIES);
            callback.get(1, TimeUnit.SECONDS);
            assertThat(fixture.sends).isEmpty();
            assertThat(fixture.acknowledgments).isEmpty();
            assertThat(fixture.consumer.getPossibleSendToDeadLetterTopicMessages()).isEmpty();
            assertThat(fixture.consumer.numMessagesInQueue()).as("close stops the remaining batch notifications")
                    .isZero();
            assertThat(frame.refCnt()).as("messages not submitted after the callback closes must be released")
                    .isZero();
        }
    }

    @Test(dataProvider = "staleDeliveries", timeOut = 30000)
    public void testStaleDeliveryDoesNotCacheDeadLetterMessages(boolean batch, boolean reconnect) throws Exception {
        try (Fixture fixture = new Fixture(batch)) {
            ClientCnx sourceConnection = fixture.consumer.getClientCnx();
            if (reconnect) {
                assertThat(fixture.consumer.changeToConnecting()).isTrue();
                fixture.consumer.connectionOpened(fixture.newConnection()).get(1, TimeUnit.SECONDS);
            } else {
                fixture.consumer.closeAsync().get(1, TimeUnit.SECONDS);
            }
            ByteBuf frame = fixture.deliver(MAX_REDELIVERIES + 1, sourceConnection);
            assertThat(fixture.consumer.getPossibleSendToDeadLetterTopicMessages()).isEmpty();
            assertThat(fixture.sends).isEmpty();
            assertThat(fixture.acknowledgments).isEmpty();
            assertThat(frame.refCnt()).as("a delivery from a stale connection must release its originals")
                    .isZero();
        }
    }

    @Test(dataProvider = "rejectedBatchNotifications", timeOut = 30000)
    public void testRejectedBatchNotificationReleasesUndeliveredMessages(int acceptedNotifications) throws Exception {
        try (Fixture fixture = new Fixture(true)) {
            fixture.queueNotifications = true;
            fixture.notificationsBeforeRejection = acceptedNotifications;
            RejectedExecutionException failure = new RejectedExecutionException("Controlled notification rejection");
            fixture.notificationRejection = failure;
            assertThatThrownBy(() -> fixture.deliver(MAX_REDELIVERIES)).isSameAs(failure);
            ByteBuf frame = fixture.frames.get(0);
            assertThat(fixture.notifications).hasSize(acceptedNotifications);
            assertThat(fixture.consumer.getPossibleSendToDeadLetterTopicMessages().get(ORIGINAL_ID)).hasSize(2);
            fixture.runNotifications();
            for (int index = 0; index < acceptedNotifications; index++) {
                Message<byte[]> message = fixture.consumer.receive(1, TimeUnit.SECONDS);
                assertThat(message).isNotNull();
                message.release();
            }
            assertThat(frame.refCnt()).as("rejected and never submitted notifications release their messages")
                    .isZero();
        }
    }

    @Test(dataProvider = "messageTypes", timeOut = 30000)
    public void testProducerInitializationFailureDoesNotEscapeSynchronously(boolean batch) throws Exception {
        AtomicInteger customizations = new AtomicInteger();
        try (Fixture fixture = new Fixture(batch, (context, builder) -> {
            customizations.incrementAndGet();
            throw new IllegalStateException("Controlled DLQ producer customization failure");
        })) {
            ByteBuf frame = fixture.deliver(MAX_REDELIVERIES);
            fixture.releaseApplicationMessages();
            assertThat(frame.refCnt()).isZero();
            assertThatCode(() -> fixture.consumer.redeliverUnacknowledgedMessages(Set.of(ORIGINAL_ID)))
                    .doesNotThrowAnyException();
            assertThat(customizations.get()).isEqualTo(1);
            assertThat(fixture.sends).isEmpty();
            assertThat(fixture.acknowledgments).isEmpty();
            assertThat(fixture.consumer.getPossibleSendToDeadLetterTopicMessages()).containsKey(ORIGINAL_ID);
        }
    }

    @Test(dataProvider = "messageTypes", timeOut = 30000)
    public void testProducerBackoffFailureCompletesProducerFuture(boolean batch) throws Exception {
        AtomicInteger customizations = new AtomicInteger();
        try (Fixture fixture = new Fixture(batch, (context, builder) -> {
            if (customizations.incrementAndGet() == 2) {
                throw new IllegalStateException("Controlled DLQ producer retry customization failure");
            }
        }, true)) {
            ByteBuf frame = fixture.deliver();
            assertThat(frame.refCnt()).isZero();
            assertThat(customizations.get()).isEqualTo(1);
            fixture.consumer.redeliverUnacknowledgedMessages(Set.of(ORIGINAL_ID));
            assertThat(fixture.scheduledTasks).hasSize(1);
            assertThatCode(() -> fixture.scheduledTasks.remove(0).run()).doesNotThrowAnyException();
            assertThat(customizations.get()).isEqualTo(2);
            assertThat(fixture.sends).isEmpty();
            assertThat(fixture.acknowledgments).isEmpty();
            Throwable closeFailure = fixture.consumer.closeAsync().handle((ignored, exception) -> exception)
                    .get(1, TimeUnit.SECONDS);
            assertThat(closeFailure).as("the failed producer may fail close, but must never leave it pending")
                    .isNotNull();
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final ExecutorService executor = MoreExecutors.newDirectExecutorService();
        private final List<CompletableFuture<MessageId>> sends = new ArrayList<>();
        private final List<TypedMessageBuilder<byte[]>> sentMessages = new ArrayList<>();
        private final List<ByteBuf> frames = new ArrayList<>();
        private final List<Runnable> scheduledTasks = new ArrayList<>();
        private final List<Runnable> notifications = new ArrayList<>();
        private final List<BaseCommand> acknowledgments = new ArrayList<>();
        private final ConsumerImpl<byte[]> consumer;
        private final boolean batch;
        private boolean queueNotifications;
        private RejectedExecutionException notificationRejection;
        private int notificationsBeforeRejection;
        private Throwable messageCreationFailure;
        private int redeliveries;

        private Fixture(boolean batch) throws Exception {
            this(batch, null, false);
        }

        private Fixture(boolean batch, boolean groupAcknowledgments) throws Exception {
            this(batch, null, false, groupAcknowledgments);
        }

        private Fixture(boolean batch, DeadLetterProducerBuilderCustomizer customizer) throws Exception {
            this(batch, customizer, false);
        }

        private Fixture(boolean batch, DeadLetterProducerBuilderCustomizer customizer, boolean failProducerCreation)
                throws Exception {
            this(batch, customizer, failProducerCreation, false);
        }

        @SuppressWarnings("unchecked")
        private Fixture(boolean batch, DeadLetterProducerBuilderCustomizer customizer, boolean failProducerCreation,
                        boolean groupAcknowledgments) throws Exception {
            this.batch = batch;
            PulsarClientImpl client = mock(PulsarClientImpl.class);
            ClientConfigurationData clientConf = new ClientConfigurationData();
            clientConf.setStatsIntervalSeconds(0);
            when(client.getConfiguration()).thenReturn(clientConf);
            when(client.getCnxPool()).thenReturn(mock(ConnectionPool.class));
            EventLoopGroup eventLoopGroup = mock(EventLoopGroup.class);
            EventLoop eventLoop = mock(EventLoop.class);
            when(eventLoopGroup.next()).thenReturn(eventLoop);
            when(client.eventLoopGroup()).thenReturn(eventLoopGroup);
            when(client.instrumentProvider()).thenReturn(InstrumentProvider.NOOP);
            ExecutorService internalExecutor = mock(ExecutorService.class);
            doAnswer(invocation -> {
                Runnable notification = invocation.getArgument(0);
                if (notificationRejection != null && notificationsBeforeRejection-- == 0) {
                    throw notificationRejection;
                }
                if (queueNotifications) {
                    notifications.add(notification);
                } else {
                    executor.execute(notification);
                }
                return null;
            }).when(internalExecutor).execute(any(Runnable.class));
            when(client.getInternalExecutorService()).thenReturn(internalExecutor);
            when(client.getConnection(anyString(), anyInt())).thenReturn(new CompletableFuture<>());
            if (failProducerCreation) {
                ScheduledExecutorProvider scheduled = mock(ScheduledExecutorProvider.class);
                ScheduledExecutorService scheduledExecutor = mock(ScheduledExecutorService.class);
                when(client.getScheduledExecutorProvider()).thenReturn(scheduled);
                when(scheduled.getExecutor(any(Object.class))).thenReturn(scheduledExecutor);
                when(scheduledExecutor.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class)))
                        .thenAnswer(invocation -> {
                            scheduledTasks.add(invocation.getArgument(0));
                            return null;
                        });
            }
            AtomicLong requests = new AtomicLong();
            when(client.newRequestId()).thenAnswer(invocation -> requests.incrementAndGet());
            ExecutorProvider external = mock(ExecutorProvider.class);
            when(external.getExecutor()).thenReturn(executor);

            Producer<byte[]> producer = mock(Producer.class);
            ProducerBuilderImpl<byte[]> producerBuilder = mock(ProducerBuilderImpl.class, RETURNS_SELF);
            doReturn(producerBuilder).when(client).newProducer(any(Schema.class));
            when(producerBuilder.createAsync()).thenReturn(failProducerCreation
                    ? CompletableFuture.failedFuture(
                            new IllegalStateException("Controlled DLQ producer creation failure"))
                    : CompletableFuture.completedFuture(producer));
            when(producer.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
            when(producer.newMessage(any(Schema.class))).thenAnswer(invocation -> {
                if (sends.size() == 1 && messageCreationFailure != null) {
                    throw messageCreationFailure;
                }
                TypedMessageBuilder<byte[]> builder = mock(TypedMessageBuilder.class, RETURNS_SELF);
                CompletableFuture<MessageId> send = new CompletableFuture<>();
                when(builder.sendAsync()).thenAnswer(sendInvocation -> {
                    sends.add(send);
                    sentMessages.add(builder);
                    return send;
                });
                return builder;
            });

            ConsumerConfigurationData<byte[]> conf = new ConsumerConfigurationData<>();
            conf.setSubscriptionName("dlq-processing-sub");
            conf.setSubscriptionType(SubscriptionType.Shared);
            conf.setBatchIndexAckEnabled(true);
            conf.setPoolMessages(true);
            conf.setAcknowledgementsGroupTimeMicros(groupAcknowledgments ? TimeUnit.HOURS.toMicros(1) : 0);
            conf.setDeadLetterPolicy(DeadLetterPolicy.builder().maxRedeliverCount(MAX_REDELIVERIES)
                    .deadLetterTopic(TOPIC + "-DLQ").deadLetterProducerBuilderCustomizer(customizer).build());
            consumer = ConsumerImpl.newConsumerImpl(client, TOPIC, conf, external, -1, false,
                    new CompletableFuture<>(), null, Schema.BYTES, null, true);
            consumer.connectionOpened(newConnection()).get(1, TimeUnit.SECONDS);
        }

        private void runNotifications() {
            notificationRejection = null;
            queueNotifications = false;
            List<Runnable> queued = List.copyOf(notifications);
            notifications.clear();
            queued.forEach(Runnable::run);
        }

        private void releaseApplicationMessages() throws Exception {
            for (int index = 0; index < (batch ? 2 : 1); index++) {
                Message<byte[]> message = consumer.receive(1, TimeUnit.SECONDS);
                assertThat(message).isNotNull();
                message.release();
            }
        }

        private void completeSend(int index, SendCompletion completion) {
            switch (completion) {
                case SUCCESS -> sends.get(index).complete(new MessageIdImpl(9, index, -1));
                case FAILURE -> sends.get(index)
                        .completeExceptionally(new IllegalStateException("Controlled DLQ send failure"));
                case CANCELLATION -> sends.get(index).cancel(false);
            }
        }

        private void assertLastAcknowledgmentCoversEntry() {
            BaseCommand acknowledgment = acknowledgments.get(acknowledgments.size() - 1);
            assertThat(acknowledgment.getAck().getMessageIdAt(0).getAckSetsCount())
                    .as("DLQ completes the whole source entry, including a batch-index redelivery request")
                    .isZero();
        }

        private ByteBuf deliver() {
            return deliver(MAX_REDELIVERIES + 1);
        }

        private ByteBuf deliver(int redeliveryCount) {
            return deliver(redeliveryCount, null);
        }

        private ByteBuf deliver(int redeliveryCount, ClientCnx decodedSourceConnection) {
            MessageMetadata metadata = new MessageMetadata().setProducerName("dlq-processing-producer")
                    .setSequenceId(1).setPublishTime(1).setPartitionKey("key-0")
                    .setOrderingKey(new byte[] {0}).setEventTime(10);
            metadata.addProperty().setKey("application-property").setValue("value-0");
            ByteBuf payload = Unpooled.buffer();
            ByteBuf encoded = null;
            ByteBuf frame = null;
            try {
                if (batch) {
                    metadata.setNumMessagesInBatch(2);
                    for (int index = 0; index < 2; index++) {
                        ByteBuf singlePayload = Unpooled.wrappedBuffer(PAYLOAD);
                        try {
                            SingleMessageMetadata singleMetadata = new SingleMessageMetadata()
                                    .setPartitionKey(index == 0 ? "key-0"
                                            : Base64.getEncoder().encodeToString(new byte[] {1}))
                                    .setPartitionKeyB64Encoded(index != 0).setOrderingKey(new byte[] {(byte) index})
                                    .setEventTime(10 + index);
                            singleMetadata.addProperty().setKey("application-property").setValue("value-" + index);
                            Commands.serializeSingleMessageInBatchWithPayload(singleMetadata, singlePayload, payload);
                        } finally {
                            singlePayload.release();
                        }
                    }
                } else {
                    payload.writeBytes(PAYLOAD);
                }
                encoded = Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, metadata, payload);
                // Keep no observation retain: after delivery only real MessageImpl owners keep this frame alive.
                frame = Unpooled.directBuffer(encoded.readableBytes())
                        .writeBytes(encoded, encoded.readerIndex(), encoded.readableBytes());
                frames.add(frame);
                CommandMessage command = new CommandMessage().setConsumerId(consumer.consumerId)
                        .setRedeliveryCount(redeliveryCount).setConsumerEpoch(consumer.getConsumerEpoch());
                command.setMessageId().setLedgerId(1).setEntryId(1);
                if (decodedSourceConnection == null || !batch) {
                    consumer.messageReceived(command, frame,
                            decodedSourceConnection == null ? consumer.getClientCnx() : decodedSourceConnection);
                } else {
                    MessageMetadata decodedMetadata = Commands.parseMessageMetadata(frame);
                    consumer.receiveIndividualMessagesFromBatch(null, decodedMetadata, redeliveryCount, null,
                            frame, command.getMessageId(), decodedSourceConnection, Commands.DEFAULT_CONSUMER_EPOCH,
                            false);
                }
                return frame;
            } finally {
                if (frame != null) {
                    frame.release();
                }
                if (encoded != null) {
                    encoded.release();
                }
                payload.release();
            }
        }

        private void assertSentMetadata(int... batchIndexes) {
            assertThat(sentMessages).hasSize(batchIndexes.length);
            for (int index = 0; index < sentMessages.size(); index++) {
                int batchIndex = batchIndexes[index];
                TypedMessageBuilder<byte[]> builder = sentMessages.get(index);
                verify(builder).value(PAYLOAD);
                verify(builder).properties(argThat(properties ->
                        ("value-" + batchIndex).equals(properties.get("application-property"))));
                if (batchIndex == 0) {
                    verify(builder).key("key-0");
                } else {
                    verify(builder).keyBytes(new byte[] {1});
                }
                verify(builder).orderingKey(new byte[] {(byte) batchIndex});
                verify(builder).eventTime(10 + batchIndex);
            }
        }

        private ClientCnx newConnection() {
            ClientCnx connection = mock(ClientCnx.class);
            ChannelHandlerContext context = mock(ChannelHandlerContext.class);
            Channel channel = mock(Channel.class);
            ChannelPromise promise = mock(ChannelPromise.class);
            when(connection.ctx()).thenReturn(context);
            when(connection.channel()).thenReturn(channel);
            when(connection.getMaxMessageSize()).thenReturn(Commands.DEFAULT_MAX_MESSAGE_SIZE);
            when(connection.getRemoteEndpointProtocolVersion()).thenReturn(ProtocolVersion.v12_VALUE);
            when(context.channel()).thenReturn(channel);
            when(context.voidPromise()).thenReturn(promise);
            doAnswer(invocation -> completeWrite(invocation.getArgument(0), channel))
                    .when(context).writeAndFlush(any(ByteBuf.class), any(ChannelPromise.class));
            doAnswer(invocation -> completeWrite(invocation.getArgument(0), channel))
                    .when(context).writeAndFlush(any(ByteBuf.class));
            doAnswer(invocation -> completeWrite(invocation.getArgument(0), channel))
                    .when(context).write(any(ByteBuf.class), any(ChannelPromise.class));
            doAnswer(invocation -> completeWrite(invocation.getArgument(0), channel))
                    .when(context).write(any(ByteBuf.class));
            when(connection.sendRequestWithId(any(ByteBuf.class), anyLong())).thenAnswer(invocation -> {
                BaseCommand command = decodeAndRelease(invocation.getArgument(0));
                assertThat(command.getType()).isIn(BaseCommand.Type.SUBSCRIBE, BaseCommand.Type.CLOSE_CONSUMER);
                return CompletableFuture.<ProducerResponse>completedFuture(null);
            });
            return connection;
        }

        private ChannelPromise completeWrite(ByteBuf frame, Channel channel) {
            BaseCommand command = decodeAndRelease(frame);
            if (command.getType() == BaseCommand.Type.ACK) {
                assertThat(command.getAck().getMessageIdsCount()).isEqualTo(1);
                assertThat(command.getAck().getMessageIdAt(0).getLedgerId()).isEqualTo(1);
                assertThat(command.getAck().getMessageIdAt(0).getEntryId()).isEqualTo(1);
                acknowledgments.add(command);
            } else if (command.getType() == BaseCommand.Type.REDELIVER_UNACKNOWLEDGED_MESSAGES) {
                assertThat(command.getRedeliverUnacknowledgedMessages().getMessageIdsCount()).isEqualTo(1);
                assertThat(command.getRedeliverUnacknowledgedMessages().getMessageIdAt(0).getLedgerId()).isEqualTo(1);
                assertThat(command.getRedeliverUnacknowledgedMessages().getMessageIdAt(0).getEntryId()).isEqualTo(1);
                redeliveries++;
            }
            return new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE).setSuccess();
        }

        private static BaseCommand decodeAndRelease(ByteBuf frame) {
            try {
                frame.skipBytes(Integer.BYTES);
                int size = (int) frame.readUnsignedInt();
                BaseCommand command = new BaseCommand();
                command.parseFrom(frame, size);
                return command;
            } finally {
                frame.release();
            }
        }

        @Override
        public void close() throws Exception {
            try {
                runNotifications();
                for (CompletableFuture<MessageId> send : sends) {
                    send.complete(new MessageIdImpl(9, 1, -1));
                }
                CompletableFuture<Void> close = consumer.closeAsync();
                close.handle((ignored, exception) -> null).get(1, TimeUnit.SECONDS);
            } finally {
                executor.shutdownNow();
                // Assertions run before cleanup. Release leaked frames when running against an unpatched consumer.
                for (ByteBuf frame : frames) {
                    if (frame.refCnt() > 0) {
                        frame.release(frame.refCnt());
                    }
                }
            }
        }
    }
}
