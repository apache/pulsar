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

public class ConsumerDeadLetterMessageReleaseTest {
    private static final String TOPIC = "persistent://public/default/dlq-message-release";
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
    public Object[][] completionOrders() {
        List<Object[]> cases = new ArrayList<>();
        for (boolean batch : new boolean[] {false, true}) {
            for (boolean originalBeforeReconnect : new boolean[] {false, true}) {
                for (boolean originalSuccess : new boolean[] {false, true}) {
                    for (boolean replacementSuccess : new boolean[] {false, true}) {
                        cases.add(new Object[] {batch, originalBeforeReconnect, originalSuccess, replacementSuccess});
                    }
                }
            }
        }
        return cases.toArray(Object[][]::new);
    }

    @DataProvider
    public Object[][] pendingSendCompletions() {
        List<Object[]> cases = new ArrayList<>();
        for (boolean batch : new boolean[] {false, true}) {
            for (SendCompletion completion : SendCompletion.values()) {
                cases.add(new Object[] {batch, completion});
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
    public Object[][] epochFilteredMessageTypes() {
        return new Object[][] {
                {SubscriptionType.Exclusive, false},
                {SubscriptionType.Exclusive, true},
                {SubscriptionType.Failover, false},
                {SubscriptionType.Failover, true}
        };
    }

    @DataProvider
    public Object[][] rejectedBatchNotifications() {
        return new Object[][] {{0}, {1}};
    }

    @DataProvider
    public Object[][] staleBatchTransitions() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "messageTypes", timeOut = 30000)
    public void testAcknowledgmentPreservesApplicationMessage(boolean batch) throws Exception {
        try (Fixture fixture = new Fixture(batch)) {
            ByteBuf frame = fixture.deliver(MAX_REDELIVERIES);
            List<Message<byte[]>> received = new ArrayList<>();
            try {
                for (int index = 0; index < (batch ? 2 : 1); index++) {
                    Message<byte[]> message = fixture.consumer.receive(1, TimeUnit.SECONDS);
                    assertThat(message).isNotNull();
                    received.add(message);
                }
                assertThat(fixture.sends).isEmpty();
                assertThat(fixture.consumer.getPossibleSendToDeadLetterTopicMessages()).containsKey(ORIGINAL_ID);
                for (Message<byte[]> message : received) {
                    fixture.consumer.acknowledgeAsync(message).get(1, TimeUnit.SECONDS);
                    assertThat(message.getData()).isEqualTo(PAYLOAD);
                }
                assertThat(fixture.consumer.getPossibleSendToDeadLetterTopicMessages()).isEmpty();
                fixture.consumer.closeAsync().get(1, TimeUnit.SECONDS);
                for (Message<byte[]> message : received) {
                    assertThat(message.getData()).as("an ACK and consumer close must preserve application ownership")
                            .isEqualTo(PAYLOAD);
                }
            } finally {
                received.forEach(Message::release);
            }
            assertThat(frame.refCnt()).as("the application releases the final payload reference").isZero();
        }
    }

    @Test(dataProvider = "messageTypes", timeOut = 30000)
    public void testApplicationReleasePreservesDeadLetterCache(boolean batch) throws Exception {
        try (Fixture fixture = new Fixture(batch)) {
            ByteBuf frame = fixture.deliver(MAX_REDELIVERIES);
            for (int index = 0; index < (batch ? 2 : 1); index++) {
                Message<byte[]> message = fixture.consumer.receive(1, TimeUnit.SECONDS);
                assertThat(message).isNotNull();
                message.release();
            }
            assertThat(frame.refCnt()).as("the cache owns its payload independently of the application")
                    .isPositive();
            fixture.consumer.redeliverUnacknowledgedMessages(Set.of(ORIGINAL_ID));
            assertThat(fixture.sends).hasSize(batch ? 2 : 1);
            fixture.assertSentMetadata();
            fixture.completeDelivery(0, true);
            fixture.consumer.closeAsync().get(1, TimeUnit.SECONDS);
            assertThat(frame.refCnt()).as("ACK and send completion release both internal owners").isZero();
        }
    }

    @Test(dataProvider = "messageTypes", timeOut = 30000)
    public void testAcknowledgmentFromReceiveCallbackReleasesDeadLetterCache(boolean batch) throws Exception {
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
            // Batch index acknowledgments send an ACK for each application message.
            assertThat(fixture.acknowledgments).isEqualTo(batch ? 2 : 1);
            assertThat(fixture.consumer.getPossibleSendToDeadLetterTopicMessages()).isEmpty();
            assertThat(frame.refCnt()).as("inline ACKs must release candidates published by the receive path")
                    .isZero();
        }
    }

    @Test(dataProvider = "messageTypes", timeOut = 30000)
    public void testCloseFromReceiveCallbackReleasesDeadLetterCache(boolean batch) throws Exception {
        try (Fixture fixture = new Fixture(batch)) {
            List<CompletableFuture<Void>> callbacks = new ArrayList<>();
            int messagesPerDelivery = batch ? 2 : 1;
            for (int index = 0; index < messagesPerDelivery; index++) {
                boolean closeAfterReceive = index == messagesPerDelivery - 1;
                callbacks.add(fixture.consumer.receiveAsync().thenCompose(message -> {
                    try {
                        CompletableFuture<Void> completion = closeAfterReceive ? fixture.consumer.closeAsync()
                                : CompletableFuture.completedFuture(null);
                        assertThat(message.getData()).as("close must preserve the application's message owner")
                                .isEqualTo(PAYLOAD);
                        return completion;
                    } finally {
                        message.release();
                    }
                }));
            }

            ByteBuf frame = fixture.deliver(MAX_REDELIVERIES);
            CompletableFuture.allOf(callbacks.toArray(CompletableFuture<?>[]::new)).get(1, TimeUnit.SECONDS);
            assertThat(fixture.sends).isEmpty();
            assertThat(fixture.acknowledgments).isZero();
            assertThat(fixture.consumer.getPossibleSendToDeadLetterTopicMessages()).isEmpty();
            assertThat(frame.refCnt()).as("the receive path must not publish candidates after close cleanup")
                    .isZero();
        }
    }

    @Test(dataProvider = "completionOrders", timeOut = 30000)
    public void testDeadLetterCompletionReleasesRedeliveredMessage(boolean batch, boolean originalBeforeReconnect,
                                                                  boolean originalSuccess,
                                                                  boolean replacementSuccess) throws Exception {
        try (Fixture fixture = new Fixture(batch)) {
            ByteBuf original = fixture.deliver();
            int messagesPerDelivery = batch ? 2 : 1;
            assertThat(fixture.sends).hasSize(messagesPerDelivery);
            assertThat(original.refCnt()).as("A must remain live while its DLQ sends are pending").isPositive();
            if (originalBeforeReconnect) {
                fixture.completeDelivery(0, originalSuccess);
            }

            assertThat(fixture.consumer.changeToConnecting()).isTrue();
            fixture.consumer.connectionOpened(fixture.newConnection()).get(1, TimeUnit.SECONDS);
            ByteBuf replacement = fixture.deliver();
            assertThat(fixture.sends).hasSize(2 * messagesPerDelivery);
            assertThat(fixture.consumer.getPossibleSendToDeadLetterTopicMessages().get(ORIGINAL_ID))
                    .hasSize(messagesPerDelivery);

            if (!originalBeforeReconnect) {
                fixture.completeDelivery(0, originalSuccess);
                assertThat(fixture.consumer.getPossibleSendToDeadLetterTopicMessages().containsKey(ORIGINAL_ID))
                        .as("the old ACK removes the current cache entry only after a successful send")
                        .isEqualTo(!originalSuccess);
            }
            assertThat(replacement.refCnt()).as("B's pending DLQ operation must still own its payload")
                    .isPositive();
            fixture.assertSentMetadata();

            fixture.completeDelivery(1, replacementSuccess);
            assertThat(fixture.acknowledgments).isEqualTo((originalSuccess ? 1 : 0) + (replacementSuccess ? 1 : 0));
            fixture.consumer.closeAsync().get(1, TimeUnit.SECONDS);
            assertThat(original.refCnt()).as("A's buffer after real consumer close").isZero();
            assertThat(replacement.refCnt()).as("B's buffer after real consumer close").isZero();
        }
    }

    @Test(dataProvider = "pendingSendCompletions", timeOut = 30000)
    public void testClosePreservesPendingDeadLetterOperation(boolean batch, SendCompletion completion)
            throws Exception {
        try (Fixture fixture = new Fixture(batch)) {
            ByteBuf frame = fixture.deliver();
            int messagesPerDelivery = batch ? 2 : 1;
            assertThat(fixture.sends).hasSize(messagesPerDelivery);
            assertThat(fixture.consumer.getPossibleSendToDeadLetterTopicMessages().get(ORIGINAL_ID))
                    .hasSize(messagesPerDelivery);

            fixture.consumer.closeAsync().get(1, TimeUnit.SECONDS);
            assertThat(fixture.consumer.getPossibleSendToDeadLetterTopicMessages()).isEmpty();
            assertThat(frame.refCnt()).as("pending DLQ operations must retain their payload after close")
                    .isPositive();
            fixture.assertSentMetadata();

            fixture.completeDelivery(0, completion);
            assertThat(fixture.acknowledgments).as("a closed consumer must not acknowledge the source message")
                    .isZero();
            assertThat(frame.refCnt()).as("completion must release the final DLQ operation owner").isZero();
        }
    }

    @Test(dataProvider = "partialBatchSendFailures", timeOut = 30000)
    public void testPartialBatchSendFailurePreservesPendingSend(Throwable failure) throws Exception {
        try (Fixture fixture = new Fixture(true)) {
            fixture.messageCreationFailure = failure;
            ByteBuf frame = fixture.deliver();
            assertThat(fixture.sends).as("the first send was submitted before constructing the second message")
                    .hasSize(1);
            CompletableFuture<MessageId> firstSend = fixture.sends.get(0);
            assertThat(firstSend.isDone()).isFalse();
            fixture.consumer.closeAsync().get(1, TimeUnit.SECONDS);
            assertThat(fixture.consumer.getPossibleSendToDeadLetterTopicMessages()).isEmpty();
            assertThat(firstSend.isDone()).as("close must leave the submitted send pending").isFalse();
            assertThat(frame.refCnt()).as("construction failure must preserve the pending send owner after close")
                    .isPositive();

            firstSend.complete(new MessageIdImpl(9, 0, -1));
            assertThat(fixture.acknowledgments).as("a partially failed batch must not acknowledge the source")
                    .isZero();
            assertThat(frame.refCnt()).as("the final submitted send releases the operation's payload references")
                    .isZero();
        }
    }

    @Test(dataProvider = "messageTypes", timeOut = 30000)
    public void testProducerInitializationFailureReleasesDeadLetterOperation(boolean batch) throws Exception {
        AtomicInteger customizations = new AtomicInteger();
        IllegalStateException failure = new IllegalStateException("Controlled DLQ producer customization failure");
        try (Fixture fixture = new Fixture(batch, (context, builder) -> {
            customizations.incrementAndGet();
            throw failure;
        })) {
            ByteBuf frame = fixture.deliver(MAX_REDELIVERIES);
            for (int index = 0; index < (batch ? 2 : 1); index++) {
                Message<byte[]> message = fixture.consumer.receive(1, TimeUnit.SECONDS);
                assertThat(message).isNotNull();
                message.release();
            }

            IllegalStateException synchronousFailure = null;
            try {
                fixture.consumer.redeliverUnacknowledgedMessages(Set.of(ORIGINAL_ID));
            } catch (IllegalStateException exception) {
                // Observe the leaked operation owner even if unpatched initialization escapes synchronously.
                assertThat(exception).isSameAs(failure);
                synchronousFailure = exception;
            }
            assertThat(customizations.get()).isEqualTo(1);
            assertThat(fixture.sends).isEmpty();
            assertThat(fixture.acknowledgments).isZero();
            fixture.consumer.closeAsync().get(1, TimeUnit.SECONDS);
            assertThat(frame.refCnt()).as("producer initialization failure must release the DLQ operation owner")
                    .isZero();
            assertThat(synchronousFailure).as("initialization failure must be handled by the async DLQ path").isNull();
        }
    }

    @Test(dataProvider = "epochFilteredMessageTypes", timeOut = 30000)
    public void testEpochFilteringPreservesDeadLetterCache(SubscriptionType subscriptionType, boolean batch)
            throws Exception {
        try (Fixture fixture = new Fixture(batch, subscriptionType)) {
            long deliveryEpoch = fixture.consumer.getConsumerEpoch();
            fixture.queueNotifications = true;
            ByteBuf frame = fixture.deliver(MAX_REDELIVERIES);
            int messageCount = batch ? 2 : 1;
            assertThat(fixture.notifications).hasSize(messageCount);
            List<MessageImpl<byte[]>> cachedMessages =
                    fixture.consumer.getPossibleSendToDeadLetterTopicMessages().get(ORIGINAL_ID);
            assertThat(cachedMessages).hasSize(messageCount);

            fixture.consumer.redeliverUnacknowledgedMessages();
            assertThat(fixture.consumer.getConsumerEpoch()).isEqualTo(deliveryEpoch + 1);
            fixture.runNotifications();

            assertThat(fixture.consumer.numMessagesInQueue()).as("stale callbacks must discard application messages")
                    .isZero();
            assertThat(fixture.sends).isEmpty();
            for (int index = 0; index < messageCount; index++) {
                MessageImpl<byte[]> cachedMessage = cachedMessages.get(index);
                assertThat(cachedMessage.getConsumerEpoch()).isEqualTo(deliveryEpoch);
                assertThat(cachedMessage.getData()).as("epoch filtering must preserve the independent DLQ owner")
                        .isEqualTo(PAYLOAD);
                assertThat(cachedMessage.getProperties()).containsEntry("application-property", "value-" + index);
            }
            fixture.consumer.closeAsync().get(1, TimeUnit.SECONDS);
            assertThat(fixture.consumer.getPossibleSendToDeadLetterTopicMessages()).isEmpty();
            assertThat(frame.refCnt()).as("epoch discard and close release their own references exactly once")
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

            assertThatThrownBy(() -> fixture.deliver(MAX_REDELIVERIES))
                    .isSameAs(failure);
            ByteBuf frame = fixture.frames.get(0);
            assertThat(fixture.notifications).hasSize(acceptedNotifications);
            List<MessageImpl<byte[]>> cachedMessages =
                    fixture.consumer.getPossibleSendToDeadLetterTopicMessages().get(ORIGINAL_ID);
            assertThat(cachedMessages).hasSize(2);
            for (MessageImpl<byte[]> cachedMessage : cachedMessages) {
                assertThat(cachedMessage.getData()).as("notification rejection must preserve cache ownership")
                        .isEqualTo(PAYLOAD);
            }
            fixture.runNotifications();
            assertThat(fixture.consumer.numMessagesInQueue()).isEqualTo(acceptedNotifications);
            fixture.consumer.closeAsync().get(1, TimeUnit.SECONDS);
            assertThat(frame.refCnt()).as("the rejected message and every unsubmitted message must be released")
                    .isZero();
        }
    }

    @Test(dataProvider = "staleBatchTransitions", timeOut = 30000)
    public void testStaleDecodedBatchDoesNotCacheDeadLetterMessages(boolean reconnect) throws Exception {
        try (Fixture fixture = new Fixture(true)) {
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
            assertThat(fixture.acknowledgments).isZero();
            assertThat(frame.refCnt()).as("a late decoded batch must release rejected cache ownership").isZero();
        }
    }

    @Test(dataProvider = "messageTypes", timeOut = 30000)
    public void testProducerBackoffFailureCompletesDeadLetterOperation(boolean batch) throws Exception {
        AtomicInteger customizations = new AtomicInteger();
        IllegalStateException failure =
                new IllegalStateException("Controlled DLQ producer retry customization failure");
        try (Fixture fixture = new Fixture(batch, (context, builder) -> {
            if (customizations.incrementAndGet() == 2) {
                throw failure;
            }
        }, true)) {
            ByteBuf frame = fixture.deliver();
            assertThat(customizations.get()).isEqualTo(1);
            assertThat(fixture.sends).isEmpty();
            fixture.consumer.redeliverUnacknowledgedMessages(Set.of(ORIGINAL_ID));
            assertThat(fixture.scheduledTasks).hasSize(1);

            IllegalStateException synchronousFailure = null;
            try {
                fixture.scheduledTasks.remove(0).run();
            } catch (IllegalStateException exception) {
                assertThat(exception).isSameAs(failure);
                synchronousFailure = exception;
            }
            assertThat(customizations.get()).isEqualTo(2);
            assertThat(fixture.scheduledTasks).isEmpty();
            assertThat(fixture.sends).isEmpty();
            assertThat(fixture.acknowledgments).isZero();
            CompletableFuture<Void> close = fixture.consumer.closeAsync();
            Throwable closeFailure = close.handle((ignored, exception) -> exception).get(1, TimeUnit.SECONDS);
            assertThat(closeFailure).as("the failed producer may make close exceptional, but never pending")
                    .isNotNull();
            assertThat(frame.refCnt()).as("a producer backoff failure must release its operation owner").isZero();
            assertThat(synchronousFailure).as("the scheduled callback must complete its producer future").isNull();
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final ExecutorService executor = MoreExecutors.newDirectExecutorService();
        private final List<CompletableFuture<MessageId>> sends = new ArrayList<>();
        private final List<TypedMessageBuilder<byte[]>> sentMessages = new ArrayList<>();
        private final List<ByteBuf> frames = new ArrayList<>();
        private final List<Runnable> scheduledTasks = new ArrayList<>();
        private final List<Runnable> notifications = new ArrayList<>();
        private final ConsumerImpl<byte[]> consumer;
        private final boolean batch;
        private final boolean failProducerCreation;
        private boolean queueNotifications;
        private RejectedExecutionException notificationRejection;
        private int notificationsBeforeRejection;
        private Throwable messageCreationFailure;
        private int acknowledgments;

        private Fixture(boolean batch) throws Exception {
            this(batch, null, false);
        }

        private Fixture(boolean batch, DeadLetterProducerBuilderCustomizer customizer) throws Exception {
            this(batch, customizer, false);
        }

        private Fixture(boolean batch, SubscriptionType subscriptionType) throws Exception {
            this(batch, null, false, subscriptionType);
        }

        private Fixture(boolean batch, DeadLetterProducerBuilderCustomizer customizer, boolean failProducerCreation)
                throws Exception {
            this(batch, customizer, failProducerCreation, SubscriptionType.Shared);
        }

        @SuppressWarnings("unchecked")
        private Fixture(boolean batch, DeadLetterProducerBuilderCustomizer customizer, boolean failProducerCreation,
                        SubscriptionType subscriptionType) throws Exception {
            this.batch = batch;
            this.failProducerCreation = failProducerCreation;
            PulsarClientImpl client = mock(PulsarClientImpl.class);
            ClientConfigurationData clientConf = new ClientConfigurationData();
            clientConf.setStatsIntervalSeconds(0);
            when(client.getConfiguration()).thenReturn(clientConf);
            when(client.getCnxPool()).thenReturn(mock(ConnectionPool.class));
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
            conf.setSubscriptionName("dlq-message-release-sub");
            conf.setSubscriptionType(subscriptionType);
            conf.setPoolMessages(true);
            conf.setAcknowledgementsGroupTimeMicros(0);
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

        private void completeDelivery(int delivery, boolean success) {
            completeDelivery(delivery, success ? SendCompletion.SUCCESS : SendCompletion.FAILURE);
        }

        private void completeDelivery(int delivery, SendCompletion completion) {
            int size = batch ? 2 : 1;
            for (int index = delivery * size; index < (delivery + 1) * size; index++) {
                switch (completion) {
                    case SUCCESS -> sends.get(index).complete(new MessageIdImpl(9, index, -1));
                    case FAILURE -> sends.get(index)
                            .completeExceptionally(new IllegalStateException("Controlled DLQ send failure"));
                    case CANCELLATION -> sends.get(index).cancel(false);
                }
            }
        }

        private ByteBuf deliver() {
            return deliver(MAX_REDELIVERIES + 1);
        }

        private ByteBuf deliver(int redeliveryCount) {
            return deliver(redeliveryCount, null);
        }

        private ByteBuf deliver(int redeliveryCount, ClientCnx decodedSourceConnection) {
            MessageMetadata metadata = new MessageMetadata().setProducerName("dlq-message-release-producer")
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
                if (decodedSourceConnection == null) {
                    consumer.messageReceived(command, frame, consumer.getClientCnx());
                } else {
                    assertThat(batch).isTrue();
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

        private void assertSentMetadata() {
            for (int index = 0; index < sentMessages.size(); index++) {
                int batchIndex = batch ? index % 2 : 0;
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
            doAnswer(invocation -> {
                BaseCommand command = decodeAndRelease(invocation.getArgument(0));
                if (command.getType() == BaseCommand.Type.ACK) {
                    assertThat(command.getAck().getMessageIdsCount()).isEqualTo(1);
                    assertThat(command.getAck().getMessageIdAt(0).getLedgerId()).isEqualTo(1);
                    assertThat(command.getAck().getMessageIdAt(0).getEntryId()).isEqualTo(1);
                    acknowledgments++;
                }
                return new DefaultChannelPromise(channel, ImmediateEventExecutor.INSTANCE).setSuccess();
            }).when(context).writeAndFlush(any(ByteBuf.class), any(ChannelPromise.class));
            when(connection.sendRequestWithId(any(ByteBuf.class), anyLong())).thenAnswer(invocation -> {
                BaseCommand command = decodeAndRelease(invocation.getArgument(0));
                assertThat(command.getType()).isIn(BaseCommand.Type.SUBSCRIBE, BaseCommand.Type.CLOSE_CONSUMER);
                return CompletableFuture.<ProducerResponse>completedFuture(null);
            });
            return connection;
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
                if (failProducerCreation) {
                    close.handle((ignored, exception) -> null).get(1, TimeUnit.SECONDS);
                } else {
                    close.get(1, TimeUnit.SECONDS);
                }
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
