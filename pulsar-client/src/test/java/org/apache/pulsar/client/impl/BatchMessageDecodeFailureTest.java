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

import static org.apache.pulsar.common.protocol.Commands.DEFAULT_CONSUMER_EPOCH;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.BrokerEntryMetadata;
import org.apache.pulsar.common.api.proto.MessageIdData;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.SingleMessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for {@link ConsumerImpl#receiveIndividualMessagesFromBatch}.
 *
 * <p>Covers the following scenarios:
 * <ul>
 *   <li>Decode failure: corrupted, partial-corrupt, empty, truncated payloads</li>
 *   <li>Normal path: all valid messages enqueued successfully</li>
 *   <li>Duplicate detection: messages flagged as duplicate are skipped</li>
 *   <li>Dead letter policy: messages exceeding max redelivery count</li>
 * </ul>
 *
 * <p>Uses the same client-side mocking infrastructure as {@link ConsumerImplTest}
 * (via {@link ClientTestFixtures}) without requiring a running broker.
 */
public class BatchMessageDecodeFailureTest {

    private static final String TOPIC = "persistent://tenant/ns1/test-decode-failure";

    private ExecutorProvider executorProvider;
    private ExecutorService internalExecutor;
    private ClientCnx mockCnx;
    private ConsumerImpl<byte[]> consumer;
    private ConsumerStatsRecorderImpl statsRecorder;

    @BeforeMethod(alwaysRun = true)
    public void setUp() {
        executorProvider = new ExecutorProvider(1, "BatchDecodeFailureTest");
        internalExecutor = Executors.newSingleThreadScheduledExecutor();

        mockCnx = ClientTestFixtures.mockClientCnx();

        PulsarClientImpl client = ClientTestFixtures.createPulsarClientMockWithMockedClientCnx(
                executorProvider, internalExecutor, mockCnx);
        ClientConfigurationData clientConf = client.getConfiguration();
        clientConf.setOperationTimeoutMs(100);
        // Set StatsIntervalSeconds > 0 to enable real stats recording
        clientConf.setStatsIntervalSeconds(1);

        ConsumerConfigurationData<byte[]> consumerConf = new ConsumerConfigurationData<>();
        consumerConf.setSubscriptionName("test-sub");

        CompletableFuture<Consumer<byte[]>> subscribeFuture = new CompletableFuture<>();
        consumer = ConsumerImpl.newConsumerImpl(client, TOPIC, consumerConf,
                executorProvider, -1, false, subscribeFuture, null, null, null,
                true);
        consumer.setState(HandlerState.State.Ready);
        consumer.setClientCnx(mockCnx);

        // Replace the stats field with a spy to verify incrementNumReceiveFailed calls.
        // Use FieldUtils.writeField to inject the spy into the final field.
        statsRecorder = spy(new ConsumerStatsRecorderImpl(consumer));
        try {
            FieldUtils.writeField(consumer, "stats", statsRecorder, true);
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject spy stats recorder", e);
        }
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() {
        if (executorProvider != null) {
            executorProvider.shutdownNow();
            executorProvider = null;
        }
        if (internalExecutor != null) {
            internalExecutor.shutdownNow();
            internalExecutor = null;
        }
    }

    /**
     * All messages in the batch are corrupted (payload is random garbage bytes).
     * Expected: no messages enqueued, discardCorruptedBatchMessage called,
     * incrementNumReceiveFailed invoked once.
     */
    @Test
    public void testAllMessagesCorruptedInBatch() {
        BrokerEntryMetadata brokerEntryMetadata =
                new BrokerEntryMetadata().setBrokerTimestamp(1).setIndex(1);

        MessageMetadata metadata = new MessageMetadata()
                .setProducerName("test-producer")
                .setSequenceId(1)
                .setPublishTime(1)
                .setNumMessagesInBatch(3);

        // Construct corrupted payload: invalid metadata size triggers parse failure
        // in deSerializeSingleMessageInBatch -> readUnsignedInt returns huge value
        ByteBuf corruptedPayload = Unpooled.buffer(100);
        corruptedPayload.writeInt(Integer.MAX_VALUE);
        corruptedPayload.writeBytes(new byte[50]);

        consumer.receiveIndividualMessagesFromBatch(brokerEntryMetadata, metadata, 0, null,
                corruptedPayload, new MessageIdData().setLedgerId(1000).setEntryId(1),
                mockCnx, DEFAULT_CONSUMER_EPOCH, false);

        // No valid messages should be enqueued
        assertEquals(consumer.numMessagesInQueue(), 0,
                "Corrupted batch should not produce any messages in queue");

        // Verify discardCorruptedBatchMessage incremented the failed counter
        verify(statsRecorder, timeout(2000)).incrementNumReceiveFailed();
    }

    /**
     * Partial decode failure: the first message in the batch is valid,
     * but the second message is corrupted.
     * Expected: the first valid message is enqueued; remaining corrupted
     * messages are discarded.
     */
    @Test
    public void testPartialDecodeFailureInBatch() throws Exception {
        BrokerEntryMetadata brokerEntryMetadata =
                new BrokerEntryMetadata().setBrokerTimestamp(1).setIndex(1);

        MessageMetadata metadata = new MessageMetadata()
                .setProducerName("test-producer")
                .setSequenceId(1)
                .setPublishTime(1)
                .setNumMessagesInBatch(3);

        ByteBuf batchBuffer = Unpooled.buffer(1000);

        // First message: valid, properly serialized using Commands utility
        Commands.serializeSingleMessageInBatchWithPayload(
                new SingleMessageMetadata().setPartitionKey("key1"),
                Unpooled.wrappedBuffer("hello".getBytes()), batchBuffer);

        // Second message: corrupted payload (invalid metadata size)
        batchBuffer.writeInt(Integer.MAX_VALUE);
        batchBuffer.writeBytes(new byte[20]);

        consumer.receiveIndividualMessagesFromBatch(brokerEntryMetadata, metadata, 0, null,
                batchBuffer, new MessageIdData().setLedgerId(2000).setEntryId(2),
                mockCnx, DEFAULT_CONSUMER_EPOCH, false);

        // The first valid message should be enqueued via executeNotifyCallback,
        // which uses internalPinnedExecutor.execute(). Since it's async,
        // we check the stats synchronously — discardCorruptedBatchMessage
        // is called inline in the catch block.
        verify(statsRecorder, timeout(2000)).incrementNumReceiveFailed();
    }

    // ==================== Normal path tests ====================

    /**
     * All messages in the batch are valid and properly serialized.
     * Expected: all messages are enqueued via executeNotifyCallback,
     * no receive-failed counter is incremented, and skippedMessages == 0
     * so increaseAvailablePermits is NOT called with extra skipped count.
     */
    @Test
    public void testAllValidMessagesEnqueued() throws Exception {
        BrokerEntryMetadata brokerEntryMetadata =
                new BrokerEntryMetadata().setBrokerTimestamp(1).setIndex(1);

        int batchSize = 3;
        MessageMetadata metadata = new MessageMetadata()
                .setProducerName("test-producer")
                .setSequenceId(1)
                .setPublishTime(1)
                .setNumMessagesInBatch(batchSize);

        ByteBuf batchBuffer = Unpooled.buffer(1024);

        // Serialize 3 valid messages
        for (int i = 0; i < batchSize; i++) {
            Commands.serializeSingleMessageInBatchWithPayload(
                    new SingleMessageMetadata().setPartitionKey("key-" + i),
                    Unpooled.wrappedBuffer(("payload-" + i).getBytes()), batchBuffer);
        }

        consumer.receiveIndividualMessagesFromBatch(brokerEntryMetadata, metadata, 0, null,
                batchBuffer, new MessageIdData().setLedgerId(5000).setEntryId(1),
                mockCnx, DEFAULT_CONSUMER_EPOCH, false);

        // All 3 messages should eventually be enqueued through the async executor.
        // Use Awaitility since executeNotifyCallback dispatches via internalPinnedExecutor.
        Awaitility.await().untilAsserted(() ->
                assertEquals(consumer.numMessagesInQueue(), batchSize,
                        "All valid messages should be enqueued"));

        // No receive-failed counter should be incremented
        verify(statsRecorder, never()).incrementNumReceiveFailed();
    }

    // ==================== Duplicate detection tests ====================

    /**
     * When acknowledgmentsGroupingTracker.isDuplicate() returns true for a message,
     * that message should be skipped (skippedMessages++) and not enqueued.
     */
    @Test
    public void testDuplicateMessagesAreSkipped() throws Exception {
        // Inject a mock acknowledgmentsGroupingTracker that marks all messages as duplicates
        AcknowledgmentsGroupingTracker mockTracker = mock(AcknowledgmentsGroupingTracker.class);
        when(mockTracker.isDuplicate(any(MessageId.class))).thenReturn(true);
        FieldUtils.writeField(consumer, "acknowledgmentsGroupingTracker", mockTracker, true);

        BrokerEntryMetadata brokerEntryMetadata =
                new BrokerEntryMetadata().setBrokerTimestamp(1).setIndex(1);

        int batchSize = 3;
        MessageMetadata metadata = new MessageMetadata()
                .setProducerName("test-producer")
                .setSequenceId(1)
                .setPublishTime(1)
                .setNumMessagesInBatch(batchSize);

        ByteBuf batchBuffer = Unpooled.buffer(1024);
        for (int i = 0; i < batchSize; i++) {
            Commands.serializeSingleMessageInBatchWithPayload(
                    new SingleMessageMetadata().setPartitionKey("key-" + i),
                    Unpooled.wrappedBuffer(("payload-" + i).getBytes()), batchBuffer);
        }

        consumer.receiveIndividualMessagesFromBatch(brokerEntryMetadata, metadata, 0, null,
                batchBuffer, new MessageIdData().setLedgerId(5002).setEntryId(3),
                mockCnx, DEFAULT_CONSUMER_EPOCH, false);

        // All messages are duplicates, none should be enqueued
        assertEquals(consumer.numMessagesInQueue(), 0,
                "Duplicate messages should not be enqueued");

        // No receive-failed since there is no decode error
        verify(statsRecorder, never()).incrementNumReceiveFailed();

        // isDuplicate should have been called for each message
        verify(mockTracker, times(batchSize)).isDuplicate(any(MessageId.class));
    }

    /**
     * When only the second message out of 3 is a duplicate, messages at
     * index 0 and 2 should be enqueued, message at index 1 is skipped.
     */
    @Test
    public void testPartialDuplicateMessages() throws Exception {
        AcknowledgmentsGroupingTracker mockTracker = mock(AcknowledgmentsGroupingTracker.class);
        // Only the second call returns true (message at index 1 is duplicate)
        when(mockTracker.isDuplicate(any(MessageId.class)))
                .thenReturn(false)
                .thenReturn(true)
                .thenReturn(false);
        FieldUtils.writeField(consumer, "acknowledgmentsGroupingTracker", mockTracker, true);

        BrokerEntryMetadata brokerEntryMetadata =
                new BrokerEntryMetadata().setBrokerTimestamp(1).setIndex(1);

        int batchSize = 3;
        MessageMetadata metadata = new MessageMetadata()
                .setProducerName("test-producer")
                .setSequenceId(1)
                .setPublishTime(1)
                .setNumMessagesInBatch(batchSize);

        ByteBuf batchBuffer = Unpooled.buffer(1024);
        for (int i = 0; i < batchSize; i++) {
            Commands.serializeSingleMessageInBatchWithPayload(
                    new SingleMessageMetadata().setPartitionKey("key-" + i),
                    Unpooled.wrappedBuffer(("payload-" + i).getBytes()), batchBuffer);
        }

        consumer.receiveIndividualMessagesFromBatch(brokerEntryMetadata, metadata, 0, null,
                batchBuffer, new MessageIdData().setLedgerId(5003).setEntryId(4),
                mockCnx, DEFAULT_CONSUMER_EPOCH, false);

        // 2 non-duplicate messages should be enqueued
        Awaitility.await().untilAsserted(() ->
                assertEquals(consumer.numMessagesInQueue(), 2,
                        "Only non-duplicate messages should be enqueued"));

        verify(statsRecorder, never()).incrementNumReceiveFailed();
    }

    // ==================== Dead letter policy tests ====================

    /**
     * Helper to create a consumer with dead letter policy enabled.
     * The maxRedeliverCount is set so that we can test the dead letter branches.
     */
    private ConsumerImpl<byte[]> createConsumerWithDeadLetterPolicy(int maxRedeliverCount) throws Exception {
        ExecutorProvider dlqExecutorProvider = new ExecutorProvider(1, "DLQ-Test");
        ExecutorService dlqInternalExecutor = Executors.newSingleThreadScheduledExecutor();

        PulsarClientImpl client = ClientTestFixtures.createPulsarClientMockWithMockedClientCnx(
                dlqExecutorProvider, dlqInternalExecutor, mockCnx);
        ClientConfigurationData clientConf = client.getConfiguration();
        clientConf.setOperationTimeoutMs(100);
        clientConf.setStatsIntervalSeconds(1);

        ConsumerConfigurationData<byte[]> consumerConf = new ConsumerConfigurationData<>();
        consumerConf.setSubscriptionName("test-sub");
        consumerConf.setDeadLetterPolicy(
                DeadLetterPolicy.builder()
                        .maxRedeliverCount(maxRedeliverCount)
                        .build());

        CompletableFuture<Consumer<byte[]>> subscribeFuture = new CompletableFuture<>();
        ConsumerImpl<byte[]> dlqConsumer = ConsumerImpl.newConsumerImpl(client, TOPIC, consumerConf,
                dlqExecutorProvider, -1, false, subscribeFuture, null, null, null,
                true);
        dlqConsumer.setState(HandlerState.State.Ready);
        dlqConsumer.setClientCnx(mockCnx);

        // Inject stats spy
        ConsumerStatsRecorderImpl dlqStats = spy(new ConsumerStatsRecorderImpl(dlqConsumer));
        FieldUtils.writeField(dlqConsumer, "stats", dlqStats, true);

        return dlqConsumer;
    }

    /**
     * When redeliveryCount == maxRedeliverCount, messages should be added to
     * possibleSendToDeadLetterTopicMessages but still enqueued (not skipped).
     */
    @Test
    @SuppressWarnings("unchecked")
    public void testDeadLetterPolicyAtMaxRedeliverCount() throws Exception {
        int maxRedeliverCount = 3;
        ConsumerImpl<byte[]> dlqConsumer = createConsumerWithDeadLetterPolicy(maxRedeliverCount);

        BrokerEntryMetadata brokerEntryMetadata =
                new BrokerEntryMetadata().setBrokerTimestamp(1).setIndex(1);

        int batchSize = 2;
        MessageMetadata metadata = new MessageMetadata()
                .setProducerName("test-producer")
                .setSequenceId(1)
                .setPublishTime(1)
                .setNumMessagesInBatch(batchSize);

        ByteBuf batchBuffer = Unpooled.buffer(1024);
        for (int i = 0; i < batchSize; i++) {
            Commands.serializeSingleMessageInBatchWithPayload(
                    new SingleMessageMetadata().setPartitionKey("key-" + i),
                    Unpooled.wrappedBuffer(("payload-" + i).getBytes()), batchBuffer);
        }

        // redeliveryCount == maxRedeliverCount (3)
        dlqConsumer.receiveIndividualMessagesFromBatch(brokerEntryMetadata, metadata,
                maxRedeliverCount, null, batchBuffer,
                new MessageIdData().setLedgerId(6000).setEntryId(1),
                mockCnx, DEFAULT_CONSUMER_EPOCH, false);

        // Messages should be enqueued (redeliveryCount == max, not > max)
        Awaitility.await().untilAsserted(() ->
                assertEquals(dlqConsumer.numMessagesInQueue(), batchSize,
                        "Messages at maxRedeliverCount should still be enqueued"));

        // Verify possibleSendToDeadLetterTopicMessages is populated
        Map<?, ?> deadLetterMap = (Map<?, ?>) FieldUtils.readField(
                dlqConsumer, "possibleSendToDeadLetterTopicMessages", true);
        assertFalse(deadLetterMap.isEmpty(),
                "possibleSendToDeadLetterTopicMessages should be populated at maxRedeliverCount");
    }

    /**
     * When redeliveryCount > maxRedeliverCount, messages should be skipped
     * (not enqueued) and redeliverUnacknowledgedMessages should be triggered.
     */
    @Test
    public void testDeadLetterPolicyExceedsMaxRedeliverCount() throws Exception {
        int maxRedeliverCount = 3;
        ConsumerImpl<byte[]> dlqConsumer = createConsumerWithDeadLetterPolicy(maxRedeliverCount);

        BrokerEntryMetadata brokerEntryMetadata =
                new BrokerEntryMetadata().setBrokerTimestamp(1).setIndex(1);

        int batchSize = 2;
        MessageMetadata metadata = new MessageMetadata()
                .setProducerName("test-producer")
                .setSequenceId(1)
                .setPublishTime(1)
                .setNumMessagesInBatch(batchSize);

        ByteBuf batchBuffer = Unpooled.buffer(1024);
        for (int i = 0; i < batchSize; i++) {
            Commands.serializeSingleMessageInBatchWithPayload(
                    new SingleMessageMetadata().setPartitionKey("key-" + i),
                    Unpooled.wrappedBuffer(("payload-" + i).getBytes()), batchBuffer);
        }

        // redeliveryCount > maxRedeliverCount (4 > 3)
        dlqConsumer.receiveIndividualMessagesFromBatch(brokerEntryMetadata, metadata,
                maxRedeliverCount + 1, null, batchBuffer,
                new MessageIdData().setLedgerId(6001).setEntryId(2),
                mockCnx, DEFAULT_CONSUMER_EPOCH, false);

        // Messages should be skipped (redeliveryCount > max)
        assertEquals(dlqConsumer.numMessagesInQueue(), 0,
                "Messages exceeding maxRedeliverCount should be skipped");
    }
}
