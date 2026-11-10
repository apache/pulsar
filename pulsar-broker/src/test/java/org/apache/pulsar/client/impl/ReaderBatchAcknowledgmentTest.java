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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TopicMessageId;
import org.apache.pulsar.client.impl.conf.ReaderConfigurationData;
import org.mockito.MockedConstruction;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-impl")
public class ReaderBatchAcknowledgmentTest {
    private static final String TOPIC_A = "persistent://public/default/reader-ack-a";
    private static final String TOPIC_B = "persistent://public/default/reader-ack-b";

    private record Ack(String topic, long ledger, long entry, int partition, int batchIndex) { }

    @DataProvider(name = "readModes")
    public Object[][] readModes() {
        return new Object[][]{{false}, {true}};
    }

    @DataProvider(name = "readerKindsAndReadModes")
    public Object[][] readerKindsAndReadModes() {
        return new Object[][]{{false, false}, {false, true}, {true, false}, {true, true}};
    }

    @Test(dataProvider = "readModes")
    public void testSingleTopicBatchAcknowledgesLastMessageCumulatively(boolean async) throws Exception {
        MessagesImpl<byte[]> batch = new MessagesImpl<>(0, 0);
        batch.add(message(TOPIC_A, -1, 1, false));
        batch.add(message(TOPIC_A, -1, 2, false));
        batch.add(message(TOPIC_A, -1, 3, false));

        checkBatch(false, async, batch, List.of(new Ack(TOPIC_A, 1, 3, -1, -1)));
    }

    @Test(dataProvider = "readModes")
    public void testInterleavedPartitionsAcknowledgeLastMessageCumulatively(boolean async) throws Exception {
        MessagesImpl<byte[]> batch = new MessagesImpl<>(0, 0);
        batch.add(message(TOPIC_A, 0, 1, true));
        batch.add(message(TOPIC_B, 0, 1, true));
        batch.add(message(TOPIC_A, 1, 1, true));
        batch.add(message(TOPIC_B, 1, 1, true));
        batch.add(message(TOPIC_B, 1, 2, true));
        batch.add(message(TOPIC_A, 1, 2, true));
        batch.add(message(TOPIC_B, 0, 2, true));
        batch.add(message(TOPIC_A, 0, 2, true));

        checkBatch(true, async, batch, List.of(
                new Ack(TOPIC_A + "-partition-0", 1, 2, 0, -1),
                new Ack(TOPIC_A + "-partition-1", 1, 2, 1, -1),
                new Ack(TOPIC_B + "-partition-0", 1, 2, 0, -1),
                new Ack(TOPIC_B + "-partition-1", 1, 2, 1, -1)));
    }

    @Test(dataProvider = "readerKindsAndReadModes")
    public void testEmptyBatchDoesNotAcknowledge(boolean multiTopic, boolean async) throws Exception {
        checkBatch(multiTopic, async, new MessagesImpl<>(0, 0), List.of());
    }

    private void checkBatch(boolean multiTopic, boolean async, Messages<byte[]> batch, List<Ack> expected)
            throws Exception {
        ReaderConfigurationData<byte[]> configuration = new ReaderConfigurationData<>();
        configuration.setTopicName(TOPIC_A);
        configuration.setStartMessageId(MessageId.earliest);
        if (multiTopic) {
            configuration.getTopicNames().add(TOPIC_B);
            try (MockedConstruction<MultiTopicsConsumerImpl> construction =
                         mockConstruction(MultiTopicsConsumerImpl.class)) {
                MultiTopicsReaderImpl<byte[]> reader = new MultiTopicsReaderImpl<>(
                        null, configuration, null, new CompletableFuture<>(), Schema.BYTES);
                assertThat(construction.constructed()).hasSize(1);
                MultiTopicsConsumerImpl<byte[]> consumer = reader.getMultiTopicsConsumer();
                doCallRealMethod().when(consumer).tryAcknowledgeMessage(any());
                checkBatch(reader, consumer, batch, expected, async, true);
            }
        } else {
            try (MockedConstruction<ConsumerImpl> construction = mockConstruction(ConsumerImpl.class)) {
                ReaderImpl<byte[]> reader = new ReaderImpl<>(
                        null, configuration, null, new CompletableFuture<>(), Schema.BYTES);
                assertThat(construction.constructed()).hasSize(1);
                checkBatch(reader, reader.getConsumer(), batch, expected, async, false);
            }
        }
    }

    private void checkBatch(Reader<byte[]> reader, ConsumerBase<byte[]> consumer, Messages<byte[]> batch,
                            List<Ack> expected, boolean async, boolean multiTopic) throws Exception {
        List<Ack> acknowledgments = new ArrayList<>();
        when(consumer.batchReceive()).thenReturn(batch);
        when(consumer.batchReceiveAsync()).thenReturn(CompletableFuture.completedFuture(batch));
        when(consumer.acknowledgeAsync(any(Messages.class))).thenReturn(CompletableFuture.completedFuture(null));
        doAnswer(invocation -> {
            Message<?> message = invocation.getArgument(0);
            acknowledgments.add(ack(message.getMessageId(), multiTopic));
            return CompletableFuture.completedFuture(null);
        }).when(consumer).acknowledgeCumulativeAsync(any(Message.class));
        doAnswer(invocation -> {
            MessageId id = invocation.getArgument(0);
            acknowledgments.add(ack(id, multiTopic));
            return CompletableFuture.completedFuture(null);
        }).when(consumer).acknowledgeCumulativeAsync(any(MessageId.class));
        clearInvocations(consumer);

        Messages<byte[]> result = async ? reader.batchReadNextAsync().get(5, TimeUnit.SECONDS)
                : reader.batchReadNext();

        assertThat(result).as("returned batch").isSameAs(batch);
        assertThat(acknowledgments).as("cumulative acknowledgments per topic partition")
                .containsExactlyInAnyOrderElementsOf(expected);
        if (async) {
            verify(consumer).batchReceiveAsync();
        } else {
            verify(consumer).batchReceive();
        }
        verify(consumer, atLeast(0)).acknowledgeCumulativeAsync(any(Message.class));
        verify(consumer, atLeast(0)).acknowledgeCumulativeAsync(any(MessageId.class));
        if (multiTopic) {
            verify((MultiTopicsConsumerImpl<byte[]>) consumer, atLeast(0)).tryAcknowledgeMessage(any());
        }
        verifyNoMoreInteractions(consumer);
    }

    private Ack ack(MessageId id, boolean multiTopic) {
        assertThat(id).isInstanceOf(MessageIdAdv.class);
        if (multiTopic) {
            assertThat(id).as("multi-topic acknowledgments must retain their owner topic")
                    .isInstanceOf(TopicMessageId.class);
        }
        String topic = id instanceof TopicMessageId topicId ? topicId.getOwnerTopic() : TOPIC_A;
        MessageIdAdv advancedId = (MessageIdAdv) id;
        return new Ack(topic, advancedId.getLedgerId(), advancedId.getEntryId(),
                advancedId.getPartitionIndex(), advancedId.getBatchIndex());
    }

    @SuppressWarnings("unchecked")
    private Message<byte[]> message(String topic, int partition, int entry, boolean multiTopic) {
        String partitionTopic = partition < 0 ? topic : topic + "-partition-" + partition;
        Message<byte[]> message = mock(Message.class);
        when(message.getTopicName()).thenReturn(partitionTopic);
        when(message.getMessageId()).thenReturn(new MessageIdImpl(1, entry, partition));
        return multiTopic ? new TopicMessageImpl<>(partitionTopic, message, null) : message;
    }
}