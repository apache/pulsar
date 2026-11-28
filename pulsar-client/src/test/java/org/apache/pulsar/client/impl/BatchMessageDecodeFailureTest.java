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
import static org.mockito.Mockito.doAnswer;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandAck;
import org.apache.pulsar.common.api.proto.MessageIdData;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.SingleMessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests how {@link ConsumerImpl#receiveIndividualMessagesFromBatch} handles a batch with a message that fails to
 * deserialize: the messages before it are delivered, the rest of the batch is discarded, the broker gets back the
 * permits it charged for the discarded messages, and only the discarded messages are acknowledged.
 */
public class BatchMessageDecodeFailureTest {

    private static final String TOPIC = "persistent://tenant/ns1/test-decode-failure";
    private static final long LEDGER_ID = 1000;

    private enum Corruption {
        // payload_size is a required field, an empty SingleMessageMetadata fails checkRequiredFields()
        MISSING_REQUIRED_FIELD,
        // tag 0x0B has wire type 3, which the parser rejects with an IllegalArgumentException
        UNKNOWN_WIRE_TYPE,
        // the metadata size points past the end of the entry, which fails with an IndexOutOfBoundsException
        TRUNCATED
    }

    private ExecutorProvider executorProvider;
    private ExecutorService internalExecutor;
    private ConsumerImpl<byte[]> consumer;
    private final List<BaseCommand> sentCommands = new CopyOnWriteArrayList<>();

    @BeforeMethod(alwaysRun = true)
    public void setUp() {
        executorProvider = new ExecutorProvider(1, "BatchDecodeFailureTest");
        internalExecutor = Executors.newSingleThreadScheduledExecutor();
        sentCommands.clear();
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

    private void createConsumer(boolean batchIndexAckEnabled) {
        ClientCnx cnx = ClientTestFixtures.mockClientCnx();
        ChannelHandlerContext ctx = cnx.ctx();
        doAnswer(invocation -> {
            sentCommands.add(decodeAndRelease(invocation.getArgument(0)));
            return null;
        }).when(ctx).writeAndFlush(any(), any());
        doAnswer(invocation -> {
            sentCommands.add(decodeAndRelease(invocation.getArgument(0)));
            return null;
        }).when(ctx).writeAndFlush(any());

        PulsarClientImpl client = ClientTestFixtures.createPulsarClientMockWithMockedClientCnx(
                executorProvider, internalExecutor, cnx);
        client.getConfiguration().setOperationTimeoutMs(100);

        ConsumerConfigurationData<byte[]> consumerConf = new ConsumerConfigurationData<>();
        consumerConf.setSubscriptionName("test-sub");
        consumerConf.setBatchIndexAckEnabled(batchIndexAckEnabled);
        // send the acks right away, so that the test sees them without flushing the tracker
        consumerConf.setAcknowledgementsGroupTimeMicros(0);

        CompletableFuture<Consumer<byte[]>> subscribeFuture = new CompletableFuture<>();
        consumer = ConsumerImpl.newConsumerImpl(client, TOPIC, consumerConf,
                executorProvider, -1, false, subscribeFuture, null, null, null,
                true);
        consumer.setState(HandlerState.State.Ready);
        consumer.setClientCnx(cnx);
    }

    @DataProvider
    public Object[][] corruptions() {
        List<Object[]> params = new ArrayList<>();
        for (Corruption corruption : Corruption.values()) {
            params.add(new Object[]{corruption, true});
            params.add(new Object[]{corruption, false});
        }
        return params.toArray(new Object[0][]);
    }

    @DataProvider
    public Object[][] batchIndexAck() {
        return new Object[][]{{true}, {false}};
    }

    /**
     * Batch of 5 on its first delivery, index 3 corrupted: indexes 0-2 are delivered, the broker gets back the 2
     * permits of indexes 3-4, and only indexes 3-4 are acknowledged.
     */
    @Test(dataProvider = "corruptions")
    public void testCorruptedMessageInTheMiddleOfTheBatch(Corruption corruption, boolean batchIndexAckEnabled)
            throws Exception {
        createConsumer(batchIndexAckEnabled);
        int permitsBefore = consumer.getAvailablePermits();

        receiveBatch(1, 5, null, batchPayload(5, 3, corruption));

        assertEquals(consumer.numMessagesInQueue(), 3);
        assertEquals(consumer.getAvailablePermits() - permitsBefore, 2);
        List<CommandAck> acks = sentAcks();
        if (batchIndexAckEnabled) {
            assertEquals(acks.size(), 1);
            assertEquals(ackSetOf(acks.get(0), 1), bits(0, 1, 2));
            assertTrue(acks.get(0).hasValidationError());
        } else {
            // the whole entry is acked once the application acks the delivered messages
            assertTrue(acks.isEmpty());
        }
    }

    /**
     * When the first message is corrupted, nothing is delivered and the whole entry is acked.
     */
    @Test(dataProvider = "corruptions")
    public void testCorruptedFirstMessageAcksTheWholeEntry(Corruption corruption, boolean batchIndexAckEnabled)
            throws Exception {
        createConsumer(batchIndexAckEnabled);
        int permitsBefore = consumer.getAvailablePermits();

        receiveBatch(2, 5, null, batchPayload(5, 0, corruption));

        assertEquals(consumer.numMessagesInQueue(), 0);
        assertEquals(consumer.getAvailablePermits() - permitsBefore, 5);
        List<CommandAck> acks = sentAcks();
        assertEquals(acks.size(), 1);
        assertNull(ackSetOf(acks.get(0), 2));
        assertTrue(acks.get(0).hasValidationError());
    }

    /**
     * Redelivery of a batch of 5 where the broker only delivers index 2 (ack set {2}) and index 3 is corrupted: the
     * broker charged no permit for indexes 3-4, and index 2, which the application holds, must stay unacknowledged.
     */
    @Test
    public void testCorruptedMessageOnPartialRedelivery() throws Exception {
        createConsumer(true);
        int permitsBefore = consumer.getAvailablePermits();

        receiveBatch(3, 5, bits(2).toLongArray(), batchPayload(5, 3, Corruption.MISSING_REQUIRED_FIELD));

        assertEquals(consumer.numMessagesInQueue(), 1);
        assertEquals(consumer.getAvailablePermits() - permitsBefore, 0);
        List<CommandAck> acks = sentAcks();
        assertEquals(acks.size(), 1);
        assertEquals(ackSetOf(acks.get(0), 3), bits(2));
    }

    /**
     * Redelivery of a batch of 6 with indexes 1, 2 and 4 still unacknowledged and index 4 corrupted: only index 4's
     * permit goes back, and indexes 1-2 stay unacknowledged.
     */
    @Test
    public void testCorruptedUnackedMessageOnSparseRedelivery() throws Exception {
        createConsumer(true);
        int permitsBefore = consumer.getAvailablePermits();

        receiveBatch(4, 6, bits(1, 2, 4).toLongArray(), batchPayload(6, 4, Corruption.UNKNOWN_WIRE_TYPE));

        assertEquals(consumer.numMessagesInQueue(), 2);
        assertEquals(consumer.getAvailablePermits() - permitsBefore, 1);
        List<CommandAck> acks = sentAcks();
        assertEquals(acks.size(), 1);
        assertEquals(ackSetOf(acks.get(0), 4), bits(1, 2));
    }

    /**
     * A batch whose metadata announces more messages than the entry holds: the messages it does hold are delivered,
     * and the missing one is discarded instead of failing the whole connection.
     */
    @Test(dataProvider = "batchIndexAck")
    public void testTruncatedBatch(boolean batchIndexAckEnabled) throws Exception {
        createConsumer(batchIndexAckEnabled);
        int permitsBefore = consumer.getAvailablePermits();

        receiveBatch(5, 3, null, batchPayload(2, -1, null));

        assertEquals(consumer.numMessagesInQueue(), 2);
        assertEquals(consumer.getAvailablePermits() - permitsBefore, 1);
        List<CommandAck> acks = sentAcks();
        if (batchIndexAckEnabled) {
            assertEquals(acks.size(), 1);
            assertEquals(ackSetOf(acks.get(0), 5), bits(0, 1));
        } else {
            assertTrue(acks.isEmpty());
        }
    }

    /**
     * After the corrupted tail is discarded, acking the delivered messages acks the whole entry, also when the
     * broker ignores batch index acks.
     */
    @Test(dataProvider = "batchIndexAck")
    public void testWholeEntryAckedOnceTheDeliveredMessagesAreAcked(boolean batchIndexAckEnabled) throws Exception {
        createConsumer(batchIndexAckEnabled);

        receiveBatch(6, 3, null, batchPayload(3, 2, Corruption.MISSING_REQUIRED_FIELD));
        assertEquals(consumer.numMessagesInQueue(), 2);

        Message<byte[]> first = consumer.receive(1, TimeUnit.SECONDS);
        Message<byte[]> second = consumer.receive(1, TimeUnit.SECONDS);
        assertNotNull(first);
        assertNotNull(second);
        assertEquals(new String(first.getData(), StandardCharsets.UTF_8), "payload-0");
        assertEquals(new String(second.getData(), StandardCharsets.UTF_8), "payload-1");

        consumer.acknowledge(first);
        List<CommandAck> acks = sentAcks();
        if (batchIndexAckEnabled) {
            assertNotNull(ackSetOf(acks.get(acks.size() - 1), 6), "the entry must not be fully acked yet");
        } else {
            assertTrue(acks.isEmpty(), "the entry must not be acked yet");
        }

        consumer.acknowledge(second);
        acks = sentAcks();
        assertNull(ackSetOf(acks.get(acks.size() - 1), 6), "the last ack must ack the whole entry");
    }

    @Test
    public void testValidBatch() throws Exception {
        createConsumer(true);
        int permitsBefore = consumer.getAvailablePermits();

        receiveBatch(7, 3, null, batchPayload(3, -1, null));

        assertEquals(consumer.numMessagesInQueue(), 3);
        assertEquals(consumer.getAvailablePermits() - permitsBefore, 0);
        assertTrue(sentAcks().isEmpty());
    }

    private void receiveBatch(long entryId, int numMessagesInBatch, long[] ackSet, ByteBuf payload)
            throws Exception {
        MessageMetadata metadata = new MessageMetadata()
                .setProducerName("test-producer")
                .setSequenceId(1)
                .setPublishTime(1)
                .setNumMessagesInBatch(numMessagesInBatch);
        try {
            consumer.receiveIndividualMessagesFromBatch(null, metadata, 0, ackSet, payload,
                    new MessageIdData().setLedgerId(LEDGER_ID).setEntryId(entryId),
                    consumer.getClientCnx(), DEFAULT_CONSUMER_EPOCH, false);
        } finally {
            payload.release();
        }
        // the delivered messages are enqueued on the internal executor, wait for them
        internalExecutor.submit(() -> { }).get();
    }

    /**
     * Serializes {@code numMessages} messages, with the one at {@code corruptedIndex} (if any) replaced by
     * {@code corruption}. A corrupted message ends the payload, since nothing after it can be read anyway.
     */
    private static ByteBuf batchPayload(int numMessages, int corruptedIndex, Corruption corruption) {
        ByteBuf payload = Unpooled.buffer(1024);
        for (int i = 0; i < numMessages; i++) {
            if (i == corruptedIndex) {
                switch (corruption) {
                    case MISSING_REQUIRED_FIELD -> payload.writeInt(0);
                    case UNKNOWN_WIRE_TYPE -> payload.writeInt(1).writeByte(0x0B);
                    case TRUNCATED -> payload.writeInt(100).writeByte(0);
                }
                break;
            }
            ByteBuf singlePayload = Unpooled.wrappedBuffer(("payload-" + i).getBytes(StandardCharsets.UTF_8));
            Commands.serializeSingleMessageInBatchWithPayload(
                    new SingleMessageMetadata().setPartitionKey("key-" + i), singlePayload, payload);
            singlePayload.release();
        }
        return payload;
    }

    private List<CommandAck> sentAcks() {
        List<CommandAck> acks = new ArrayList<>();
        for (BaseCommand command : sentCommands) {
            if (command.getType() == BaseCommand.Type.ACK) {
                acks.add(command.getAck());
            }
        }
        return acks;
    }

    /**
     * Returns the ack set the ack sends for the given entry, null when it acks the whole entry.
     */
    private static BitSet ackSetOf(CommandAck ack, long entryId) {
        for (int i = 0; i < ack.getMessageIdsCount(); i++) {
            MessageIdData id = ack.getMessageIdAt(i);
            if (id.getLedgerId() == LEDGER_ID && id.getEntryId() == entryId) {
                if (id.getAckSetsCount() == 0) {
                    return null;
                }
                long[] ackSet = new long[id.getAckSetsCount()];
                for (int j = 0; j < ackSet.length; j++) {
                    ackSet[j] = id.getAckSetAt(j);
                }
                return BitSet.valueOf(ackSet);
            }
        }
        throw new AssertionError("The ack does not include entry " + entryId);
    }

    private static BitSet bits(int... indexes) {
        BitSet bitSet = new BitSet();
        for (int index : indexes) {
            bitSet.set(index);
        }
        return bitSet;
    }

    private static BaseCommand decodeAndRelease(ByteBuf frame) {
        try {
            frame.skipBytes(Integer.BYTES);
            int commandSize = (int) frame.readUnsignedInt();
            BaseCommand command = new BaseCommand();
            command.parseFrom(frame, commandSize);
            // copy the parts that the tests read, since the frame is released below
            command.materialize();
            return command;
        } finally {
            frame.release();
        }
    }
}
