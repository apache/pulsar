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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import io.netty.buffer.ByteBuf;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.metrics.InstrumentProvider;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.SingleMessageMetadata;
import org.apache.pulsar.common.protocol.Commands;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class BatchMessageKeyBasedContainerTest {

    @DataProvider
    public Object[][] equivalentKeys() {
        return new Object[][] {
                {KeyKind.TEXT, KeyKind.TEXT, "a"},
                {KeyKind.BYTES, KeyKind.BYTES, "a"},
                {KeyKind.ORDERING, KeyKind.ORDERING, "a"},
                {KeyKind.TEXT, KeyKind.BYTES, "a"},
                {KeyKind.BYTES, KeyKind.TEXT, "a"},
                {KeyKind.TEXT, KeyKind.ORDERING, "a"},
                {KeyKind.ORDERING, KeyKind.TEXT, "a"},
                {KeyKind.BYTES, KeyKind.ORDERING, "a"},
                {KeyKind.ORDERING, KeyKind.BYTES, "a"},
                {KeyKind.TEXT, KeyKind.BYTES, "\u00e9"},
                {KeyKind.TEXT, KeyKind.BYTES, ""},
                {KeyKind.ORDERING, KeyKind.TEXT, ""}
        };
    }

    @Test(dataProvider = "equivalentKeys")
    public void testEquivalentKeysPreserveOrder(KeyKind firstKind, KeyKind secondKind, String key) throws Exception {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        try (BatchFixture fixture = new BatchFixture()) {
            fixture.add(fixture.message(0, firstKind, keyBytes));
            fixture.add(fixture.message(1, secondKind, keyBytes));
            fixture.add(fixture.message(2, firstKind, keyBytes));

            List<ProducerImpl.OpSendMsg> ops = fixture.flush();

            assertThat(ops.stream().flatMap(op -> op.msgs.stream()).map(MessageImpl::getSequenceId).toList())
                    .as("the actual send order for one sticky key")
                    .containsExactly(0L, 1L, 2L);
            assertThat(ops).as("equivalent sticky keys must share one batch").hasSize(1);
            assertThat(ops.get(0).msgs).extracting(MessageImpl::getSequenceId).containsExactly(0L, 1L, 2L);
            assertRoutingKey(ops.get(0), keyBytes);
        }
    }

    @Test(dataProvider = "equivalentKeys")
    public void testBatchRoundTripPreservesOriginalKeyRepresentations(KeyKind firstKind, KeyKind secondKind, String key)
            throws Exception {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        try (BatchFixture fixture = new BatchFixture()) {
            List<MessageImpl<?>> messages = List.of(fixture.message(0, firstKind, keyBytes),
                    fixture.message(1, secondKind, keyBytes), fixture.message(2, firstKind, keyBytes));
            messages.forEach(fixture::add);

            List<ProducerImpl.OpSendMsg> ops = fixture.flush();

            assertThat(ops).hasSize(1);
            ProducerImpl.OpSendMsg op = ops.get(0);
            MessageMetadata batchMetadata = metadataOf(op);
            assertBatchKeyRepresentation(batchMetadata, messages.get(0).getMessageBuilder());
            ByteBuf batchPayload = op.cmd.getSecond().duplicate();
            for (int i = 0; i < messages.size(); i++) {
                SingleMessageMetadata singleMetadata = new SingleMessageMetadata();
                ByteBuf payload = Commands.deSerializeSingleMessageInBatch(
                        batchPayload, singleMetadata, i, messages.size());
                try {
                    MessageImpl<byte[]> decoded = MessageImpl.create("test-topic",
                            new BatchMessageIdImpl(1, 2, -1, i), batchMetadata, singleMetadata,
                            payload, Optional.empty(), null, Schema.BYTES, 0, true,
                            Commands.DEFAULT_CONSUMER_EPOCH);
                    try {
                        assertKeyRepresentation(decoded, messages.get(i));
                        assertThat(decoded.getSequenceId()).as("the original message sequence id")
                                .isEqualTo(messages.get(i).getSequenceId());
                    } finally {
                        decoded.release();
                    }
                } finally {
                    payload.release();
                }
            }
            assertThat(batchPayload.isReadable()).as("all serialized messages have been decoded").isFalse();
        }
    }

    @DataProvider
    public Object[][] differentKeys() {
        return new Object[][] {
                {KeyKind.BYTES, new byte[] {'a'}, KeyKind.TEXT, "YQ==".getBytes(StandardCharsets.UTF_8)},
                {KeyKind.ORDERING, new byte[] {'a'}, KeyKind.TEXT, "YQ==".getBytes(StandardCharsets.UTF_8)},
                {KeyKind.BYTES, new byte[] {(byte) 0x80}, KeyKind.BYTES, new byte[] {(byte) 0x81}}
        };
    }

    @Test(dataProvider = "differentKeys")
    public void testDifferentKeysAreNeverBatchedTogether(KeyKind firstKind, byte[] firstKey,
                                                       KeyKind secondKind, byte[] secondKey) throws Exception {
        try (BatchFixture fixture = new BatchFixture()) {
            MessageImpl<?> first = fixture.message(0, firstKind, firstKey);
            MessageImpl<?> second = fixture.message(1, secondKind, secondKey);
            assertThat(Commands.resolveStickyKeyHash(first.getMessageBuilder()))
                    .as("the broker routes these messages by different sticky keys")
                    .isNotEqualTo(Commands.resolveStickyKeyHash(second.getMessageBuilder()));
            fixture.add(first);
            fixture.add(second);

            List<ProducerImpl.OpSendMsg> ops = fixture.flush();

            assertThat(ops).as("different sticky keys need separate routing metadata").hasSize(2);
            assertThat(ops.get(0).msgs).containsExactly(first);
            assertThat(ops.get(1).msgs).containsExactly(second);
            assertRoutingKey(ops.get(0), firstKey);
            assertRoutingKey(ops.get(1), secondKey);
        }
    }

    @Test
    public void testOrderingKeyTakesPrecedenceOverPartitionKey() throws Exception {
        try (BatchFixture fixture = new BatchFixture()) {
            byte[] firstKey = new byte[] {'a'};
            byte[] secondKey = new byte[] {'b'};
            MessageImpl<?> first = fixture.message(0, KeyKind.ORDERING, firstKey);
            first.getMessageBuilder().setPartitionKey("shared-partition");
            MessageImpl<?> second = fixture.message(1, KeyKind.ORDERING, secondKey);
            second.getMessageBuilder().setPartitionKey("shared-partition");
            MessageImpl<?> third = fixture.message(2, KeyKind.TEXT, firstKey);
            fixture.add(first);
            fixture.add(second);
            fixture.add(third);

            List<ProducerImpl.OpSendMsg> ops = fixture.flush();

            assertThat(ops).hasSize(2);
            assertThat(ops.get(0).msgs).containsExactly(second);
            assertThat(ops.get(1).msgs).containsExactly(first, third);
            assertRoutingKey(ops.get(0), secondKey);
            assertRoutingKey(ops.get(1), firstKey);
        }
    }

    @Test(dataProvider = "equivalentKeys")
    public void testSchemaLookupUsesTheSameStickyKey(KeyKind firstKind, KeyKind secondKind, String key)
            throws Exception {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        try (BatchFixture fixture = new BatchFixture()) {
            MessageImpl<?> first = fixture.message(0, firstKind, keyBytes);
            first.getMessageBuilder().setSchemaVersion(new byte[] {1});
            fixture.add(first);
            MessageImpl<?> sameSchema = fixture.message(1, secondKind, keyBytes);
            sameSchema.getMessageBuilder().setSchemaVersion(new byte[] {1});
            MessageImpl<?> differentSchema = fixture.message(2, secondKind, keyBytes);
            differentSchema.getMessageBuilder().setSchemaVersion(new byte[] {2});
            MessageImpl<?> differentKey = fixture.message(3, KeyKind.TEXT,
                    "another-key".getBytes(StandardCharsets.UTF_8));
            differentKey.getMessageBuilder().setSchemaVersion(new byte[] {2});

            assertThat(fixture.container.hasSameSchema(sameSchema)).isTrue();
            assertThat(fixture.container.hasSameSchema(differentSchema))
                    .as("schema compatibility must be checked against the batch for the same raw key")
                    .isFalse();
            assertThat(fixture.container.hasSameSchema(differentKey))
                    .as("a new sticky key has no existing batch schema")
                    .isTrue();
        }
    }

    @Test(dataProvider = "differentKeys")
    public void testSchemaLookupKeepsDifferentStickyKeysIndependent(KeyKind firstKind, byte[] firstKey,
                                                                  KeyKind secondKind, byte[] secondKey) {
        try (BatchFixture fixture = new BatchFixture()) {
            MessageImpl<?> first = fixture.message(0, firstKind, firstKey);
            first.getMessageBuilder().setSchemaVersion(new byte[] {1});
            fixture.add(first);
            MessageImpl<?> differentKey = fixture.message(1, secondKind, secondKey);
            differentKey.getMessageBuilder().setSchemaVersion(new byte[] {2});

            assertThat(fixture.container.hasSameSchema(differentKey))
                    .as("a different raw key has no existing batch schema, even when stored key strings match")
                    .isTrue();
        }
    }

    @Test
    public void testMessagesWithoutKeysStillBatchSeparatelyFromEmptyKeys() throws Exception {
        try (BatchFixture fixture = new BatchFixture()) {
            byte[] emptyKey = new byte[0];
            MessageImpl<?> first = fixture.message(0, KeyKind.NONE, emptyKey);
            MessageImpl<?> second = fixture.message(1, KeyKind.TEXT, emptyKey);
            MessageImpl<?> third = fixture.message(2, KeyKind.NONE, emptyKey);
            MessageImpl<?> fourth = fixture.message(3, KeyKind.BYTES, emptyKey);
            fixture.add(first);
            fixture.add(second);
            fixture.add(third);
            fixture.add(fourth);

            List<ProducerImpl.OpSendMsg> ops = fixture.flush();

            assertThat(ops).hasSize(2);
            assertThat(ops.get(0).msgs).containsExactly(first, third);
            assertThat(ops.get(1).msgs).containsExactly(second, fourth);
            assertThat(metadataOf(ops.get(0)).hasPartitionKey()).isFalse();
            assertRoutingKey(ops.get(1), emptyKey);
        }
    }

    @Test
    public void testNullKeysStillBatchWithMessagesWithoutKeys() throws Exception {
        try (BatchFixture fixture = new BatchFixture()) {
            byte[] emptyKey = new byte[0];
            MessageImpl<?> first = fixture.message(0, KeyKind.NONE, emptyKey);
            MessageImpl<?> second = fixture.message(1, KeyKind.NULL_TEXT, emptyKey);
            MessageImpl<?> third = fixture.message(2, KeyKind.NULL_BYTES, emptyKey);
            fixture.add(first);
            fixture.add(second);
            fixture.add(third);

            List<ProducerImpl.OpSendMsg> ops = fixture.flush();

            assertThat(ops).hasSize(1);
            assertThat(ops.get(0).msgs).containsExactly(first, second, third);
            assertThat(ops.get(0).msgs).allSatisfy(message -> assertThat(message.hasKey()).isFalse());
            assertThat(metadataOf(ops.get(0)).hasPartitionKey()).isFalse();
        }
    }

    private static void assertBatchKeyRepresentation(MessageMetadata actual, MessageMetadata expected) {
        assertThat(actual.hasPartitionKey()).as("batch partition key presence").isEqualTo(expected.hasPartitionKey());
        if (expected.hasPartitionKey()) {
            assertThat(actual.getPartitionKey()).as("batch partition key representation")
                    .isEqualTo(expected.getPartitionKey());
        }
        assertThat(actual.hasPartitionKeyB64Encoded()).as("batch Base64 flag presence")
                .isEqualTo(expected.hasPartitionKeyB64Encoded());
        assertThat(actual.isPartitionKeyB64Encoded()).as("batch Base64 flag")
                .isEqualTo(expected.isPartitionKeyB64Encoded());
        assertThat(actual.hasOrderingKey()).as("batch ordering key presence").isEqualTo(expected.hasOrderingKey());
        if (expected.hasOrderingKey()) {
            assertThat(actual.getOrderingKey()).as("batch ordering key bytes").isEqualTo(expected.getOrderingKey());
        }
    }

    private static void assertKeyRepresentation(MessageImpl<?> actual, MessageImpl<?> expected) {
        assertThat(actual.hasKey()).as("partition key presence").isEqualTo(expected.hasKey());
        assertThat(actual.getKey()).as("partition key representation").isEqualTo(expected.getKey());
        assertThat(actual.hasBase64EncodedKey()).as("partition key Base64 flag")
                .isEqualTo(expected.hasBase64EncodedKey());
        assertThat(actual.getKeyBytes()).as("partition key bytes").isEqualTo(expected.getKeyBytes());
        assertThat(actual.hasOrderingKey()).as("ordering key presence").isEqualTo(expected.hasOrderingKey());
        assertThat(actual.getOrderingKey()).as("ordering key bytes").isEqualTo(expected.getOrderingKey());
    }

    private static void assertRoutingKey(ProducerImpl.OpSendMsg op, byte[] key) {
        assertThat(Commands.resolveStickyKey(metadataOf(op))).as("the batch routing key").isEqualTo(key);
        assertThat(op.msgs).allSatisfy(message ->
                assertThat(Commands.resolveStickyKey(message.getMessageBuilder()))
                        .as("each message must use the batch routing key")
                        .isEqualTo(key));
    }

    private static MessageMetadata metadataOf(ProducerImpl.OpSendMsg op) {
        ByteBuf header = op.cmd.getFirst().duplicate();
        int commandSize = header.getInt(header.readerIndex() + 4);
        header.skipBytes(8 + commandSize);
        MessageMetadata metadata = new MessageMetadata();
        Commands.peekMessageMetadata(header, metadata);
        return metadata;
    }

    private enum KeyKind {
        TEXT,
        BYTES,
        ORDERING,
        NULL_TEXT,
        NULL_BYTES,
        NONE;

        void apply(TypedMessageBuilder<byte[]> builder, byte[] key) {
            switch (this) {
                case TEXT -> builder.key(new String(key, StandardCharsets.UTF_8));
                case BYTES -> builder.keyBytes(key);
                case ORDERING -> builder.orderingKey(key);
                case NULL_TEXT -> builder.key(null);
                case NULL_BYTES -> builder.keyBytes(null);
                case NONE -> { }
            }
        }
    }

    private static final class BatchFixture implements AutoCloseable {
        private final ProducerImpl<?> producer = createProducer();
        private final BatchMessageKeyBasedContainer container = new BatchMessageKeyBasedContainer();
        private final List<MessageImpl<?>> messages = new ArrayList<>();
        private List<ProducerImpl.OpSendMsg> ops;

        private BatchFixture() {
            container.setProducer(producer);
        }

        MessageImpl<?> message(long sequenceId, KeyKind keyKind, byte[] key) {
            TypedMessageBuilderImpl<byte[]> builder = new TypedMessageBuilderImpl<>(producer, Schema.BYTES);
            builder.value(new byte[] {0}).sequenceId(sequenceId);
            keyKind.apply(builder, key);
            MessageImpl<?> message = (MessageImpl<?>) builder.getMessage();
            message.getMessageBuilder().setProducerName("producer").setPublishTime(1);
            messages.add(message);
            return message;
        }

        void add(MessageImpl<?> message) {
            container.add(message, null);
        }

        List<ProducerImpl.OpSendMsg> flush() throws Exception {
            ops = container.createOpSendMsgs();
            return ops;
        }

        @Override
        public void close() {
            if (ops == null) {
                container.discard(null);
            } else {
                // Successful sends transfer the buffers to the operations, as in the producer's flush path.
                container.clear();
                for (ProducerImpl.OpSendMsg op : ops) {
                    op.cmd.release();
                    op.recycle();
                }
            }
            for (MessageImpl<?> message : messages) {
                message.getDataBuffer().release();
                message.recycle();
            }
        }

        private static ProducerImpl<?> createProducer() {
            ProducerConfigurationData producerConfiguration = new ProducerConfigurationData();
            producerConfiguration.setCompressionType(CompressionType.NONE);
            PulsarClientImpl client = mock(PulsarClientImpl.class);
            when(client.newProducerId()).thenReturn(1L);
            when(client.getCnxPool()).thenReturn(mock(ConnectionPool.class));
            when(client.getMemoryLimitController()).thenReturn(mock(MemoryLimitController.class));
            Timer timer = mock(Timer.class);
            when(timer.newTimeout(any(), anyLong(), any())).thenReturn(mock(Timeout.class));
            when(client.timer()).thenReturn(timer);
            ClientConfigurationData clientConfiguration = new ClientConfigurationData();
            clientConfiguration.setStatsIntervalSeconds(0);
            when(client.getConfiguration()).thenReturn(clientConfiguration);
            when(client.instrumentProvider()).thenReturn(InstrumentProvider.NOOP);
            return mock(ProducerImpl.class, withSettings()
                    .useConstructor(client, "persistent://public/default/key-based-batch-test",
                            producerConfiguration, new CompletableFuture<>(), 0, Schema.BYTES,
                            null, Optional.empty())
                    .defaultAnswer(CALLS_REAL_METHODS));
        }
    }
}
