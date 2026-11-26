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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
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
                {KeyKind.TEXT, KeyKind.BYTES, "\ud83d\ude80"},
                {KeyKind.TEXT, KeyKind.BYTES, ""},
                {KeyKind.ORDERING, KeyKind.TEXT, ""},
                {KeyKind.TEXT, KeyKind.BYTES, "\0"},
                {KeyKind.TEXT, KeyKind.BYTES, "before\0after"},
                {KeyKind.TEXT, KeyKind.BYTES, "\u007f"},
                {KeyKind.TEXT, KeyKind.BYTES, "before\u007fafter"},
                {KeyKind.TEXT, KeyKind.BYTES, "\0\u007f"}
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
            assertWireMessages(ops.get(0), messages);
        }
    }

    @DataProvider
    public Object[][] differentKeys() {
        return new Object[][] {
                {KeyKind.BYTES, new byte[] {'a'}, KeyKind.TEXT, "YQ==".getBytes(StandardCharsets.UTF_8)},
                {KeyKind.ORDERING, new byte[] {'a'}, KeyKind.TEXT, "YQ==".getBytes(StandardCharsets.UTF_8)},
                {KeyKind.BYTES, new byte[] {(byte) 0x80}, KeyKind.BYTES, new byte[] {(byte) 0x81}},
                {KeyKind.BYTES, new byte[] {(byte) 0x80}, KeyKind.TEXT, "\u0080".getBytes(StandardCharsets.UTF_8)},
                {KeyKind.BYTES, new byte[] {(byte) 0xff}, KeyKind.TEXT, "\u00ff".getBytes(StandardCharsets.UTF_8)},
                {KeyKind.BYTES, new byte[] {(byte) 0xc0}, KeyKind.TEXT, "\u00c0".getBytes(StandardCharsets.UTF_8)},
                {KeyKind.BYTES, new byte[] {(byte) 0x80}, KeyKind.TEXT, "\ufffd".getBytes(StandardCharsets.UTF_8)}
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

    @Test
    public void testEveryByteValueHasAnIndependentBatchIdentity() throws Exception {
        try (BatchFixture fixture = new BatchFixture()) {
            for (int value = 0; value < 256; value++) {
                MessageImpl<?> message = fixture.message(value, KeyKind.BYTES, new byte[] {(byte) value});
                message.getMessageBuilder().setSchemaVersion(new byte[] {(byte) value});
                fixture.add(message);
            }
            for (int value = 0; value < 256; value++) {
                MessageImpl<?> message = fixture.message(256 + value, KeyKind.ORDERING, new byte[] {(byte) value});
                message.getMessageBuilder().setSchemaVersion(new byte[] {(byte) value});
                assertThat(fixture.container.hasSameSchema(message)).as("same schema for byte %s", value).isTrue();
                message.getMessageBuilder().setSchemaVersion(new byte[] {(byte) value, 0});
                assertThat(fixture.container.hasSameSchema(message)).as("different schema for byte %s", value)
                        .isFalse();
                message.getMessageBuilder().setSchemaVersion(new byte[] {(byte) value});
                fixture.add(message);
            }

            List<ProducerImpl.OpSendMsg> ops = fixture.flush();

            assertThat(ops).as("each raw byte value has its own batch").hasSize(256);
            for (int value = 0; value < 256; value++) {
                assertThat(ops.get(value).msgs).extracting(MessageImpl::getSequenceId)
                        .as("binary and ordering keys for byte %s", value)
                        .containsExactly((long) value, 256L + value);
                assertRoutingKey(ops.get(value), new byte[] {(byte) value});
            }
        }
    }

    @DataProvider
    public Object[][] noncanonicalBase64Keys() {
        return new Object[][] {{"YQ"}, {"YR=="}};
    }

    @Test(dataProvider = "noncanonicalBase64Keys")
    public void testEquivalentBase64SpellingsShareABatch(String encodedKey) throws Exception {
        try (BatchFixture fixture = new BatchFixture()) {
            byte[] rawKey = new byte[] {'a'};
            MessageImpl<?> first = fixture.message(0, KeyKind.BYTES, rawKey);
            first.getMessageBuilder().setSchemaVersion(new byte[] {1});
            fixture.add(first);
            MessageImpl<?> second = fixture.message(1, KeyKind.BYTES, rawKey);
            second.getMessageBuilder().setPartitionKey(encodedKey).setSchemaVersion(new byte[] {2});
            assertThat(Commands.resolveStickyKey(second.getMessageBuilder())).isEqualTo(rawKey);
            assertThat(fixture.container.hasSameSchema(second)).isFalse();
            second.getMessageBuilder().setSchemaVersion(new byte[] {1});
            fixture.add(second);
            MessageImpl<?> third = fixture.message(2, KeyKind.TEXT, rawKey);
            third.getMessageBuilder().setSchemaVersion(new byte[] {1});
            fixture.add(third);

            List<ProducerImpl.OpSendMsg> ops = fixture.flush();

            assertThat(ops).hasSize(1);
            assertThat(ops.get(0).msgs).extracting(MessageImpl::getSequenceId).containsExactly(0L, 1L, 2L);
            assertRoutingKey(ops.get(0), rawKey);
            assertThat(second.getKey()).as("the original Base64 spelling is preserved").isEqualTo(encodedKey);
        }
    }

    @DataProvider
    public Object[][] malformedUtf16Keys() {
        return new Object[][] {
                {"\ud800", new byte[] {'?'}},
                {"\udc00", new byte[] {'?'}},
                {"before\ud800after", "before?after".getBytes(StandardCharsets.UTF_8)},
                {"\ud800\ud800", new byte[] {'?', '?'}}
        };
    }

    @Test(dataProvider = "malformedUtf16Keys")
    public void testMalformedUtf16GroupingUsesBrokerReplacementBytesBeforeSerialization(String textKey, byte[] rawKey) {
        try (BatchFixture fixture = new BatchFixture()) {
            MessageImpl<?> first = fixture.message(0, builder -> builder.key(textKey));
            assertThat(Commands.resolveStickyKey(first.getMessageBuilder())).isEqualTo(rawKey);
            first.getMessageBuilder().setSchemaVersion(new byte[] {1});
            fixture.add(first);
            MessageImpl<?> binaryKey = fixture.message(1, KeyKind.BYTES, rawKey);
            binaryKey.getMessageBuilder().setSchemaVersion(new byte[] {2});
            MessageImpl<?> orderingKey = fixture.message(2, KeyKind.ORDERING, rawKey);
            orderingKey.getMessageBuilder().setSchemaVersion(new byte[] {2});

            assertThat(fixture.container.hasSameSchema(binaryKey))
                    .as("the binary key finds the batch for the same pre-serialization replacement bytes")
                    .isFalse();
            assertThat(fixture.container.hasSameSchema(orderingKey))
                    .as("the ordering key finds the batch for the same pre-serialization replacement bytes")
                    .isFalse();
        }
    }

    @Test
    public void testBatchIdentitySnapshotsCallerOwnedOrderingKey() {
        try (BatchFixture fixture = new BatchFixture()) {
            byte[] orderingKey = new byte[] {'a'};
            MessageImpl<?> first = fixture.message(0, KeyKind.ORDERING, orderingKey);
            first.getMessageBuilder().setSchemaVersion(new byte[] {1});
            fixture.add(first);
            orderingKey[0] = 'b';
            assertThat(first.getOrderingKey()).as("the caller changed the shared metadata array")
                    .containsExactly((byte) 'b');
            MessageImpl<?> originalKey = fixture.message(1, KeyKind.BYTES, new byte[] {'a'});
            originalKey.getMessageBuilder().setSchemaVersion(new byte[] {2});
            MessageImpl<?> changedKey = fixture.message(2, KeyKind.BYTES, new byte[] {'b'});
            changedKey.getMessageBuilder().setSchemaVersion(new byte[] {2});

            assertThat(fixture.container.hasSameSchema(originalKey)).as("the original batch identity is retained")
                    .isFalse();
            assertThat(fixture.container.hasSameSchema(changedKey)).as("the mutation does not move the batch")
                    .isTrue();
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

    @DataProvider
    public Object[][] randomSeeds() {
        return new Object[][] {{0L}, {1L}, {0x5eedL}};
    }

    @Test(dataProvider = "randomSeeds")
    public void testRandomizedBatchingMatchesRawStickyKeyModel(long seed) throws Exception {
        Random random = new Random(seed);
        for (int round = 0; round < 20; round++) {
            try (BatchFixture fixture = new BatchFixture()) {
                Map<ByteBuffer, ExpectedBatch> expected = new LinkedHashMap<>();
                List<Consumer<TypedMessageBuilder<byte[]>>> keySetters = randomKeySetters(random);
                for (int sequenceId = 0; sequenceId < keySetters.size(); sequenceId++) {
                    Consumer<TypedMessageBuilder<byte[]>> setKey = keySetters.get(sequenceId);
                    byte[] payload = new byte[random.nextInt(33)];
                    random.nextBytes(payload);
                    MessageImpl<?> message = fixture.message(sequenceId, builder -> {
                        setKey.accept(builder);
                        builder.value(payload);
                    });
                    if (message.hasBase64EncodedKey() && message.getKey().endsWith("=") && random.nextBoolean()) {
                        message.getMessageBuilder().setPartitionKey(message.getKey().replace("=", ""));
                    }
                    ByteBuffer key = referenceKey(message.getMessageBuilder());
                    ExpectedBatch batch = expected.get(key);
                    if (batch == null) {
                        batch = new ExpectedBatch(new byte[] {(byte) (expected.size() >>> 8), (byte) expected.size()},
                                new ArrayList<>());
                        expected.put(key, batch);
                    } else {
                        message.getMessageBuilder().setSchemaVersion(new byte[] {0, 0, 0});
                        assertThat(fixture.container.hasSameSchema(message))
                                .as("a different schema for an existing key")
                                .isFalse();
                    }
                    message.getMessageBuilder().setSchemaVersion(batch.schemaVersion());
                    assertThat(fixture.container.hasSameSchema(message)).as("the reference batch schema").isTrue();
                    fixture.add(message);
                    batch.messages().add(message);
                }

                List<ProducerImpl.OpSendMsg> ops = fixture.flush();

                assertThat(ops).as("one batch per reference key").hasSize(expected.size());
                Set<ByteBuffer> sentKeys = new HashSet<>();
                List<MessageImpl<?>> sentMessages = new ArrayList<>();
                for (ProducerImpl.OpSendMsg op : ops) {
                    ByteBuffer key = referenceKey(metadataOf(op));
                    assertThat(sentKeys.add(key)).as("each key is sent in exactly one batch").isTrue();
                    ExpectedBatch batch = expected.get(key);
                    assertThat(batch).as("the wire routing key belongs to the reference model").isNotNull();
                    assertThat(op.msgs).as("messages retain their input order within each key")
                            .containsExactlyElementsOf(batch.messages());
                    assertWireMessages(op, batch.messages());
                    sentMessages.addAll(op.msgs);
                }
                assertThat(sentMessages).as("every input message is sent exactly once")
                        .containsExactlyInAnyOrderElementsOf(fixture.messages);
            } catch (AssertionError | Exception error) {
                throw new AssertionError("seed=" + seed + ", round=" + round, error);
            }
        }
    }

    private static List<Consumer<TypedMessageBuilder<byte[]>>> randomKeySetters(Random random) {
        List<Consumer<TypedMessageBuilder<byte[]>>> setters = new ArrayList<>();
        List<String> texts = new ArrayList<>(List.of("", "\0", "\u007f", "before\0after", "before\u007fafter"));
        for (int i = 0; i < 6; i++) {
            texts.add(randomText(random, i == 0));
        }
        for (String text : texts) {
            byte[] rawKey = text.getBytes(StandardCharsets.UTF_8);
            setters.add(builder -> builder.key(text));
            setters.add(builder -> builder.keyBytes(rawKey));
            setters.add(builder -> builder.key("unrelated-partition").orderingKey(rawKey));
        }
        for (int i = 0; i < 6; i++) {
            byte[] rawKey = new byte[random.nextInt(25)];
            random.nextBytes(rawKey);
            byte[] partitionKey = new byte[1 + random.nextInt(8)];
            random.nextBytes(partitionKey);
            // This text key collides with the binary key's stored spelling, rather than its raw bytes.
            String storedKey = Base64.getEncoder().encodeToString(rawKey);
            setters.add(builder -> builder.key(storedKey));
            setters.add(builder -> builder.keyBytes(rawKey));
            setters.add(builder -> builder.keyBytes(rawKey));
            setters.add(builder -> builder.keyBytes(partitionKey).orderingKey(rawKey));
        }
        for (int i = 0; i < 2; i++) {
            setters.add(builder -> { });
            setters.add(builder -> builder.key(null));
            setters.add(builder -> builder.keyBytes(null));
        }
        Collections.shuffle(setters, random);
        return setters;
    }

    private static String randomText(Random random, boolean asciiOnly) {
        StringBuilder text = new StringBuilder();
        int length = random.nextInt(13);
        for (int i = 0; i < length; i++) {
            int codePoint = asciiOnly ? random.nextInt(128) : switch (random.nextInt(4)) {
                case 0 -> random.nextInt(128);
                case 1 -> 128 + random.nextInt(0xd800 - 128);
                case 2 -> 0xe000 + random.nextInt(0x10000 - 0xe000);
                default -> 0x10000 + random.nextInt(0x110000 - 0x10000);
            };
            text.appendCodePoint(codePoint);
        }
        return text.toString();
    }

    private static ByteBuffer referenceKey(MessageMetadata metadata) {
        // Preserve the existing shared batch for unkeyed messages, without the producer/sequence fallback.
        return metadata.hasOrderingKey() || metadata.hasPartitionKey()
                ? ByteBuffer.wrap(Commands.resolveStickyKey(metadata).clone()) : null;
    }

    private record ExpectedBatch(byte[] schemaVersion, List<MessageImpl<?>> messages) { }

    private static void assertWireMessages(ProducerImpl.OpSendMsg op, List<MessageImpl<?>> messages) throws Exception {
        MessageMetadata batchMetadata = metadataOf(op);
        assertBatchKeyRepresentation(batchMetadata, messages.get(0).getMessageBuilder());
        ByteBuf batchPayload = op.cmd.getSecond().duplicate();
        for (int i = 0; i < messages.size(); i++) {
            SingleMessageMetadata singleMetadata = messages.size() == 1 ? null : new SingleMessageMetadata();
            ByteBuf payload = singleMetadata == null ? batchPayload.readRetainedSlice(batchPayload.readableBytes())
                    : Commands.deSerializeSingleMessageInBatch(batchPayload, singleMetadata, i, messages.size());
            try {
                MessageImpl<byte[]> decoded = singleMetadata == null
                        ? MessageImpl.create("test-topic", new MessageIdImpl(1, 2, -1), batchMetadata,
                                payload, Optional.empty(), null, Schema.BYTES, 0, true, Commands.DEFAULT_CONSUMER_EPOCH)
                        : MessageImpl.create("test-topic", new BatchMessageIdImpl(1, 2, -1, i),
                                batchMetadata, singleMetadata, payload, Optional.empty(), null, Schema.BYTES, 0, true,
                                Commands.DEFAULT_CONSUMER_EPOCH);
                try {
                    MessageImpl<?> original = messages.get(i);
                    assertKeyRepresentation(decoded, original);
                    assertThat(decoded.getSequenceId()).as("the original message sequence id")
                            .isEqualTo(original.getSequenceId());
                    assertThat(decoded.getData()).as("the original message payload").isEqualTo(original.getData());
                    assertThat(decoded.getSchemaVersion()).as("the original schema version")
                            .isEqualTo(original.getSchemaVersion());
                } finally {
                    decoded.release();
                }
            } finally {
                payload.release();
            }
        }
        assertThat(batchPayload.isReadable()).as("all serialized messages have been decoded").isFalse();
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
            return message(sequenceId, builder -> keyKind.apply(builder, key));
        }

        MessageImpl<?> message(long sequenceId, Consumer<TypedMessageBuilder<byte[]>> configureKey) {
            TypedMessageBuilderImpl<byte[]> builder = new TypedMessageBuilderImpl<>(producer, Schema.BYTES);
            builder.value(new byte[] {0}).sequenceId(sequenceId);
            configureKey.accept(builder);
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
