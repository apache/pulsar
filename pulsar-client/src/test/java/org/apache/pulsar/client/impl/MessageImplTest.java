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
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.fail;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.BooleanSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.impl.schema.SchemaTestUtils;
import org.apache.pulsar.client.impl.schema.generic.MultiVersionSchemaInfoProvider;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.BrokerEntryMetadata;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.api.proto.SingleMessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodecZLib;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.protocol.Commands.ChecksumType;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit test of {@link MessageImpl}.
 */
public class MessageImplTest {

    @DataProvider(name = "deserializedMessageBufferTypes")
    public Object[][] deserializedMessageBufferTypes() {
        return new Object[][]{{false, false}, {false, true}, {true, false}, {true, true}};
    }

    @Test(dataProvider = "deserializedMessageBufferTypes")
    public void testGetDataOfDeserializedMessage(boolean skipBrokerEntryMetadata, boolean directBuffer)
            throws Exception {
        byte[] expected = {1, 2, 3, 4};
        MessageMetadata metadata = new MessageMetadata().setProducerName("test-producer")
                .setSequenceId(1).setPublishTime(1);
        ByteBuf payload = Unpooled.wrappedBuffer(expected);
        try {
            ByteBuf serialized = Commands.serializeMetadataAndPayload(ChecksumType.Crc32c, metadata, payload);
            try {
                ByteBuf input = directBuffer ? Unpooled.directBuffer(serialized.readableBytes())
                        : Unpooled.buffer(serialized.readableBytes());
                try {
                    input.writeBytes(serialized);
                    MessageImpl<byte[]> message = skipBrokerEntryMetadata
                            ? MessageImpl.deserializeSkipBrokerEntryMetaData(input) : MessageImpl.deserialize(input);
                    try {
                        assertThat(input.readerIndex()).as("metadata was consumed by the deserializer").isPositive();
                        assertThat(message.size()).isEqualTo(expected.length);
                        assertRepeatedGetData(message, expected);
                    } finally {
                        message.recycle();
                    }
                } finally {
                    input.release();
                }
                assertThat(input.refCnt()).isZero();
            } finally {
                serialized.release();
            }
        } finally {
            payload.release();
        }
    }

    @Test(dataProvider = "messageBufferTypes")
    public void testGetDataOfPartiallyWrittenBuffer(boolean pooledMessage, boolean directBuffer) {
        byte[] expected = {1, 2, 3};
        ByteBuf source = directBuffer ? Unpooled.directBuffer(8) : Unpooled.buffer(8);
        try {
            source.writeBytes(expected);
            MessageImpl<byte[]> message = MessageImpl.create("test-topic", new MessageIdImpl(1, 2, -1),
                    new MessageMetadata(), source, Optional.empty(), null, Schema.BYTES, 0, pooledMessage,
                    Commands.DEFAULT_CONSUMER_EPOCH);
            try {
                assertRepeatedGetData(message, expected);
            } finally {
                if (pooledMessage) {
                    message.release();
                } else {
                    message.getDataBuffer().release();
                }
            }
        } finally {
            source.release();
        }
        assertThat(source.refCnt()).isZero();
    }

    @Test
    public void testGetDataReusesCompleteHeapArray() {
        byte[] expected = {1, 2, 3};
        MessageImpl<byte[]> message = MessageImpl.create(new MessageMetadata(), ByteBuffer.wrap(expected),
                Schema.BYTES, "test-topic");
        try {
            assertThat(message.getData()).isSameAs(expected);
            assertRepeatedGetData(message, expected);
        } finally {
            message.getDataBuffer().release();
            message.recycle();
        }
    }

    @DataProvider(name = "messageBufferTypes")
    public Object[][] messageBufferTypes() {
        return new Object[][]{
                {true, false},
                {true, true},
                {false, false},
                {false, true}
        };
    }

    @Test(dataProvider = "messageBufferTypes")
    public void testRepeatedGetDataOfBatchMessage(boolean pooledMessage, boolean directBuffer) {
        ByteBuf source = directBuffer ? Unpooled.directBuffer(6) : Unpooled.buffer(6);
        try {
            source.writeBytes(new byte[]{0, 1, 2, 3, 4, 5});
            ByteBuf payload = source.slice(1, 4).readerIndex(1);
            MessageImpl<byte[]> message = MessageImpl.create("test-topic", new BatchMessageIdImpl(1, 2, -1, 0),
                    new MessageMetadata().setNumMessagesInBatch(1), new SingleMessageMetadata(), payload,
                    Optional.empty(), null, Schema.BYTES, 0, pooledMessage, Commands.DEFAULT_CONSUMER_EPOCH);
            try {
                assertRepeatedGetData(message, new byte[]{2, 3, 4});
            } finally {
                if (pooledMessage) {
                    message.release();
                } else {
                    message.getDataBuffer().release();
                }
            }
        } finally {
            source.release();
        }
        assertThat(source.refCnt()).as("source buffer references after message release").isZero();
    }

    @Test
    public void testRepeatedGetDataOfZlibBatchMessages() throws Exception {
        byte[][] expected = {{1, 2, 3, 4}, {5, 6, 7}};
        ByteBuf batch = Unpooled.buffer();
        try {
            for (byte[] data : expected) {
                ByteBuf payload = Unpooled.wrappedBuffer(data);
                try {
                    Commands.serializeSingleMessageInBatchWithPayload(new SingleMessageMetadata(), payload, batch);
                } finally {
                    payload.release();
                }
            }
            CompressionCodecZLib codec = new CompressionCodecZLib();
            ByteBuf compressed = codec.encode(batch);
            try {
                ByteBuf uncompressed = codec.decode(compressed, batch.readableBytes());
                try {
                    assertThat(uncompressed.hasArray()).as("ZLIB decoded batch uses heap memory").isTrue();
                    for (int i = 0; i < expected.length; i++) {
                        SingleMessageMetadata singleMetadata = new SingleMessageMetadata();
                        ByteBuf payload = Commands.deSerializeSingleMessageInBatch(
                                uncompressed, singleMetadata, i, expected.length);
                        try {
                            MessageImpl<byte[]> message = MessageImpl.create("test-topic",
                                    new BatchMessageIdImpl(1, 2, -1, i),
                                    new MessageMetadata().setNumMessagesInBatch(expected.length), singleMetadata,
                                    payload, Optional.empty(), null, Schema.BYTES, 0, true,
                                    Commands.DEFAULT_CONSUMER_EPOCH);
                            try {
                                assertRepeatedGetData(message, expected[i]);
                            } finally {
                                message.release();
                            }
                        } finally {
                            payload.release();
                        }
                    }
                } finally {
                    uncompressed.release();
                }
                assertThat(uncompressed.refCnt()).as("decoded batch references after message release").isZero();
            } finally {
                compressed.release();
            }
        } finally {
            batch.release();
        }
    }

    private static void assertRepeatedGetData(MessageImpl<byte[]> message, byte[] expected) {
        int readerIndex = message.getDataBuffer().readerIndex();
        for (int i = 0; i < 2; i++) {
            assertThat(message.getData()).as("payload on read %s", i).containsExactly(expected);
            assertThat(message.size()).as("message size after read %s", i).isEqualTo(expected.length);
            assertThat(message.getDataBuffer().readerIndex()).as("reader index after read %s", i)
                    .isEqualTo(readerIndex);
        }
    }

    @Test
    public void testOutgoingBrokerMetadataBeforeAndAfterRecycle() {
        MessageImpl<byte[]> message = MessageImpl.create(new MessageMetadata(),
                ByteBuffer.wrap(new byte[1]), Schema.BYTES, "test-topic");
        try {
            assertFalse(message.hasBrokerPublishTime());
            assertFalse(message.hasIndex());
            assertTrue(message.getBrokerPublishTime().isEmpty());
            assertTrue(message.getIndex().isEmpty());
            message.setBrokerEntryMetadata(new BrokerEntryMetadata().setBrokerTimestamp(123).setIndex(456));
            assertEquals(message.getBrokerPublishTime().orElseThrow().longValue(), 123L);
            assertEquals(message.getIndex().orElseThrow().longValue(), 456L);
        } finally {
            message.getDataBuffer().release();
            message.recycle();
        }

        MessageImpl<byte[]> next = MessageImpl.create(new MessageMetadata(),
                ByteBuffer.wrap(new byte[1]), Schema.BYTES, "test-topic");
        try {
            assertFalse(next.hasBrokerPublishTime());
            assertFalse(next.hasIndex());
            assertTrue(next.getBrokerPublishTime().isEmpty());
            assertTrue(next.getIndex().isEmpty());
        } finally {
            next.getDataBuffer().release();
            next.recycle();
        }
    }

    @DataProvider(name = "releaseOrder")
    public Object[][] releaseOrder() {
        return new Object[][]{{true}, {false}};
    }

    @Test(dataProvider = "releaseOrder")
    public void testReleaseKeepsBrokerMetadataOfPooledBatchSiblings(boolean releaseFirstMessageFirst) {
        BrokerEntryMetadata sharedBrokerMetadata = new BrokerEntryMetadata().setBrokerTimestamp(123).setIndex(456);
        List<MessageImpl<byte[]>> liveMessages = new ArrayList<>();
        try {
            liveMessages.add(newPooledBatchMessage(0, sharedBrokerMetadata));
            liveMessages.add(newPooledBatchMessage(1, sharedBrokerMetadata));
            int releasedBatchIndex = releaseFirstMessageFirst ? 0 : 1;
            int liveBatchIndex = 1 - releasedBatchIndex;
            MessageImpl<byte[]> live = liveMessages.get(liveBatchIndex);

            // a released message is no longer live, so it must not be released again in the finally block
            liveMessages.remove(releasedBatchIndex).release();

            assertTrue(live.hasBrokerPublishTime());
            assertEquals(live.getBrokerPublishTime().orElseThrow().longValue(), 123L);
            assertTrue(live.hasIndex());
            // the index of a batch message is the entry index minus the number of messages after it in the batch
            assertEquals(live.getIndex().orElseThrow().longValue(), 455L + liveBatchIndex);
            assertTrue(sharedBrokerMetadata.hasBrokerTimestamp());
            assertTrue(sharedBrokerMetadata.hasIndex());
        } finally {
            liveMessages.forEach(MessageImpl::release);
        }
    }

    private static MessageImpl<byte[]> newPooledBatchMessage(int batchIndex, BrokerEntryMetadata brokerMetadata) {
        ByteBuf payload = Unpooled.wrappedBuffer(new byte[]{(byte) batchIndex});
        try {
            MessageImpl<byte[]> message = MessageImpl.create("test-topic",
                    new BatchMessageIdImpl(1, 2, -1, batchIndex, 2, null),
                    new MessageMetadata().setNumMessagesInBatch(2), new SingleMessageMetadata(), payload,
                    Optional.empty(), null, Schema.BYTES, 0, true, Commands.DEFAULT_CONSUMER_EPOCH);
            message.setBrokerEntryMetadata(brokerMetadata);
            return message;
        } finally {
            // the pooled message retains the payload, so release the reference owned by this method
            payload.release();
        }
    }

    @Test
    public void testGetSequenceIdNotAssociated() {
        ByteBuffer payload = ByteBuffer.wrap(new byte[0]);
        MessageImpl<?> msg = MessageImpl.create(new MessageMetadata(), payload, Schema.BYTES, null);

        assertEquals(-1, msg.getSequenceId());
    }

    @Test
    public void testSetDuplicatePropertiesKey() {
        MessageMetadata builder = new MessageMetadata();
        builder.addProperty().setKey("key1").setValue("value1");
        builder.addProperty().setKey("key1").setValue("value2");
        builder.addProperty().setKey("key3").setValue("value3");
        ByteBuffer payload = ByteBuffer.wrap(new byte[0]);
        MessageImpl<?> msg = MessageImpl.create(builder, payload, Schema.BYTES, null);
        assertEquals("value2", msg.getProperty("key1"));
        assertEquals("value3", msg.getProperty("key3"));
    }

    @Test
    public void testGetSequenceIdAssociated() {
        MessageMetadata builder = new MessageMetadata()
            .setSequenceId(1234);

        ByteBuffer payload = ByteBuffer.wrap(new byte[0]);
        MessageImpl<?> msg = MessageImpl.create(builder, payload, Schema.BYTES, null);

        assertEquals(1234, msg.getSequenceId());
    }

    @Test
    public void testGetProducerNameNotAssigned() {
        MessageMetadata builder = new MessageMetadata();
        ByteBuffer payload = ByteBuffer.wrap(new byte[0]);
        MessageImpl<?> msg = MessageImpl.create(builder, payload, Schema.BYTES, null);

        assertNull(msg.getProducerName());
    }

    @Test
    public void testGetProducerNameAssigned() {
        MessageMetadata builder = new MessageMetadata()
            .setProducerName("test-producer");

        ByteBuffer payload = ByteBuffer.wrap(new byte[0]);
        MessageImpl<?> msg = MessageImpl.create(builder, payload, Schema.BYTES, null);

        assertEquals("test-producer", msg.getProducerName());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDefaultGetProducerDataAssigned() {
        AvroSchema<SchemaTestUtils.Foo> fooSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Foo>builder().withPojo(SchemaTestUtils.Foo.class).build());
        AvroSchema<SchemaTestUtils.Bar> barSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Bar>builder().withPojo(SchemaTestUtils.Bar.class).build());

        Schema<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> keyValueSchema =
                Schema.KeyValue(fooSchema, barSchema);
        SchemaTestUtils.Foo foo = new SchemaTestUtils.Foo();
        foo.setField1("field1");
        foo.setField2("field2");
        foo.setField3(3);
        SchemaTestUtils.Bar bar = new SchemaTestUtils.Bar();
        bar.setField1(true);

        // // Check kv.encoding.type default, not set value
        byte[] encodeBytes = keyValueSchema.encode(new KeyValue<>(foo, bar));
        MessageMetadata builder = new MessageMetadata()
                .setProducerName("default");
        MessageImpl<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> msg = MessageImpl.create(
                builder, ByteBuffer.wrap(encodeBytes), keyValueSchema, null);
        KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar> keyValue = msg.getValue();
        assertEquals(keyValue.getKey(), foo);
        assertEquals(keyValue.getValue(), bar);
        assertFalse(builder.hasPartitionKey());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testInlineGetProducerDataAssigned() {

        AvroSchema<SchemaTestUtils.Foo> fooSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Foo>builder().withPojo(SchemaTestUtils.Foo.class).build());
        AvroSchema<SchemaTestUtils.Bar> barSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Bar>builder().withPojo(SchemaTestUtils.Bar.class).build());

        Schema<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> keyValueSchema = Schema.KeyValue(
                fooSchema, barSchema, KeyValueEncodingType.INLINE);
        SchemaTestUtils.Foo foo = new SchemaTestUtils.Foo();
        foo.setField1("field1");
        foo.setField2("field2");
        foo.setField3(3);
        SchemaTestUtils.Bar bar = new SchemaTestUtils.Bar();
        bar.setField1(true);

        // Check kv.encoding.type INLINE
        byte[] encodeBytes = keyValueSchema.encode(new KeyValue<>(foo, bar));
        MessageMetadata builder = new MessageMetadata()
                .setProducerName("inline");
        MessageImpl<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> msg = MessageImpl.create(
                builder, ByteBuffer.wrap(encodeBytes), keyValueSchema, null);
        KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar> keyValue = msg.getValue();
        assertEquals(keyValue.getKey(), foo);
        assertEquals(keyValue.getValue(), bar);
        assertFalse(builder.hasPartitionKey());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSeparatedGetProducerDataAssigned() {
        AvroSchema<SchemaTestUtils.Foo> fooSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Foo>builder().withPojo(SchemaTestUtils.Foo.class).build());
        AvroSchema<SchemaTestUtils.Bar> barSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Bar>builder().withPojo(SchemaTestUtils.Bar.class).build());

        Schema<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> keyValueSchema = Schema.KeyValue(
                fooSchema, barSchema, KeyValueEncodingType.SEPARATED);
        SchemaTestUtils.Foo foo = new SchemaTestUtils.Foo();
        foo.setField1("field1");
        foo.setField2("field2");
        foo.setField3(3);
        SchemaTestUtils.Bar bar = new SchemaTestUtils.Bar();
        bar.setField1(true);

        // Check kv.encoding.type SPRAERATE
        byte[] encodeBytes = keyValueSchema.encode(new KeyValue<>(foo, bar));
        MessageMetadata builder = new MessageMetadata()
                .setProducerName("separated");
        builder.setPartitionKey(Base64.getEncoder().encodeToString(fooSchema.encode(foo)));
        builder.setPartitionKeyB64Encoded(true);
        MessageImpl<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> msg = MessageImpl.create(
                builder, ByteBuffer.wrap(encodeBytes), keyValueSchema, null);
        KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar> keyValue = msg.getValue();
        assertEquals(keyValue.getKey(), foo);
        assertEquals(keyValue.getValue(), bar);
        assertTrue(builder.hasPartitionKey());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDefaultAVROVersionGetProducerDataAssigned() {
        AvroSchema<SchemaTestUtils.Foo> fooSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Foo>builder().withPojo(SchemaTestUtils.Foo.class).build());
        AvroSchema<SchemaTestUtils.Bar> barSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Bar>builder().withPojo(SchemaTestUtils.Bar.class).build());

        MultiVersionSchemaInfoProvider multiVersionSchemaInfoProvider = mock(MultiVersionSchemaInfoProvider.class);
        Schema<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> keyValueSchema =
                Schema.KeyValue(fooSchema, barSchema);
        keyValueSchema.setSchemaInfoProvider(multiVersionSchemaInfoProvider);
        when(multiVersionSchemaInfoProvider.getSchemaByVersion(any(byte[].class)))
                .thenReturn(CompletableFuture.completedFuture(keyValueSchema.getSchemaInfo()));

        SchemaTestUtils.Foo foo = new SchemaTestUtils.Foo();
        foo.setField1("field1");
        foo.setField2("field2");
        foo.setField3(3);
        SchemaTestUtils.Bar bar = new SchemaTestUtils.Bar();
        bar.setField1(true);

        byte[] encodeBytes = keyValueSchema.encode(new KeyValue<>(foo, bar));
        MessageMetadata builder = new MessageMetadata()
                .setProducerName("default");
        builder.setSchemaVersion(new byte[10]);
        MessageImpl<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> msg = MessageImpl.create(
                builder, ByteBuffer.wrap(encodeBytes), keyValueSchema, null);
        KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar> keyValue = msg.getValue();
        assertEquals(keyValue.getKey(), foo);
        assertEquals(keyValue.getValue(), bar);
        assertFalse(builder.hasPartitionKey());
        Assert.assertEquals(
                KeyValueEncodingType.valueOf(keyValueSchema.getSchemaInfo().getProperties().get("kv.encoding.type")),
                KeyValueEncodingType.INLINE);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSeparatedAVROVersionGetProducerDataAssigned() {
        AvroSchema<SchemaTestUtils.Foo> fooSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Foo>builder().withPojo(SchemaTestUtils.Foo.class).build());
        AvroSchema<SchemaTestUtils.Bar> barSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Bar>builder().withPojo(SchemaTestUtils.Bar.class).build());

        MultiVersionSchemaInfoProvider multiVersionSchemaInfoProvider = mock(MultiVersionSchemaInfoProvider.class);
        Schema<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> keyValueSchema = Schema.KeyValue(
                fooSchema, barSchema, KeyValueEncodingType.SEPARATED);
        keyValueSchema.setSchemaInfoProvider(multiVersionSchemaInfoProvider);
        when(multiVersionSchemaInfoProvider.getSchemaByVersion(any(byte[].class)))
                .thenReturn(CompletableFuture.completedFuture(keyValueSchema.getSchemaInfo()));

        SchemaTestUtils.Foo foo = new SchemaTestUtils.Foo();
        foo.setField1("field1");
        foo.setField2("field2");
        foo.setField3(3);
        SchemaTestUtils.Bar bar = new SchemaTestUtils.Bar();
        bar.setField1(true);

        byte[] encodeBytes = keyValueSchema.encode(new KeyValue<>(foo, bar));
        MessageMetadata builder = new MessageMetadata()
                .setProducerName("separated");
        builder.setSchemaVersion(new byte[10]);
        builder.setPartitionKey(Base64.getEncoder().encodeToString(fooSchema.encode(foo)));
        builder.setPartitionKeyB64Encoded(true);
        MessageImpl<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> msg = MessageImpl.create(
                builder, ByteBuffer.wrap(encodeBytes), keyValueSchema, null);
        KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar> keyValue = msg.getValue();
        assertEquals(keyValue.getKey(), foo);
        assertEquals(keyValue.getValue(), bar);
        assertTrue(builder.hasPartitionKey());
        Assert.assertEquals(
                KeyValueEncodingType.valueOf(keyValueSchema.getSchemaInfo().getProperties().get("kv.encoding.type")),
                KeyValueEncodingType.SEPARATED);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDefaultJSONVersionGetProducerDataAssigned() {
        JSONSchema<SchemaTestUtils.Foo> fooSchema = JSONSchema.of(
                SchemaDefinition.<SchemaTestUtils.Foo>builder().withPojo(SchemaTestUtils.Foo.class).build());
        JSONSchema<SchemaTestUtils.Bar> barSchema = JSONSchema.of(
                SchemaDefinition.<SchemaTestUtils.Bar>builder().withPojo(SchemaTestUtils.Bar.class).build());

        MultiVersionSchemaInfoProvider multiVersionSchemaInfoProvider = mock(MultiVersionSchemaInfoProvider.class);
        Schema<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> keyValueSchema =
                Schema.KeyValue(fooSchema, barSchema);
        keyValueSchema.setSchemaInfoProvider(multiVersionSchemaInfoProvider);
        when(multiVersionSchemaInfoProvider.getSchemaByVersion(any(byte[].class)))
                .thenReturn(CompletableFuture.completedFuture(keyValueSchema.getSchemaInfo()));

        SchemaTestUtils.Foo foo = new SchemaTestUtils.Foo();
        foo.setField1("field1");
        foo.setField2("field2");
        foo.setField3(3);
        SchemaTestUtils.Bar bar = new SchemaTestUtils.Bar();
        bar.setField1(true);

        byte[] encodeBytes = keyValueSchema.encode(new KeyValue<>(foo, bar));
        MessageMetadata builder = new MessageMetadata()
                .setProducerName("default");
        builder.setSchemaVersion(new byte[10]);
        MessageImpl<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> msg = MessageImpl.create(
                builder, ByteBuffer.wrap(encodeBytes), keyValueSchema, null);
        KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar> keyValue = msg.getValue();
        assertEquals(keyValue.getKey(), foo);
        assertEquals(keyValue.getValue(), bar);
        assertFalse(builder.hasPartitionKey());
        Assert.assertEquals(
                KeyValueEncodingType.valueOf(keyValueSchema.getSchemaInfo().getProperties().get("kv.encoding.type")),
                KeyValueEncodingType.INLINE);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSeparatedJSONVersionGetProducerDataAssigned() {
        JSONSchema<SchemaTestUtils.Foo> fooSchema = JSONSchema.of(
                SchemaDefinition.<SchemaTestUtils.Foo>builder().withPojo(SchemaTestUtils.Foo.class).build());
        JSONSchema<SchemaTestUtils.Bar> barSchema = JSONSchema.of(
                SchemaDefinition.<SchemaTestUtils.Bar>builder().withPojo(SchemaTestUtils.Bar.class).build());

        MultiVersionSchemaInfoProvider multiVersionSchemaInfoProvider = mock(MultiVersionSchemaInfoProvider.class);
        Schema<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> keyValueSchema = Schema.KeyValue(
                fooSchema, barSchema, KeyValueEncodingType.SEPARATED);
        keyValueSchema.setSchemaInfoProvider(multiVersionSchemaInfoProvider);
        when(multiVersionSchemaInfoProvider.getSchemaByVersion(any(byte[].class)))
                .thenReturn(CompletableFuture.completedFuture(keyValueSchema.getSchemaInfo()));

        SchemaTestUtils.Foo foo = new SchemaTestUtils.Foo();
        foo.setField1("field1");
        foo.setField2("field2");
        foo.setField3(3);
        SchemaTestUtils.Bar bar = new SchemaTestUtils.Bar();
        bar.setField1(true);

        byte[] encodeBytes = keyValueSchema.encode(new KeyValue<>(foo, bar));
        MessageMetadata builder = new MessageMetadata()
                .setProducerName("separated");
        builder.setSchemaVersion(new byte[10]);
        builder.setPartitionKey(Base64.getEncoder().encodeToString(fooSchema.encode(foo)));
        builder.setPartitionKeyB64Encoded(true);
        MessageImpl<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> msg = MessageImpl.create(
                builder, ByteBuffer.wrap(encodeBytes), keyValueSchema, null);
        KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar> keyValue = msg.getValue();
        assertEquals(keyValue.getKey(), foo);
        assertEquals(keyValue.getValue(), bar);
        assertTrue(builder.hasPartitionKey());
        Assert.assertEquals(
                KeyValueEncodingType.valueOf(keyValueSchema.getSchemaInfo().getProperties().get("kv.encoding.type")),
                KeyValueEncodingType.SEPARATED);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testDefaultAVROJSONVersionGetProducerDataAssigned() {
        AvroSchema<SchemaTestUtils.Foo> fooSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Foo>builder().withPojo(SchemaTestUtils.Foo.class).build());
        JSONSchema<SchemaTestUtils.Bar> barSchema = JSONSchema.of(
                SchemaDefinition.<SchemaTestUtils.Bar>builder().withPojo(SchemaTestUtils.Bar.class).build());

        MultiVersionSchemaInfoProvider multiVersionSchemaInfoProvider = mock(MultiVersionSchemaInfoProvider.class);
        Schema<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> keyValueSchema =
                Schema.KeyValue(fooSchema, barSchema);
        keyValueSchema.setSchemaInfoProvider(multiVersionSchemaInfoProvider);
        when(multiVersionSchemaInfoProvider.getSchemaByVersion(any(byte[].class)))
                .thenReturn(CompletableFuture.completedFuture(keyValueSchema.getSchemaInfo()));

        SchemaTestUtils.Foo foo = new SchemaTestUtils.Foo();
        foo.setField1("field1");
        foo.setField2("field2");
        foo.setField3(3);
        SchemaTestUtils.Bar bar = new SchemaTestUtils.Bar();
        bar.setField1(true);

        byte[] encodeBytes = keyValueSchema.encode(new KeyValue<>(foo, bar));
        MessageMetadata builder = new MessageMetadata()
                .setProducerName("default");
        builder.setSchemaVersion(new byte[10]);
        MessageImpl<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> msg = MessageImpl.create(
                builder, ByteBuffer.wrap(encodeBytes), keyValueSchema, null);
        KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar> keyValue = msg.getValue();
        assertEquals(keyValue.getKey(), foo);
        assertEquals(keyValue.getValue(), bar);
        assertFalse(builder.hasPartitionKey());
        Assert.assertEquals(
                KeyValueEncodingType.valueOf(keyValueSchema.getSchemaInfo().getProperties().get("kv.encoding.type")),
                KeyValueEncodingType.INLINE);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSeparatedAVROJSONVersionGetProducerDataAssigned() {
        AvroSchema<SchemaTestUtils.Foo> fooSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Foo>builder().withPojo(SchemaTestUtils.Foo.class).build());
        JSONSchema<SchemaTestUtils.Bar> barSchema = JSONSchema.of(
                SchemaDefinition.<SchemaTestUtils.Bar>builder().withPojo(SchemaTestUtils.Bar.class).build());

        MultiVersionSchemaInfoProvider multiVersionSchemaInfoProvider = mock(MultiVersionSchemaInfoProvider.class);
        Schema<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> keyValueSchema = Schema.KeyValue(
                fooSchema, barSchema, KeyValueEncodingType.SEPARATED);
        keyValueSchema.setSchemaInfoProvider(multiVersionSchemaInfoProvider);
        when(multiVersionSchemaInfoProvider.getSchemaByVersion(any(byte[].class)))
                .thenReturn(CompletableFuture.completedFuture(keyValueSchema.getSchemaInfo()));

        SchemaTestUtils.Foo foo = new SchemaTestUtils.Foo();
        foo.setField1("field1");
        foo.setField2("field2");
        foo.setField3(3);
        SchemaTestUtils.Bar bar = new SchemaTestUtils.Bar();
        bar.setField1(true);

        byte[] encodeBytes = keyValueSchema.encode(new KeyValue<>(foo, bar));
        MessageMetadata builder = new MessageMetadata()
                .setProducerName("separated");
        builder.setSchemaVersion(new byte[10]);
        builder.setPartitionKey(Base64.getEncoder().encodeToString(fooSchema.encode(foo)));
        builder.setPartitionKeyB64Encoded(true);
        MessageImpl<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> msg = MessageImpl.create(
                builder, ByteBuffer.wrap(encodeBytes), keyValueSchema, null);
        KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar> keyValue = msg.getValue();
        assertEquals(keyValue.getKey(), foo);
        assertEquals(keyValue.getValue(), bar);
        assertTrue(builder.hasPartitionKey());
        Assert.assertEquals(
                KeyValueEncodingType.valueOf(keyValueSchema.getSchemaInfo().getProperties().get("kv.encoding.type")),
                KeyValueEncodingType.SEPARATED);
    }

    @Test
    public void testTypedSchemaGetNullValue() {
        byte[] encodeBytes = new byte[0];
        MessageMetadata builder = new MessageMetadata()
                .setProducerName("valueNotSet");
        builder.setSchemaVersion(new byte[0]);
        builder.setPartitionKey(Base64.getEncoder().encodeToString(encodeBytes));
        builder.setPartitionKeyB64Encoded(true);
        builder.setNullValue(true);
        MessageImpl<Boolean> msg = MessageImpl.create(builder, ByteBuffer.wrap(encodeBytes), BooleanSchema.of(), null);
        assertNull(msg.getValue());
    }

    @Test(timeOut = 30000)
    public void testMessageBrokerAndEntryMetadataTimestampMissed() {
        int mockBatchSize = 10;
        String data = "test-message";
        ByteBuf byteBuf = PulsarByteBufAllocator.DEFAULT.buffer(data.length(), data.length());
        byteBuf.writeBytes(data.getBytes(StandardCharsets.UTF_8));

        try {
            // test BrokerTimestamp not set.
            MessageMetadata messageMetadata = new MessageMetadata()
                    .setPublishTime(1)
                    .setProducerName("test")
                    .setSequenceId(1);
            byteBuf = Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, messageMetadata, byteBuf);
            BrokerEntryMetadata brokerMetadata = new BrokerEntryMetadata()
                            .setIndex(mockBatchSize - 1);

            int brokerMetaSize = brokerMetadata.getSerializedSize();
            ByteBuf  brokerMeta = PulsarByteBufAllocator.DEFAULT.buffer(brokerMetaSize + 6, brokerMetaSize + 6);
            brokerMeta.writeShort(Commands.magicBrokerEntryMetadata);
            brokerMeta.writeInt(brokerMetaSize);
            brokerMetadata.writeTo(brokerMeta);

            CompositeByteBuf compositeByteBuf = PulsarByteBufAllocator.DEFAULT.compositeBuffer();
            compositeByteBuf.addComponents(true, brokerMeta, byteBuf);
            long entryTimestamp = Commands.getEntryTimestamp(compositeByteBuf);
            assertTrue(MessageImpl.isEntryExpired(100, entryTimestamp));
            assertEquals(entryTimestamp, 1);

            // test BrokerTimestamp set.
            byteBuf = PulsarByteBufAllocator.DEFAULT.buffer(data.length(), data.length());
            byteBuf.writeBytes(data.getBytes(StandardCharsets.UTF_8));
            messageMetadata = new MessageMetadata()
                    .setPublishTime(System.currentTimeMillis())
                    .setProducerName("test")
                    .setSequenceId(1);
            byteBuf = Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, messageMetadata, byteBuf);
            long brokerEntryTimestamp = System.currentTimeMillis();
            brokerMetadata = new BrokerEntryMetadata()
                    .setBrokerTimestamp(brokerEntryTimestamp)
                    .setIndex(mockBatchSize - 1);

            brokerMetaSize = brokerMetadata.getSerializedSize();
            brokerMeta = PulsarByteBufAllocator.DEFAULT.buffer(brokerMetaSize + 6, brokerMetaSize + 6);
            brokerMeta.writeShort(Commands.magicBrokerEntryMetadata);
            brokerMeta.writeInt(brokerMetaSize);
            brokerMetadata.writeTo(brokerMeta);

            compositeByteBuf = PulsarByteBufAllocator.DEFAULT.compositeBuffer();
            compositeByteBuf.addComponents(true, brokerMeta, byteBuf);
            entryTimestamp = Commands.getEntryTimestamp(compositeByteBuf);
            assertFalse(MessageImpl.isEntryExpired(24 * 3600, entryTimestamp));
            assertEquals(entryTimestamp, brokerEntryTimestamp);
        } catch (Exception e) {
            fail();
        }
    }

    @Test(timeOut = 30000)
    public void testParseMessageMetadataWithBrokerEntryMetadata() {
        int mockBatchSize = 10;
        String data = "test-message";
        ByteBuf byteBuf = PulsarByteBufAllocator.DEFAULT.buffer(data.length(), data.length());
        byteBuf.writeBytes(data.getBytes(StandardCharsets.UTF_8));

        // first, build a message with broker entry metadata

        // build message metadata
        MessageMetadata messageMetadata = new MessageMetadata()
                .setPublishTime(1)
                .setProducerName("test")
                .setSequenceId(1);
        byteBuf = Commands.serializeMetadataAndPayload(Commands.ChecksumType.Crc32c, messageMetadata, byteBuf);

        // build broker entry metadata
        BrokerEntryMetadata brokerMetadata = new BrokerEntryMetadata()
                .setIndex(mockBatchSize - 1);

        // build final data which contains broker entry metadata
        int brokerMetaSize = brokerMetadata.getSerializedSize();
        ByteBuf  brokerMeta = PulsarByteBufAllocator.DEFAULT.buffer(brokerMetaSize + 6, brokerMetaSize + 6);
        brokerMeta.writeShort(Commands.magicBrokerEntryMetadata);
        brokerMeta.writeInt(brokerMetaSize);
        brokerMetadata.writeTo(brokerMeta);

        CompositeByteBuf compositeByteBuf = PulsarByteBufAllocator.DEFAULT.compositeBuffer();
        compositeByteBuf.addComponents(true, brokerMeta, byteBuf);

        CompositeByteBuf dupCompositeByteBuf = PulsarByteBufAllocator.DEFAULT.compositeBuffer();
        dupCompositeByteBuf.addComponents(true, brokerMeta, byteBuf);

        //second, parse message metadata without skip broker entry metadata
        Commands.skipChecksumIfPresent(compositeByteBuf);
        int metadataSize = (int) compositeByteBuf.readUnsignedInt();
        MessageMetadata md = new MessageMetadata();
        try {
            md.parseFrom(compositeByteBuf, metadataSize);
            Assert.fail("Parse operation should be failed.");
        } catch (IllegalArgumentException e) {
            // expected
        }

        //third, parse message metadata with skip broker entry metadata first
        MessageMetadata metadata = Commands.parseMessageMetadata(dupCompositeByteBuf);
        assertEquals(metadata.getPublishTime(), 1);
        assertEquals(metadata.getProducerName(), "test");
        assertEquals(metadata.getSequenceId(), 1);
    }
}
