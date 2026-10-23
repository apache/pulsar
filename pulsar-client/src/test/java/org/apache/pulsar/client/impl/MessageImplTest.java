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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
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
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit test of {@link MessageImpl}.
 */
public class MessageImplTest {

    @DataProvider
    public Object[][] deadLetterCopyReleaseOrders() {
        List<Object[]> cases = new ArrayList<>();
        int[][] orders = {{0, 1, 2}, {0, 2, 1}, {1, 0, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0}};
        for (boolean batchMessage : new boolean[] {false, true}) {
            for (int[] order : orders) {
                cases.add(new Object[] {batchMessage, order});
            }
        }
        return cases.toArray(new Object[0][]);
    }

    @Test(dataProvider = "deadLetterCopyReleaseOrders")
    public void testDeadLetterCopiesHaveIndependentLifetimes(boolean batchMessage, int[] releaseOrder) {
        byte[] data = {1, 2, 3};
        byte[] orderingKey = {4, 5};
        ByteBuf payload = PulsarByteBufAllocator.DEFAULT.directBuffer(data.length, data.length).writeBytes(data);
        MessageMetadata metadata = new MessageMetadata().setPartitionKey("message-key").setOrderingKey(orderingKey)
                .setEventTime(123).setReplicatedFrom("source-cluster").setSchemaVersion(new byte[] {6});
        metadata.addProperty().setKey("property").setValue(batchMessage ? "batch-value" : "message-value");
        MessageIdImpl messageId;
        MessageImpl<byte[]> original;
        if (batchMessage) {
            metadata.setNumMessagesInBatch(2);
            messageId = new BatchMessageIdImpl(1, 2, 3, 0, 2, BatchMessageIdImpl.newAckSet(2));
            SingleMessageMetadata singleMetadata = new SingleMessageMetadata().setPartitionKey("message-key")
                    .setOrderingKey(orderingKey).setEventTime(123);
            singleMetadata.addProperty().setKey("property").setValue("message-value");
            original = new MessageImpl<>("test-topic", (BatchMessageIdImpl) messageId, metadata, singleMetadata,
                    payload, Optional.empty(), null, Schema.BYTES, 5, true, 7);
        } else {
            messageId = new MessageIdImpl(1, 2, 3);
            original = new MessageImpl<>("test-topic", messageId, metadata, payload, Optional.empty(), null,
                    Schema.BYTES, 5, true, 7);
        }
        payload.release();
        MessageImpl<byte[]> cached = null;
        MessageImpl<byte[]> sending = null;
        try {
            cached = original.copyForDeadLetter();
            sending = cached.copyForDeadLetter();
            List<MessageImpl<byte[]>> messages = List.of(original, cached, sending);
            assertThat(payload.refCnt()).as("original, cache and send each own one reference").isEqualTo(3);
            assertThat(cached.getDataBuffer()).isNotSameAs(original.getDataBuffer());
            assertThat(sending.getDataBuffer()).isNotSameAs(cached.getDataBuffer());
            assertThat(cached.getMessageBuilder()).isNotSameAs(original.getMessageBuilder());
            int remainingReferences = messages.size();
            for (int index : releaseOrder) {
                messages.get(index).release();
                assertThat(payload.refCnt()).as("each release consumes exactly its own payload reference")
                        .isEqualTo(--remainingReferences);
                for (MessageImpl<byte[]> message : messages) {
                    if (message.getPayload() == null) {
                        continue;
                    }
                    assertThat(message.getData()).isEqualTo(data);
                    assertThat(message.getMessageId()).isSameAs(messageId);
                    assertThat(message.getTopicName()).isEqualTo("test-topic");
                    assertThat(message.getProperties()).isEqualTo(Map.of("property", "message-value"));
                    assertThat(message.getKey()).isEqualTo("message-key");
                    assertThat(message.getOrderingKey()).isEqualTo(orderingKey);
                    assertThat(message.getEventTime()).isEqualTo(123);
                    assertThat(message.getRedeliveryCount()).isEqualTo(5);
                    assertThat(message.getConsumerEpoch()).isEqualTo(7);
                    assertThat(message.getReaderSchema()).containsSame(Schema.BYTES);
                    assertThat(message.getSchemaVersion()).isEqualTo(new byte[] {6});
                }
            }
        } finally {
            original.release();
            if (cached != null) {
                cached.release();
            }
            if (sending != null) {
                sending.release();
            }
        }
        assertThat(payload.refCnt()).as("all owners released").isZero();
    }

    @Test
    public void testDeadLetterCopyHasIndependentReaderIndex() {
        ByteBuf payload = PulsarByteBufAllocator.DEFAULT.directBuffer(2, 2).writeBytes(new byte[] {1, 2});
        MessageImpl<byte[]> original = new MessageImpl<>("test-topic", new MessageIdImpl(1, 2, 3),
                new MessageMetadata(), payload, Optional.empty(), null, Schema.BYTES, 1, true, 0);
        payload.release();
        MessageImpl<byte[]> copy = null;
        try {
            copy = original.copyForDeadLetter();
            original.getDataBuffer().readByte();
            assertThat(original.size()).isEqualTo(1);
            assertThat(copy.getData()).isEqualTo(new byte[] {1, 2});
        } finally {
            original.release();
            if (copy != null) {
                copy.release();
            }
        }
        assertThat(payload.refCnt()).isZero();
    }

    @Test
    public void testDeadLetterCopyOfNonPooledMessageReleasesItsOwnReference() {
        ByteBuf payload = PulsarByteBufAllocator.DEFAULT.directBuffer(2, 2).writeBytes(new byte[] {1, 2});
        MessageImpl<byte[]> original = new MessageImpl<>("test-topic", new MessageIdImpl(1, 2, 3),
                new MessageMetadata(), payload, Optional.empty(), null, Schema.BYTES, 1, false, 0);
        payload.release();
        ByteBuf originalPayload = original.getDataBuffer();
        MessageImpl<byte[]> copy = null;
        try {
            copy = original.copyForDeadLetter();
            assertThat(originalPayload.refCnt()).isEqualTo(2);
            copy.release();
            assertThat(originalPayload.refCnt()).as("the copy releases even when its source is not pooled")
                    .isEqualTo(1);
            assertThat(original.getData()).isEqualTo(new byte[] {1, 2});
        } finally {
            if (copy != null) {
                copy.release();
            }
            originalPayload.release();
            original.recycle();
        }
        assertThat(originalPayload.refCnt()).isZero();
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
