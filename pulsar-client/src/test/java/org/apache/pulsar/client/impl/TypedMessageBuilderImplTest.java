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

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Base64;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.SchemaTestUtils;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.mockito.Mock;
import org.testng.annotations.Test;

/**
 * Unit test of {@link TypedMessageBuilderImpl}.
 */
public class TypedMessageBuilderImplTest {

    @Mock
    protected ProducerBase<?> producerBase;

    @Test
    @SuppressWarnings("unchecked")
    public void testDefaultValue() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        producerBase = mock(ProducerBase.class);

        AvroSchema<SchemaTestUtils.Foo> fooSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Foo>builder().withPojo(SchemaTestUtils.Foo.class).build());
        AvroSchema<SchemaTestUtils.Bar> barSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Bar>builder().withPojo(SchemaTestUtils.Bar.class).build());

        Schema<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> keyValueSchema =
                Schema.KeyValue(fooSchema, barSchema);
        @SuppressWarnings("rawtypes")
        TypedMessageBuilderImpl typedMessageBuilderImpl = new TypedMessageBuilderImpl(producerBase, keyValueSchema);

        SchemaTestUtils.Foo foo = new SchemaTestUtils.Foo();
        foo.setField1("field1");
        foo.setField2("field2");
        SchemaTestUtils.Bar bar = new SchemaTestUtils.Bar();
        bar.setField1(true);
        KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar> keyValue = new KeyValue<>(foo, bar);

        // Check kv.encoding.type default, not set value
        TypedMessageBuilderImpl<KeyValue<?, ?>>  typedMessageBuilder =
                (TypedMessageBuilderImpl<KeyValue<?, ?>>) typedMessageBuilderImpl.value(keyValue);
        Method method = TypedMessageBuilderImpl.class.getDeclaredMethod("beforeSend");
        method.setAccessible(true);
        method.invoke(typedMessageBuilder);
        ByteBuffer content = typedMessageBuilder.getContent();
        byte[] contentByte = new byte[content.remaining()];
        content.get(contentByte);
        KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>  decodeKeyValue = keyValueSchema.decode(contentByte);
        assertEquals(decodeKeyValue.getKey(), foo);
        assertEquals(decodeKeyValue.getValue(), bar);
        assertFalse(typedMessageBuilderImpl.hasKey());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testInlineValue() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        producerBase = mock(ProducerBase.class);

        AvroSchema<SchemaTestUtils.Foo> fooSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Foo>builder().withPojo(SchemaTestUtils.Foo.class).build());
        AvroSchema<SchemaTestUtils.Bar> barSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Bar>builder().withPojo(SchemaTestUtils.Bar.class).build());

        Schema<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> keyValueSchema =
                Schema.KeyValue(fooSchema, barSchema, KeyValueEncodingType.INLINE);
        @SuppressWarnings("rawtypes")
        TypedMessageBuilderImpl typedMessageBuilderImpl = new TypedMessageBuilderImpl(producerBase, keyValueSchema);

        SchemaTestUtils.Foo foo = new SchemaTestUtils.Foo();
        foo.setField1("field1");
        foo.setField2("field2");
        SchemaTestUtils.Bar bar = new SchemaTestUtils.Bar();
        bar.setField1(true);
        KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar> keyValue = new KeyValue<>(foo, bar);

        // Check kv.encoding.type INLINE
        TypedMessageBuilderImpl<KeyValue<?, ?>> typedMessageBuilder =
                (TypedMessageBuilderImpl<KeyValue<?, ?>>) typedMessageBuilderImpl.value(keyValue);
        Method method = TypedMessageBuilderImpl.class.getDeclaredMethod("beforeSend");
        method.setAccessible(true);
        method.invoke(typedMessageBuilder);
        ByteBuffer content = typedMessageBuilder.getContent();
        byte[] contentByte = new byte[content.remaining()];
        content.get(contentByte);
        KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar> decodeKeyValue = keyValueSchema.decode(contentByte);
        assertEquals(decodeKeyValue.getKey(), foo);
        assertEquals(decodeKeyValue.getValue(), bar);
        assertFalse(typedMessageBuilderImpl.hasKey());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSeparatedValue() throws Exception {
        producerBase = mock(ProducerBase.class);

        AvroSchema<SchemaTestUtils.Foo> fooSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Foo>builder().withPojo(SchemaTestUtils.Foo.class).build());
        AvroSchema<SchemaTestUtils.Bar> barSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Bar>builder().withPojo(SchemaTestUtils.Bar.class).build());

        Schema<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> keyValueSchema =
                Schema.KeyValue(fooSchema, barSchema, KeyValueEncodingType.SEPARATED);
        @SuppressWarnings("rawtypes")
        TypedMessageBuilderImpl typedMessageBuilderImpl = new TypedMessageBuilderImpl(producerBase, keyValueSchema);

        SchemaTestUtils.Foo foo = new SchemaTestUtils.Foo();
        foo.setField1("field1");
        foo.setField2("field2");
        SchemaTestUtils.Bar bar = new SchemaTestUtils.Bar();
        bar.setField1(true);
        KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar> keyValue = new KeyValue<>(foo, bar);

        // Check kv.encoding.type SEPARATED
        TypedMessageBuilderImpl<?> typedMessageBuilder =
                (TypedMessageBuilderImpl<?>) typedMessageBuilderImpl.value(keyValue);
        Method method = TypedMessageBuilderImpl.class.getDeclaredMethod("beforeSend");
        method.setAccessible(true);
        method.invoke(typedMessageBuilder);
        ByteBuffer content = typedMessageBuilder.getContent();
        byte[] contentByte = new byte[content.remaining()];
        content.get(contentByte);
        assertTrue(typedMessageBuilderImpl.hasKey());
        assertEquals(typedMessageBuilderImpl.getKey(),
                Base64.getEncoder().encodeToString(fooSchema.encode(foo)));
        assertEquals(barSchema.decode(contentByte), bar);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSetKeyEncodingTypeDefault() {
        producerBase = mock(ProducerBase.class);

        AvroSchema<SchemaTestUtils.Foo> fooSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Foo>builder().withPojo(SchemaTestUtils.Foo.class).build());
        AvroSchema<SchemaTestUtils.Bar> barSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Bar>builder().withPojo(SchemaTestUtils.Bar.class).build());

        Schema<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> keyValueSchema =
                Schema.KeyValue(fooSchema, barSchema);
        @SuppressWarnings("rawtypes")
        TypedMessageBuilderImpl typedMessageBuilderImpl = new TypedMessageBuilderImpl(producerBase, keyValueSchema);

        TypedMessageBuilderImpl<?> typedMessageBuilder =
                (TypedMessageBuilderImpl<?>) typedMessageBuilderImpl.key("default");
        assertEquals(typedMessageBuilder.getKey(), "default");
        assertFalse(typedMessageBuilder.getMetadataBuilder().isPartitionKeyB64Encoded());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSetKeyEncodingTypeInline() {
        producerBase = mock(ProducerBase.class);

        AvroSchema<SchemaTestUtils.Foo> fooSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Foo>builder().withPojo(SchemaTestUtils.Foo.class).build());
        AvroSchema<SchemaTestUtils.Bar> barSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Bar>builder().withPojo(SchemaTestUtils.Bar.class).build());

        Schema<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> keyValueSchema =
                Schema.KeyValue(fooSchema, barSchema, KeyValueEncodingType.INLINE);
        @SuppressWarnings("rawtypes")
        TypedMessageBuilderImpl typedMessageBuilderImpl = new TypedMessageBuilderImpl(producerBase, keyValueSchema);

        TypedMessageBuilderImpl<?> typedMessageBuilder =
                (TypedMessageBuilderImpl<?>) typedMessageBuilderImpl.key("inline");
        assertEquals(typedMessageBuilder.getKey(), "inline");
        assertFalse(typedMessageBuilder.getMetadataBuilder().isPartitionKeyB64Encoded());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSetKeyEncodingTypeSeparated() {
        producerBase = mock(ProducerBase.class);

        AvroSchema<SchemaTestUtils.Foo> fooSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Foo>builder().withPojo(SchemaTestUtils.Foo.class).build());
        AvroSchema<SchemaTestUtils.Bar> barSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Bar>builder().withPojo(SchemaTestUtils.Bar.class).build());

        Schema<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> keyValueSchema =
                Schema.KeyValue(fooSchema, barSchema, KeyValueEncodingType.SEPARATED);
        @SuppressWarnings("rawtypes")
        TypedMessageBuilderImpl typedMessageBuilderImpl = new TypedMessageBuilderImpl(producerBase, keyValueSchema);


        try {
            TypedMessageBuilderImpl<?> typedMessageBuilder =
                    (TypedMessageBuilderImpl<?>) typedMessageBuilderImpl.key("separated");
            fail("This should fail");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage()
                    .contains("This method is not allowed to set keys when in encoding type is SEPARATED"));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSetKeyBytesEncodingTypeDefault() {
        producerBase = mock(ProducerBase.class);

        AvroSchema<SchemaTestUtils.Foo> fooSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Foo>builder().withPojo(SchemaTestUtils.Foo.class).build());
        AvroSchema<SchemaTestUtils.Bar> barSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Bar>builder().withPojo(SchemaTestUtils.Bar.class).build());

        Schema<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> keyValueSchema =
                Schema.KeyValue(fooSchema, barSchema);
        @SuppressWarnings("rawtypes")
        TypedMessageBuilderImpl typedMessageBuilderImpl = new TypedMessageBuilderImpl(producerBase, keyValueSchema);

        TypedMessageBuilderImpl<?> typedMessageBuilder =
                (TypedMessageBuilderImpl<?>) typedMessageBuilderImpl.keyBytes("default".getBytes());
        assertEquals(typedMessageBuilder.getKey(), Base64.getEncoder().encodeToString("default".getBytes()));
        assertTrue(typedMessageBuilder.getMetadataBuilder().isPartitionKeyB64Encoded());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSetKeyBytesEncodingTypeInline() {
        producerBase = mock(ProducerBase.class);

        AvroSchema<SchemaTestUtils.Foo> fooSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Foo>builder().withPojo(SchemaTestUtils.Foo.class).build());
        AvroSchema<SchemaTestUtils.Bar> barSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Bar>builder().withPojo(SchemaTestUtils.Bar.class).build());

        Schema<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> keyValueSchema =
                Schema.KeyValue(fooSchema, barSchema, KeyValueEncodingType.INLINE);
        @SuppressWarnings("rawtypes")
        TypedMessageBuilderImpl typedMessageBuilderImpl = new TypedMessageBuilderImpl(producerBase, keyValueSchema);

        TypedMessageBuilderImpl<?> typedMessageBuilder =
                (TypedMessageBuilderImpl<?>) typedMessageBuilderImpl.keyBytes("inline".getBytes());
        assertEquals(typedMessageBuilder.getKey(), Base64.getEncoder().encodeToString("inline".getBytes()));
        assertTrue(typedMessageBuilder.getMetadataBuilder().isPartitionKeyB64Encoded());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testSetKeyBytesEncodingTypeSeparated() {
        producerBase = mock(ProducerBase.class);

        AvroSchema<SchemaTestUtils.Foo> fooSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Foo>builder().withPojo(SchemaTestUtils.Foo.class).build());
        AvroSchema<SchemaTestUtils.Bar> barSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Bar>builder().withPojo(SchemaTestUtils.Bar.class).build());

        Schema<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> keyValueSchema =
                Schema.KeyValue(fooSchema, barSchema, KeyValueEncodingType.SEPARATED);
        @SuppressWarnings("rawtypes")
        TypedMessageBuilderImpl typedMessageBuilderImpl = new TypedMessageBuilderImpl(producerBase, keyValueSchema);


        try {
            TypedMessageBuilderImpl<?> typedMessageBuilder =
                    (TypedMessageBuilderImpl<?>) typedMessageBuilderImpl.keyBytes("separated".getBytes());
            fail("This should fail");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage()
                    .contains("This method is not allowed to set keys when in encoding type is SEPARATED"));
        }
    }

    @Test
    public void testGetMessageWithNullProducer() {
        TypedMessageBuilderImpl<byte[]> builder = new TypedMessageBuilderImpl<>(null, Schema.BYTES);
        var data = "test".getBytes();
        builder.value(data);
        var message = builder.getMessage();
        assertEquals(message.getValue(), data);
    }

    @Test
    public void testMetadataRemainsIndependentWhenBuilderIsReused() {
        TypedMessageBuilderImpl<byte[]> builder = new TypedMessageBuilderImpl<>(null, Schema.BYTES);
        builder.value(new byte[] {1});
        MessageImpl<byte[]> first = (MessageImpl<byte[]>) builder.getMessage();
        MessageImpl<byte[]> second = null;
        MessageImpl<byte[]> third = null;
        try {
            // Producer-generated metadata on an emitted message must not become builder state.
            first.getMessageBuilder().setPublishTime(1234);
            builder.key("key").property("name", "value").sequenceId(5);
            second = (MessageImpl<byte[]>) builder.getMessage();
            assertFalse(first.hasKey());
            assertTrue(first.getProperties().isEmpty());
            assertEquals(first.getPublishTime(), 1234L);
            assertEquals(second.getKey(), "key");
            assertEquals(second.getProperty("name"), "value");
            assertEquals(second.getSequenceId(), 5L);
            assertFalse(second.getMessageBuilder().hasPublishTime());

            // A retained metadata accessor still updates future messages, not already emitted ones.
            var metadata = builder.getMetadataBuilder();
            metadata.setPartitionKey("changed");
            third = (MessageImpl<byte[]>) builder.getMessage();
            assertEquals(second.getKey(), "key");
            assertEquals(third.getKey(), "changed");
            assertFalse(third.getMessageBuilder().hasPublishTime());
        } finally {
            first.getDataBuffer().release();
            first.recycle();
            if (second != null) {
                second.getDataBuffer().release();
                second.recycle();
            }
            if (third != null) {
                third.getDataBuffer().release();
                third.recycle();
            }
        }
    }

    @Test
    public void testNullValueMaterializesMetadata() {
        TypedMessageBuilderImpl<byte[]> builder = new TypedMessageBuilderImpl<>(null, Schema.BYTES);
        builder.value(null);
        MessageImpl<byte[]> message = (MessageImpl<byte[]>) builder.getMessage();
        try {
            assertTrue(message.getMessageBuilder().isNullValue());
            assertFalse(message.hasKey());
        } finally {
            message.getDataBuffer().release();
            message.recycle();
        }
    }

}
