/**
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

import java.nio.ByteBuffer;
import java.util.Base64;

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.client.impl.schema.SchemaTestUtils;
import org.apache.pulsar.client.impl.schema.generic.MultiVersionSchemaInfoProvider;
import org.apache.pulsar.common.api.proto.PulsarApi.MessageMetadata;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.shaded.com.google.protobuf.v241.ByteString;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Unit test of {@link MessageImpl}.
 */
public class MessageImplTest {

    @Test
    public void testGetSequenceIdNotAssociated() {
        MessageMetadata.Builder builder = MessageMetadata.newBuilder();
        ByteBuffer payload = ByteBuffer.wrap(new byte[0]);
        MessageImpl<?> msg = MessageImpl.create(builder, payload, Schema.BYTES);

        assertEquals(-1, msg.getSequenceId());
    }

    @Test
    public void testSetDuplicatePropertiesKey() {
        MessageMetadata.Builder builder = MessageMetadata.newBuilder();
        builder.addProperties(org.apache.pulsar.common.api.proto.PulsarApi.KeyValue.newBuilder()
                .setKey("key1").setValue("value1").build());
        builder.addProperties(org.apache.pulsar.common.api.proto.PulsarApi.KeyValue.newBuilder()
                .setKey("key1").setValue("value2").build());
        builder.addProperties(org.apache.pulsar.common.api.proto.PulsarApi.KeyValue.newBuilder()
                .setKey("key3").setValue("value3").build());
        ByteBuffer payload = ByteBuffer.wrap(new byte[0]);
        MessageImpl<?> msg = MessageImpl.create(builder, payload, Schema.BYTES);
        assertEquals("value2", msg.getProperty("key1"));
        assertEquals("value3", msg.getProperty("key3"));
    }

    @Test
    public void testGetSequenceIdAssociated() {
        MessageMetadata.Builder builder = MessageMetadata.newBuilder()
            .setSequenceId(1234);

        ByteBuffer payload = ByteBuffer.wrap(new byte[0]);
        MessageImpl<?> msg = MessageImpl.create(builder, payload, Schema.BYTES);

        assertEquals(1234, msg.getSequenceId());
    }

    @Test
    public void testGetProducerNameNotAssigned() {
        MessageMetadata.Builder builder = MessageMetadata.newBuilder();
        ByteBuffer payload = ByteBuffer.wrap(new byte[0]);
        MessageImpl<?> msg = MessageImpl.create(builder, payload, Schema.BYTES);

        assertNull(msg.getProducerName());
    }

    @Test
    public void testGetProducerNameAssigned() {
        MessageMetadata.Builder builder = MessageMetadata.newBuilder()
            .setProducerName("test-producer");

        ByteBuffer payload = ByteBuffer.wrap(new byte[0]);
        MessageImpl<?> msg = MessageImpl.create(builder, payload, Schema.BYTES);

        assertEquals("test-producer", msg.getProducerName());
    }

    @Test
    public void testDefaultGetProducerDataAssigned() {
        AvroSchema<SchemaTestUtils.Foo> fooSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Foo>builder().withPojo(SchemaTestUtils.Foo.class).build());
        AvroSchema<SchemaTestUtils.Bar> barSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Bar>builder().withPojo(SchemaTestUtils.Bar.class).build());

        Schema<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> keyValueSchema = Schema.KeyValue(fooSchema, barSchema);
        SchemaTestUtils.Foo foo = new SchemaTestUtils.Foo();
        foo.setField1("field1");
        foo.setField2("field2");
        foo.setField3(3);
        SchemaTestUtils.Bar bar = new SchemaTestUtils.Bar();
        bar.setField1(true);

        // // Check kv.encoding.type default, not set value
        byte[] encodeBytes = keyValueSchema.encode(new KeyValue(foo, bar));
        MessageMetadata.Builder builder = MessageMetadata.newBuilder()
                .setProducerName("default");
        MessageImpl<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> msg = MessageImpl.create(
                builder, ByteBuffer.wrap(encodeBytes), keyValueSchema);
        KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar> keyValue = msg.getValue();
        assertEquals(keyValue.getKey(), foo);
        assertEquals(keyValue.getValue(), bar);
        assertFalse(builder.hasPartitionKey());
    }

    @Test
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
        byte[] encodeBytes = keyValueSchema.encode(new KeyValue(foo, bar));
        MessageMetadata.Builder builder = MessageMetadata.newBuilder()
                .setProducerName("inline");
        MessageImpl<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> msg = MessageImpl.create(
                builder, ByteBuffer.wrap(encodeBytes), keyValueSchema);
        KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar> keyValue = msg.getValue();
        assertEquals(keyValue.getKey(), foo);
        assertEquals(keyValue.getValue(), bar);
        assertFalse(builder.hasPartitionKey());
    }

    @Test
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
        byte[] encodeBytes = keyValueSchema.encode(new KeyValue(foo, bar));
        MessageMetadata.Builder builder = MessageMetadata.newBuilder()
                .setProducerName("separated");
        builder.setPartitionKey(Base64.getEncoder().encodeToString(fooSchema.encode(foo)));
        builder.setPartitionKeyB64Encoded(true);
        MessageImpl<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> msg = MessageImpl.create(
                builder, ByteBuffer.wrap(encodeBytes), keyValueSchema);
        KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar> keyValue = msg.getValue();
        assertEquals(keyValue.getKey(), foo);
        assertEquals(keyValue.getValue(), bar);
        assertTrue(builder.hasPartitionKey());
    }

    @Test
    public void testDefaultAVROVersionGetProducerDataAssigned() {
        AvroSchema<SchemaTestUtils.Foo> fooSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Foo>builder().withPojo(SchemaTestUtils.Foo.class).build());
        AvroSchema<SchemaTestUtils.Bar> barSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Bar>builder().withPojo(SchemaTestUtils.Bar.class).build());

        MultiVersionSchemaInfoProvider multiVersionSchemaInfoProvider = mock(MultiVersionSchemaInfoProvider.class);
        Schema<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> keyValueSchema = Schema.KeyValue(fooSchema, barSchema);
        keyValueSchema.setSchemaInfoProvider(multiVersionSchemaInfoProvider);
        when(multiVersionSchemaInfoProvider.getSchemaByVersion(any(byte[].class)))
                .thenReturn(CompletableFuture.completedFuture(keyValueSchema.getSchemaInfo()));

        SchemaTestUtils.Foo foo = new SchemaTestUtils.Foo();
        foo.setField1("field1");
        foo.setField2("field2");
        foo.setField3(3);
        SchemaTestUtils.Bar bar = new SchemaTestUtils.Bar();
        bar.setField1(true);

        byte[] encodeBytes = keyValueSchema.encode(new KeyValue(foo, bar));
        MessageMetadata.Builder builder = MessageMetadata.newBuilder()
                .setProducerName("default");
        ByteString byteString = ByteString.copyFrom(new byte[10]);
        builder.setSchemaVersion(byteString);
        MessageImpl<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> msg = MessageImpl.create(
                builder, ByteBuffer.wrap(encodeBytes), keyValueSchema);
        KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar> keyValue = msg.getValue();
        assertEquals(keyValue.getKey(), foo);
        assertEquals(keyValue.getValue(), bar);
        assertFalse(builder.hasPartitionKey());
        Assert.assertEquals(
                KeyValueEncodingType.valueOf(keyValueSchema.getSchemaInfo().getProperties().get("kv.encoding.type")),
                KeyValueEncodingType.INLINE);
    }

    @Test
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

        byte[] encodeBytes = keyValueSchema.encode(new KeyValue(foo, bar));
        MessageMetadata.Builder builder = MessageMetadata.newBuilder()
                .setProducerName("separated");
        ByteString byteString = ByteString.copyFrom(new byte[10]);
        builder.setSchemaVersion(byteString);
        builder.setPartitionKey(Base64.getEncoder().encodeToString(fooSchema.encode(foo)));
        builder.setPartitionKeyB64Encoded(true);
        MessageImpl<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> msg = MessageImpl.create(
                builder, ByteBuffer.wrap(encodeBytes), keyValueSchema);
        KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar> keyValue = msg.getValue();
        assertEquals(keyValue.getKey(), foo);
        assertEquals(keyValue.getValue(), bar);
        assertTrue(builder.hasPartitionKey());
        Assert.assertEquals(
                KeyValueEncodingType.valueOf(keyValueSchema.getSchemaInfo().getProperties().get("kv.encoding.type")),
                KeyValueEncodingType.SEPARATED);
    }

    @Test
    public void testDefaultJSONVersionGetProducerDataAssigned() {
        JSONSchema<SchemaTestUtils.Foo> fooSchema = JSONSchema.of(SchemaDefinition.<SchemaTestUtils.Foo>builder().withPojo(
                SchemaTestUtils.Foo.class).build());
        JSONSchema<SchemaTestUtils.Bar> barSchema = JSONSchema.of(SchemaDefinition.<SchemaTestUtils.Bar>builder().withPojo(
                SchemaTestUtils.Bar.class).build());

        MultiVersionSchemaInfoProvider multiVersionSchemaInfoProvider = mock(MultiVersionSchemaInfoProvider.class);
        Schema<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> keyValueSchema = Schema.KeyValue(fooSchema, barSchema);
        keyValueSchema.setSchemaInfoProvider(multiVersionSchemaInfoProvider);
        when(multiVersionSchemaInfoProvider.getSchemaByVersion(any(byte[].class)))
                .thenReturn(CompletableFuture.completedFuture(keyValueSchema.getSchemaInfo()));

        SchemaTestUtils.Foo foo = new SchemaTestUtils.Foo();
        foo.setField1("field1");
        foo.setField2("field2");
        foo.setField3(3);
        SchemaTestUtils.Bar bar = new SchemaTestUtils.Bar();
        bar.setField1(true);

        byte[] encodeBytes = keyValueSchema.encode(new KeyValue(foo, bar));
        MessageMetadata.Builder builder = MessageMetadata.newBuilder()
                .setProducerName("default");
        ByteString byteString = ByteString.copyFrom(new byte[10]);
        builder.setSchemaVersion(byteString);
        MessageImpl<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> msg = MessageImpl.create(
                builder, ByteBuffer.wrap(encodeBytes), keyValueSchema);
        KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar> keyValue = msg.getValue();
        assertEquals(keyValue.getKey(), foo);
        assertEquals(keyValue.getValue(), bar);
        assertFalse(builder.hasPartitionKey());
        Assert.assertEquals(
                KeyValueEncodingType.valueOf(keyValueSchema.getSchemaInfo().getProperties().get("kv.encoding.type")),
                KeyValueEncodingType.INLINE);
    }

    @Test
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

        byte[] encodeBytes = keyValueSchema.encode(new KeyValue(foo, bar));
        MessageMetadata.Builder builder = MessageMetadata.newBuilder()
                .setProducerName("separated");
        ByteString byteString = ByteString.copyFrom(new byte[10]);
        builder.setSchemaVersion(byteString);
        builder.setPartitionKey(Base64.getEncoder().encodeToString(fooSchema.encode(foo)));
        builder.setPartitionKeyB64Encoded(true);
        MessageImpl<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> msg = MessageImpl.create(
                builder, ByteBuffer.wrap(encodeBytes), keyValueSchema);
        KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar> keyValue = msg.getValue();
        assertEquals(keyValue.getKey(), foo);
        assertEquals(keyValue.getValue(), bar);
        assertTrue(builder.hasPartitionKey());
        Assert.assertEquals(
                KeyValueEncodingType.valueOf(keyValueSchema.getSchemaInfo().getProperties().get("kv.encoding.type")),
                KeyValueEncodingType.SEPARATED);
    }

    @Test
    public void testDefaultAVROJSONVersionGetProducerDataAssigned() {
        AvroSchema<SchemaTestUtils.Foo> fooSchema = AvroSchema.of(SchemaDefinition.<SchemaTestUtils.Foo>builder().withPojo(
                SchemaTestUtils.Foo.class).build());
        JSONSchema<SchemaTestUtils.Bar> barSchema = JSONSchema.of(SchemaDefinition.<SchemaTestUtils.Bar>builder().withPojo(
                SchemaTestUtils.Bar.class).build());

        MultiVersionSchemaInfoProvider multiVersionSchemaInfoProvider = mock(MultiVersionSchemaInfoProvider.class);
        Schema<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> keyValueSchema = Schema.KeyValue(fooSchema, barSchema);
        keyValueSchema.setSchemaInfoProvider(multiVersionSchemaInfoProvider);
        when(multiVersionSchemaInfoProvider.getSchemaByVersion(any(byte[].class)))
                .thenReturn(CompletableFuture.completedFuture(keyValueSchema.getSchemaInfo()));

        SchemaTestUtils.Foo foo = new SchemaTestUtils.Foo();
        foo.setField1("field1");
        foo.setField2("field2");
        foo.setField3(3);
        SchemaTestUtils.Bar bar = new SchemaTestUtils.Bar();
        bar.setField1(true);

        byte[] encodeBytes = keyValueSchema.encode(new KeyValue(foo, bar));
        MessageMetadata.Builder builder = MessageMetadata.newBuilder()
                .setProducerName("default");
        ByteString byteString = ByteString.copyFrom(new byte[10]);
        builder.setSchemaVersion(byteString);
        MessageImpl<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> msg = MessageImpl.create(
                builder, ByteBuffer.wrap(encodeBytes), keyValueSchema);
        KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar> keyValue = msg.getValue();
        assertEquals(keyValue.getKey(), foo);
        assertEquals(keyValue.getValue(), bar);
        assertFalse(builder.hasPartitionKey());
        Assert.assertEquals(
                KeyValueEncodingType.valueOf(keyValueSchema.getSchemaInfo().getProperties().get("kv.encoding.type")),
                KeyValueEncodingType.INLINE);
    }

    @Test
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

        byte[] encodeBytes = keyValueSchema.encode(new KeyValue(foo, bar));
        MessageMetadata.Builder builder = MessageMetadata.newBuilder()
                .setProducerName("separated");
        ByteString byteString = ByteString.copyFrom(new byte[10]);
        builder.setSchemaVersion(byteString);
        builder.setPartitionKey(Base64.getEncoder().encodeToString(fooSchema.encode(foo)));
        builder.setPartitionKeyB64Encoded(true);
        MessageImpl<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> msg = MessageImpl.create(
                builder, ByteBuffer.wrap(encodeBytes), keyValueSchema);
        KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar> keyValue = msg.getValue();
        assertEquals(keyValue.getKey(), foo);
        assertEquals(keyValue.getValue(), bar);
        assertTrue(builder.hasPartitionKey());
        Assert.assertEquals(
                KeyValueEncodingType.valueOf(keyValueSchema.getSchemaInfo().getProperties().get("kv.encoding.type")),
                KeyValueEncodingType.SEPARATED);
    }
}
